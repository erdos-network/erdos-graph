use crate::config::{Config, EdgeCacheConfig};
use crate::db::ingestion::PublicationRecord;
use crate::logger;
use bloomfilter::Bloom;
use indradb::Json;
use indradb::{
    Database, Datastore, Identifier, QueryExt, QueryOutputValue, RangeVertexQuery,
    SpecificVertexQuery,
};
use lru::LruCache;
use serde_json::json;
use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::time::Instant;
use textdistance::{Algorithm, Cosine};
use uuid::Uuid;

/// Helper struct to manage deduplication caching.
pub(crate) struct DeduplicationCache {
    /// Track which years have been loaded into the Bloom filters
    loaded_years: HashSet<u32>,
    /// Global Bloom Filter for Titles
    title_filter: Bloom<String>,
    /// Global Bloom Filter for Authors
    author_filter: Bloom<String>,
    /// Cache of known publication IDs (external IDs like arxiv:...) to avoid DB lookups
    published_ids: HashSet<String>,
}

impl DeduplicationCache {
    pub(crate) fn new(bloom_size: usize) -> Self {
        Self {
            loaded_years: HashSet::new(),
            title_filter: Bloom::new_for_fp_rate(bloom_size, 0.01).unwrap(),
            author_filter: Bloom::new_for_fp_rate(bloom_size, 0.01).unwrap(),
            published_ids: HashSet::new(),
        }
    }

    /// Adds a record to the cache.
    pub(crate) fn add(
        &mut self,
        _year: u32,
        _id: Uuid,
        title: String,
        authors: &[String],
        pub_id: String,
    ) {
        self.title_filter.set(&normalize_title(&title));
        self.published_ids.insert(pub_id);

        for author in authors {
            self.author_filter.set(&normalize_title(author));
        }
    }

    /// Checks if a record exists in the cache (and populates cache from DB if needed).
    pub(crate) fn check_exists_and_cache(
        &mut self,
        record: &PublicationRecord,
        datastore: &Database<impl Datastore>,
        config: &Config,
    ) -> bool {
        let start = Instant::now();
        // Ensure cache is populated for this year
        if !self.loaded_years.contains(&record.year) {
            self.populate_year(record.year, datastore);
        }

        // Check exact ID match in memory first - FAST path
        if self.published_ids.contains(&record.id) {
            logger::debug(&format!(
                "Cache hit (known ID) for '{}' - took {}ms",
                record.title,
                start.elapsed().as_millis()
            ));
            return true;
        }

        let record_title_norm = normalize_title(&record.title);

        // Global Title Bloom filter check
        if !self.title_filter.check(&record_title_norm) {
            logger::debug(&format!(
                "Cache miss (title bloom) for '{}' - took {}ms",
                record.title,
                start.elapsed().as_millis()
            ));
            return false;
        }

        // Global Author Bloom filter check (if record has authors)
        if !record.authors.is_empty() {
            let mut any_author_match = false;
            for author in &record.authors {
                if self.author_filter.check(&normalize_title(author)) {
                    any_author_match = true;
                    break;
                }
            }
            if !any_author_match {
                logger::debug(&format!(
                    "Cache miss (author bloom) for '{}' - took {}ms",
                    record.title,
                    start.elapsed().as_millis()
                ));
                return false;
            }
        }

        if let Ok(type_prop) = Identifier::new(crate::db::schema::PUBLICATION_TYPE)
            && let Ok(year_prop) = Identifier::new("year")
            && let Ok(title_prop) = Identifier::new("title")
        {
            let year_val = Json::new(json!(record.year.to_string()));
            let q = RangeVertexQuery::new()
                .t(type_prop)
                .with_property_equal_to(year_prop, year_val);

            if let Ok(q) = q
                && let Ok(q_props) = q.properties()
                && let Ok(results) = datastore.get(q_props)
                && let Some(QueryOutputValue::VertexProperties(vps)) = results.first()
            {
                let cosine = Cosine::default();
                let record_title_lower = record.title.to_lowercase();

                for vp in vps {
                    if let Some(val) = vp.props.iter().find(|p| p.name == title_prop)
                        && let Some(title) = val.value.as_str()
                    {
                        // If normalized titles are identical, skip cosine
                        if record_title_norm == normalize_title(title) {
                            if check_authors_similarity(
                                &vp.vertex.id,
                                &record.authors,
                                datastore,
                                config.deduplication.author_similarity_threshold,
                            ) {
                                logger::debug(&format!(
                                    "Cache hit (exact title) for '{}' - took {}ms",
                                    record.title,
                                    start.elapsed().as_millis()
                                ));
                                return true;
                            }
                            continue;
                        }

                        let db_title_lower = title.to_lowercase();
                        let similarity =
                            cosine.for_str(&record_title_lower, &db_title_lower).nval();

                        if similarity >= config.deduplication.title_similarity_threshold
                            && check_authors_similarity(
                                &vp.vertex.id,
                                &record.authors,
                                datastore,
                                config.deduplication.author_similarity_threshold,
                            )
                        {
                            logger::debug(&format!(
                                "Cache hit (similarity) for '{}' - took {}ms",
                                record.title,
                                find_elapsed_millis(start)
                            ));
                            return true;
                        }
                    }
                }
            }
        }

        logger::debug(&format!(
            "Cache checked (no match) for '{}' - took {}ms",
            record.title,
            start.elapsed().as_millis()
        ));
        false
    }

    fn populate_year(&mut self, year: u32, datastore: &Database<impl Datastore>) {
        let start = Instant::now();
        // Fetch all papers for the year
        if let Ok(type_prop) = Identifier::new(crate::db::schema::PUBLICATION_TYPE)
            && let Ok(year_prop) = Identifier::new("year")
            && let Ok(title_prop) = Identifier::new("title")
            && let Ok(id_prop) = Identifier::new("publication_id")
        {
            let year_val = Json::new(json!(year.to_string()));

            // We need vertex ID, title, and publiction ID
            let q = RangeVertexQuery::new()
                .t(type_prop)
                .with_property_equal_to(year_prop, year_val);

            if let Ok(q) = q
                && let Ok(q_props) = q.properties()
                && let Ok(results) = datastore.get(q_props)
                && let Some(QueryOutputValue::VertexProperties(vps)) = results.first()
            {
                let mut count = 0;
                let mut pub_ids = Vec::with_capacity(vps.len());

                for vp in vps {
                    let mut title_opt = None;
                    let mut id_opt = None;

                    for p in &vp.props {
                        if p.name == title_prop {
                            title_opt = p.value.as_str();
                        } else if p.name == id_prop {
                            id_opt = p.value.as_str();
                        }
                    }

                    if let Some(title) = title_opt {
                        let norm = normalize_title(title);
                        self.title_filter.set(&norm);
                        pub_ids.push(vp.vertex.id);
                        count += 1;
                    }

                    if let Some(pid) = id_opt {
                        self.published_ids.insert(pid.to_string());
                    }
                }

                // Batch fetch authors
                if !pub_ids.is_empty() {
                    for chunk in pub_ids.chunks(1000) {
                        let q = SpecificVertexQuery::new(chunk.to_vec());
                        if let Ok(q_in) = q.inbound()
                            && let Ok(q_out) = q_in.outbound()
                            && let Ok(q_props) = q_out.properties()
                            && let Ok(results) = datastore.get(q_props)
                            && let Some(QueryOutputValue::VertexProperties(author_vps)) =
                                results.first()
                            && let Ok(name_prop) = Identifier::new("name")
                        {
                            for avp in author_vps {
                                if let Some(val) = avp.props.iter().find(|p| p.name == name_prop)
                                    && let Some(name) = val.value.as_str()
                                {
                                    self.author_filter.set(&normalize_title(name));
                                }
                            }
                        }
                    }
                }

                logger::info(&format!(
                    "Populated filters for year {}: {} records in {}ms",
                    year,
                    count,
                    start.elapsed().as_millis()
                ));
            }
        }
        self.loaded_years.insert(year);
    }
}

/// Normalizes a title for Bloom filter and comparison.
pub(crate) fn normalize_title(title: &str) -> String {
    let stop_words: HashSet<&str> = [
        "a", "an", "and", "are", "as", "at", "be", "by", "for", "from", "has", "he", "in", "is",
        "it", "its", "of", "on", "that", "the", "to", "was", "were", "will", "with",
    ]
    .iter()
    .cloned()
    .collect();

    title
        .to_lowercase()
        .split_whitespace()
        .filter(|token| !stop_words.contains(token))
        .map(|token| {
            token
                .chars()
                .filter(|c| c.is_alphanumeric())
                .collect::<String>()
        })
        .collect::<String>()
}

/// Normalizes an author name for cache and deduplication.
pub(crate) fn normalize_author(name: &str) -> String {
    name.trim()
        .to_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn find_elapsed_millis(start: Instant) -> u128 {
    start.elapsed().as_millis()
}

/// Helper to check author similarity
fn check_authors_similarity(
    pub_vertex_id: &Uuid,
    authors: &[String],
    datastore: &Database<impl Datastore>,
    threshold: f64,
) -> bool {
    // Fetch authors linked to the publication vertex
    let q = SpecificVertexQuery::single(*pub_vertex_id);
    if let Ok(q_in) = q.inbound()
        && let Ok(q_out) = q_in.outbound()
        && let Ok(q_props) = q_out.properties()
        && let Ok(results) = datastore.get(q_props)
        && let Some(QueryOutputValue::VertexProperties(vps)) = results.first()
        && let Ok(name_prop) = Identifier::new("name")
    {
        let db_authors: Vec<String> = vps
            .iter()
            .filter_map(|vp| {
                vp.props
                    .iter()
                    .find(|p| p.name == name_prop)
                    .and_then(|val| val.value.as_str().map(|s| s.to_string()))
            })
            .collect();

        if db_authors.is_empty() {
            return false;
        }

        let cosine = Cosine::default();
        let mut match_count = 0;

        for author in authors {
            let author_norm = normalize_author(author);
            for db_author in &db_authors {
                let db_author_norm = normalize_author(db_author);
                if cosine.for_str(&author_norm, &db_author_norm).nval() >= threshold {
                    match_count += 1;
                    break;
                }
            }
        }

        // If at least 50% of authors match, consider it the same publication
        return (match_count as f64 / authors.len() as f64) >= 0.5;
    }
    false
}

/// Three-tier edge cache system
pub(crate) struct EdgeCacheSystem {
    /// Tier 1 hot cache
    pub(crate) hot_cache: LruCache<(Uuid, Uuid), u64>,

    /// Tier 2 warm cache
    pub(crate) warm_cache: LruCache<(Uuid, Uuid), u64>,

    /// Tier 3 cold bloom filter
    pub(crate) cold_bloom: Bloom<(Uuid, Uuid)>,

    pub edges_loaded: bool,
}

impl EdgeCacheSystem {
    pub(crate) fn new(config: EdgeCacheConfig) -> Self {
        Self {
            hot_cache: LruCache::new(NonZeroUsize::new(config.hot_size).unwrap()),
            warm_cache: LruCache::new(NonZeroUsize::new(config.warm_size).unwrap()),
            cold_bloom: Bloom::new_for_fp_rate(config.bloom_size, 0.01).unwrap(),
            edges_loaded: false,
        }
    }

    /// Get weight from cache with promotion
    pub fn get(&mut self, key: (Uuid, Uuid)) -> Option<u64> {
        // Check hot cache
        if let Some(&weight) = self.hot_cache.get(&key) {
            return Some(weight);
        }

        // Check warm cache
        if let Some(&weight) = self.warm_cache.get(&key) {
            // Promote to hot cache
            self.put(key, weight);
            return Some(weight);
        }

        None
    }

    /// Insert into cache with eviction handling
    pub fn put(&mut self, key: (Uuid, Uuid), weight: u64) {
        // Evict from hot to warm if full
        if self.hot_cache.len() == self.hot_cache.cap().get()
            && !self.hot_cache.contains(&key)
            && let Some((evicted_key, evicted_val)) = self.hot_cache.pop_lru()
        {
            self.warm_cache.put(evicted_key, evicted_val);
        }

        // Insert into hot cache
        self.hot_cache.put(key, weight);

        // Mark in bloom filter
        self.cold_bloom.set(&key);
    }

    /// Check bloom existence
    pub fn exists(&self, key: (Uuid, Uuid)) -> bool {
        self.cold_bloom.check(&key)
    }
}
