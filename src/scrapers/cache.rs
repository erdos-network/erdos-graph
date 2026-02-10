use crate::config::{Config, EdgeCacheConfig};
use crate::db::ingestion::PublicationRecord;
use crate::logger;
use bloomfilter::Bloom;
use bumpalo::Bump;
use helix_db::helix_engine::storage_core::HelixGraphStorage;
use helix_db::helix_engine::traversal_core::HelixGraphEngine;
use helix_db::protocol::value::Value;
use helix_db::utils::items::Node;
use lru::LruCache;
use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Instant;
use textdistance::{Algorithm, Cosine};

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
        _id: String,
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
        engine: &Arc<HelixGraphEngine>,
        config: &Config,
    ) -> bool {
        let start = Instant::now();
        // Ensure cache is populated for this year
        if !self.loaded_years.contains(&record.year) {
            self.populate_year(record.year, engine);
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

        // Deep check against DB using 'year' index
        let txn = match engine.storage.graph_env.read_txn() {
            Ok(t) => t,
            Err(e) => {
                logger::error(&format!("Failed to start read txn: {}", e));
                return false;
            }
        };

        let arena = Bump::new();

        if let Some((year_index_db, _)) = engine.storage.secondary_indices.get("year") {
            let year_str = record.year.to_string();
            if let Ok(key_bytes) = bincode::serialize(&Value::String(year_str))
                && let Ok(iter) = year_index_db.prefix_iter(&txn, &key_bytes)
            {
                let cosine = Cosine::default();
                let record_title_lower = record.title.to_lowercase();

                for (_, node_id) in iter.flatten() {
                    if let Ok(Some(node_bytes)) = engine.storage.nodes_db.get(&txn, &node_id)
                        && let Ok(node) = Node::from_bincode_bytes(node_id, node_bytes, &arena)
                    {
                        if node.label != crate::db::schema::PUBLICATION_TYPE {
                            continue;
                        }

                        if let Some(Value::String(title)) = node.get_property("title") {
                            if record_title_norm == normalize_title(title) {
                                if check_authors_similarity(
                                    &node.id,
                                    &record.authors,
                                    engine,
                                    &txn,
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
                                    &node.id,
                                    &record.authors,
                                    engine,
                                    &txn,
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
        }

        logger::debug(&format!(
            "Cache checked (no match) for '{}' - took {}ms",
            record.title,
            start.elapsed().as_millis()
        ));
        false
    }

    fn populate_year(&mut self, year: u32, engine: &Arc<HelixGraphEngine>) {
        let start = Instant::now();
        let txn = match engine.storage.graph_env.read_txn() {
            Ok(t) => t,
            Err(e) => {
                logger::error(&format!("Failed to start read txn for populate: {}", e));
                return;
            }
        };

        let mut count = 0;
        let arena = Bump::new();

        if let Some((year_index_db, _)) = engine.storage.secondary_indices.get("year") {
            let year_str = year.to_string();
            if let Ok(key_bytes) = bincode::serialize(&Value::String(year_str))
                && let Ok(iter) = year_index_db.prefix_iter(&txn, &key_bytes)
            {
                for (_, node_id) in iter.flatten() {
                    if let Ok(Some(node_bytes)) = engine.storage.nodes_db.get(&txn, &node_id)
                        && let Ok(node) = Node::from_bincode_bytes(node_id, node_bytes, &arena)
                    {
                        if node.label != crate::db::schema::PUBLICATION_TYPE {
                            continue;
                        }

                        let mut title_opt = None;
                        let mut id_opt = None;

                        if let Some(Value::String(t)) = node.get_property("title") {
                            title_opt = Some(t);
                        }
                        if let Some(Value::String(pid)) = node.get_property("publication_id") {
                            id_opt = Some(pid);
                        }

                        if let Some(title) = title_opt {
                            let norm = normalize_title(title);
                            self.title_filter.set(&norm);
                            count += 1;
                        }
                        if let Some(pid) = id_opt {
                            self.published_ids.insert(pid.to_string());
                        }

                        let label_hash = helix_db::utils::label_hash::hash_label(
                            crate::db::schema::AUTHORED_TYPE,
                            None,
                        );
                        let in_key = HelixGraphStorage::in_edge_key(&node.id, &label_hash);

                        if let Ok(iter) = engine.storage.in_edges_db.prefix_iter(&txn, &in_key) {
                            for (key, val_bytes) in iter.flatten() {
                                // Check if the key still matches our prefix
                                if !key.starts_with(&in_key) {
                                    break;
                                }
                                if val_bytes.len() >= 32 {
                                    let mut author_id_bytes = [0u8; 16];
                                    author_id_bytes.copy_from_slice(&val_bytes[16..32]);
                                    let author_id = u128::from_be_bytes(author_id_bytes);

                                    if let Ok(Some(author_bytes)) =
                                        engine.storage.nodes_db.get(&txn, &author_id)
                                        && let Ok(author_node) = Node::from_bincode_bytes(
                                            author_id,
                                            author_bytes,
                                            &arena,
                                        )
                                        && let Some(Value::String(name)) =
                                            author_node.get_property("name")
                                    {
                                        self.author_filter.set(&normalize_title(name));
                                    }
                                }
                            }
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
    pub_node_id: &u128,
    authors: &[String],
    engine: &Arc<HelixGraphEngine>,
    txn: &heed3::RoTxn,
    threshold: f64,
) -> bool {
    let arena = Bump::new();
    let label_hash =
        helix_db::utils::label_hash::hash_label(crate::db::schema::AUTHORED_TYPE, None);
    let in_key = HelixGraphStorage::in_edge_key(pub_node_id, &label_hash);

    let mut db_authors = Vec::new();

    if let Ok(iter) = engine.storage.in_edges_db.prefix_iter(txn, &in_key) {
        for (key, val_bytes) in iter.flatten() {
            // Check if the key still matches our prefix
            if !key.starts_with(&in_key) {
                break;
            }
            if val_bytes.len() >= 32 {
                let mut author_id_bytes = [0u8; 16];
                author_id_bytes.copy_from_slice(&val_bytes[16..32]);
                let author_id = u128::from_be_bytes(author_id_bytes);

                if let Ok(Some(author_bytes)) = engine.storage.nodes_db.get(txn, &author_id)
                    && let Ok(author_node) =
                        Node::from_bincode_bytes(author_id, author_bytes, &arena)
                    && let Some(Value::String(name)) = author_node.get_property("name")
                {
                    db_authors.push(name.to_string());
                }
            }
        }
    }

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
    (match_count as f64 / authors.len() as f64) >= 0.5
}

/// Three-tier edge cache system
pub(crate) struct EdgeCacheSystem {
    /// Tier 1 hot cache
    pub(crate) hot_cache: LruCache<(u128, u128), u64>,

    /// Tier 2 warm cache
    pub(crate) warm_cache: LruCache<(u128, u128), u64>,

    /// Tier 3 cold bloom filter
    pub(crate) cold_bloom: Bloom<(u128, u128)>,

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
    pub fn get(&mut self, key: (u128, u128)) -> Option<u64> {
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
    pub fn put(&mut self, key: (u128, u128), weight: u64) {
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
}
