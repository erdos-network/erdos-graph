use crate::config::Config;
use crate::db::ingestion::PublicationRecord;
use crate::logger;
use crate::scrapers::{ArxivScraper, DblpScraper, Scraper, ZbmathScraper};
use crate::utilities::thread_safe_queue::{QueueConfig, ThreadSafeQueue};
use bloomfilter::Bloom;
use chrono::{DateTime, Utc};
use indradb::{
    Database, Datastore, Edge, Identifier, Json, QueryExt, QueryOutputValue, RangeVertexQuery,
    SpecificEdgeQuery, SpecificVertexQuery, Vertex,
};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use textdistance::Algorithm;
use textdistance::Cosine;
use uuid::Uuid;

use std::time::Instant;

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
    fn add(&mut self, _year: u32, _id: Uuid, title: String, authors: &[String], pub_id: String) {
        self.title_filter.set(&normalize_title(&title));
        self.published_ids.insert(pub_id);

        for author in authors {
            self.author_filter.set(&normalize_title(author));
        }
    }

    /// Checks if a record exists in the cache (and populates cache from DB if needed).
    fn check_exists_and_cache(
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
                                start.elapsed().as_millis()
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
/// - Lowercases
/// - Tokenizes by whitespace
/// - Removes stop words
/// - Removes non-alphanumeric characters
/// - Concatenates
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
/// - Lowercases
/// - Splits by whitespace and rejoins with single space
pub(crate) fn normalize_author(name: &str) -> String {
    name.trim()
        .to_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// Context for ingestion to maintain caches
pub(crate) struct IngestionContext {
    pub dedup_cache: DeduplicationCache,
    pub author_cache: HashMap<String, Vertex>,
    pub edge_cache: HashMap<(Uuid, Uuid), u64>,
}

impl IngestionContext {
    fn new(bloom_size: usize) -> Self {
        Self {
            dedup_cache: DeduplicationCache::new(bloom_size),
            author_cache: HashMap::new(),
            edge_cache: HashMap::new(),
        }
    }

    /// Preloads all author vertices into the cache
    fn preload_authors(
        &mut self,
        datastore: &Database<impl Datastore>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let start = Instant::now();
        logger::info("Preloading authors...");

        if let Ok(person_type) = Identifier::new(crate::db::schema::PERSON_TYPE)
            && let Ok(name_prop) = Identifier::new("name")
        {
            let q = RangeVertexQuery::new().t(person_type);

            if let Ok(q_props) = q.properties()
                && let Ok(results) = datastore.get(q_props)
                && let Some(QueryOutputValue::VertexProperties(vps)) = results.first()
            {
                let count = vps.len();
                for vp in vps {
                    for p in &vp.props {
                        if p.name == name_prop {
                            if let Some(name) = p.value.as_str() {
                                let norm_name = normalize_author(name);
                                self.author_cache.insert(norm_name, vp.vertex.clone());
                            }
                        }
                    }
                }
                logger::info(&format!(
                    "Preloaded {} authors in {}ms",
                    count,
                    start.elapsed().as_millis()
                ));
            }
        }
        Ok(())
    }

    /// Pre-fetches existing edges between authors to warm the cache.
    /// This reduces DB reads from O(N^2) to O(1) per publication.
    fn prefetch_coauthor_edges(
        &mut self,
        authors: &[Vertex],
        datastore: &Database<impl Datastore>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::db::schema::COAUTHORED_WITH_TYPE;
        use std::collections::HashSet;

        if authors.len() < 2 {
            return Ok(());
        }

        let author_ids: Vec<Uuid> = authors.iter().map(|a| a.id).collect();
        let author_id_set: HashSet<Uuid> = author_ids.iter().cloned().collect();
        let edge_type = Identifier::new(COAUTHORED_WITH_TYPE)?;

        // 1. Get all outbound edges of type COAUTHORED_WITH from these authors
        let q = SpecificVertexQuery::new(author_ids)
            .outbound()?
            .t(edge_type);
        let results = datastore.get(q)?;

        let mut relevant_edges = Vec::new();

        if let Some(QueryOutputValue::Edges(edges)) = results.first() {
            for edge in edges {
                // Only cache edges that are internal to this group of authors
                if author_id_set.contains(&edge.inbound_id) {
                    relevant_edges.push(edge.clone());
                }
            }
        }

        if relevant_edges.is_empty() {
            return Ok(());
        }

        // 2. Fetch properties (weight) for these edges
        let weight_prop = Identifier::new("weight")?;
        let q_props = SpecificEdgeQuery::new(relevant_edges.clone())
            .properties()?
            .name(weight_prop);
        let prop_results = datastore.get(q_props)?;

        if let Some(QueryOutputValue::EdgeProperties(props_list)) = prop_results.first() {
            for (i, edge_props) in props_list.iter().enumerate() {
                if let Some(edge) = relevant_edges.get(i) {
                    let weight = edge_props
                        .props
                        .first()
                        .and_then(|p| {
                            p.value.as_u64().or_else(|| {
                                serde_json::from_value::<String>(p.value.deref().clone())
                                    .ok()
                                    .and_then(|s| s.parse::<u64>().ok())
                            })
                        })
                        .unwrap_or(1);

                    self.edge_cache
                        .insert((edge.outbound_id, edge.inbound_id), weight);
                }
            }
        }

        Ok(())
    }
}

/// Orchestrates the complete scraping process for one or more publication sources.
///
/// This function handles:
/// - Spawning tasks to scrape from specified sources
/// - Collecting scraped publication records via a thread-safe queue
/// - Ingesting records into the database
///
/// # Arguments
/// * `start_date` - Start date for scraping
/// * `end_date` - End date for scraping
/// * `sources` - List of sources to scrape ("arxiv", "dblp", or "zbmath")
/// * `datastore` - Mutable reference to the IndraDB datastore
/// * `config` - Reference to the application configuration
///
/// # Returns
/// `Ok(())` on success, or an error if scraping or ingestion fails
///
/// # Errors
/// - Configuration loading errors
/// - Scraping errors from individual sources
/// - Database ingestion errors
#[coverage(off)]
pub async fn run_scrape(
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
    sources: Vec<String>,
    datastore: &mut Database<impl Datastore>,
    config: &Config,
) -> Result<(), Box<dyn std::error::Error>> {
    // Define queue
    let queue = ThreadSafeQueue::new(QueueConfig::default());

    logger::info(&format!(
        "Starting scraping for range {} to {}",
        start_date, end_date
    ));

    // Define vector of tasks to process
    let mut tasks = vec![];

    for src in &sources {
        logger::info(&format!("Spawning scraper task for {}", src));
        // Create producer task for each source
        let producer = queue.create_producer();
        let src_name = src.clone();
        let start = start_date;
        let end = end_date;
        let scraper_config = config.scrapers.clone();

        let task = tokio::spawn(async move {
            let scraper: Box<dyn Scraper> = match src_name.as_str() {
                "arxiv" => Box::new(ArxivScraper::with_config(scraper_config.arxiv)),
                "dblp" => Box::new(DblpScraper::with_config(scraper_config.dblp)),
                "zbmath" => Box::new(ZbmathScraper::new()), // Zbmath not updated yet
                _ => {
                    logger::error(&format!("Unknown source: {}", src_name));
                    return;
                }
            };

            logger::info(&format!("{} scraper started", src_name));
            match scraper.scrape_range(start, end, producer).await {
                Ok(()) => {
                    logger::info(&format!("{} scraper finished", src_name));
                }
                Err(e) => {
                    logger::error(&format!("Scraping failed for {}: {}", src_name, e));
                }
            }
        });

        tasks.push(task);
    }

    // Consumer loop
    let mut ingested_count = 0;
    let mut context = IngestionContext::new(config.deduplication.bloom_filter_size);

    // Preload authors to speed up ingestion
    if let Err(e) = context.preload_authors(datastore) {
        logger::error(&format!("Failed to preload authors: {}", e));
    }

    let loop_start_time = Instant::now();
    let mut last_log_time = Instant::now();
    let mut last_log_count = 0;

    loop {
        if let Some(record) = queue.dequeue() {
            // Ingest publication into db
            if let Err(e) = ingest_publication(record, datastore, config, &mut context).await {
                logger::error(&format!("Failed to ingest publication: {}", e));
            }
            ingested_count += 1;

            if ingested_count % 100 == 0 {
                let now = Instant::now();
                let elapsed = now.duration_since(last_log_time).as_secs_f64();
                let total_elapsed = now.duration_since(loop_start_time).as_secs_f64();

                let current_rate = (ingested_count - last_log_count) as f64 / elapsed;
                let overall_rate = ingested_count as f64 / total_elapsed;

                logger::info(&format!(
                    "Ingested {} records (Current: {:.2} rec/s, Overall: {:.2} rec/s)",
                    ingested_count, current_rate, overall_rate
                ));

                last_log_time = now;
                last_log_count = ingested_count;
            }
        } else {
            if queue.producers_finished() {
                break;
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(
                config.polling_interval_ms,
            ))
            .await;
        }
    }

    // Cleanup
    for task in tasks {
        let _ = task.await;
    }

    logger::info("Scraping orchestration finished for current chunk");

    Ok(())
}

/// Ingests a single publication record into the database.
async fn ingest_publication(
    record: PublicationRecord,
    datastore: &mut Database<impl Datastore>,
    config: &Config,
    context: &mut IngestionContext,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let title = record.title.clone(); // Clone for logging

    // Check if record already exists in the database
    if publication_exists(&record, datastore, config, &mut context.dedup_cache) {
        logger::debug(&format!(
            "Skipped duplicate '{}' - total check time {}ms",
            title,
            start.elapsed().as_millis()
        ));
        return Ok(());
    }

    // Add publication vertex to database
    let pub_vertex = add_publication(&record, datastore)?;
    let added_pub = Instant::now();

    // Add to cache
    context.dedup_cache.add(
        record.year,
        pub_vertex.id,
        record.title.clone(),
        &record.authors,
        record.id.clone(),
    );

    let mut author_vertices = Vec::new();

    // Add new author vertices and create AUTHORED edges
    for author in &record.authors {
        let author_vertex =
            get_or_create_author_vertex(author, datastore, &mut context.author_cache)?;
        create_authored_edge(&author_vertex, &pub_vertex, datastore)?;
        author_vertices.push(author_vertex);
    }
    let added_authors = Instant::now();

    // Pre-fetch existing edges to avoid O(N^2) DB reads
    if !author_vertices.is_empty() {
        if let Err(e) = context.prefetch_coauthor_edges(&author_vertices, datastore) {
            logger::error(&format!("Failed to prefetch co-author edges: {}", e));
        }
    }

    // Create COAUTHORED_WITH edges between authors
    for i in 0..author_vertices.len() {
        for j in (i + 1)..author_vertices.len() {
            let author1 = &author_vertices[i];
            let author2 = &author_vertices[j];

            // Create COAUTHORED_WITH edge between record.authors[i] and record.authors[j]
            create_coauthor_edge(author1, author2, datastore, &mut context.edge_cache)?;
            // Also create reverse edge for undirected graph simulation
            create_coauthor_edge(author2, author1, datastore, &mut context.edge_cache)?;
        }
    }
    let added_edges = Instant::now();

    logger::debug(&format!(
        "Ingested '{}': check={}ms, pub={}ms, authors={}ms, edges={}ms, total={}ms",
        title,
        added_pub.duration_since(start).as_millis(), // Note: limits of granularity here since we do check inside
        added_pub.duration_since(start).as_millis(), // Ideally we'd separate check time from pub creation time if possible, but pub creation is fast
        added_authors.duration_since(added_pub).as_millis(),
        added_edges.duration_since(added_authors).as_millis(),
        start.elapsed().as_millis()
    ));

    Ok(())
}

/// Checks if a publication record already exists in the database.
///
/// This performs two checks:
/// 1. Exact match on `publication_id` (e.g., ArXiv ID)
/// 2. Fuzzy match based on year, title similarity (Cosine), and author set similarity (Jaccard)
///
/// # Arguments
/// * `record` - The publication record to check
/// * `datastore` - Reference to the IndraDB datastore
/// * `config` - Reference to the application configuration
///
/// # Returns
/// `true` if a matching publication exists, `false` otherwise
pub(crate) fn publication_exists(
    record: &PublicationRecord,
    datastore: &Database<impl Datastore>,
    config: &Config,
    dedup_cache: &mut DeduplicationCache,
) -> bool {
    // Check exact match using cache (fast path)
    // Note: This relies on populate_year loading all IDs for the relevant year
    // We skip the global DB check for performance (saved ~70ms per record)

    // Fuzzy match using cache
    dedup_cache.check_exists_and_cache(record, datastore, config)
}

/// Helper to check author similarity for a candidate publication
///
/// # Arguments
/// * `pub_id` - The UUID of the existing publication in the database
/// * `new_authors` - List of authors from the new candidate record
/// * `datastore` - Reference to the IndraDB datastore
/// * `threshold` - Jaccard similarity threshold (0.0 to 1.0)
///
/// # Returns
/// `true` if the author sets are similar enough, `false` otherwise
fn check_authors_similarity(
    pub_id: &uuid::Uuid,
    new_authors: &[String],
    datastore: &Database<impl Datastore>,
    threshold: f64,
) -> bool {
    // Fetch authors of the existing publication
    // Query: SpecificVertex(pub) -> Inbound (AUTHORED) -> Outbound Vertex (Author) -> Properties
    let q = SpecificVertexQuery::single(*pub_id);
    let q_edges = match q.inbound() {
        Ok(q) => q,
        Err(_) => return false,
    };
    let q_authors = match q_edges.outbound() {
        Ok(q) => q,
        Err(_) => return false,
    };

    let q_props = match q_authors.properties() {
        Ok(q) => q,
        Err(_) => return false,
    };

    let results = match datastore.get(q_props) {
        Ok(r) => r,
        Err(_) => return false,
    };

    let mut db_authors = HashSet::new();
    if let Some(QueryOutputValue::VertexProperties(vps)) = results.first() {
        let name_prop = match Identifier::new("name") {
            Ok(n) => n,
            Err(_) => return false,
        };

        for vp in vps {
            if let Some(val) = vp.props.iter().find(|p| p.name == name_prop)
                && let Some(name) = val.value.as_str()
            {
                db_authors.insert(name.to_lowercase());
            }
        }
    }

    if db_authors.is_empty() && new_authors.is_empty() {
        return true;
    }
    if db_authors.is_empty() || new_authors.is_empty() {
        return false;
    }

    let new_authors_set: HashSet<String> = new_authors.iter().map(|s| s.to_lowercase()).collect();

    let intersection = db_authors.intersection(&new_authors_set).count();
    let union = db_authors.union(&new_authors_set).count();

    let jaccard = if union == 0 {
        0.0
    } else {
        intersection as f64 / union as f64
    };

    jaccard >= threshold
}

/// Adds a publication record to the database.
///
/// Creates a new `Publication` vertex and sets its properties.
///
/// # Arguments
/// * `record` - The publication record to add
/// * `datastore` - Mutable reference to the IndraDB datastore
///
/// # Returns
/// The created Publication `Vertex`
///
/// # Errors
/// Returns an error if database operations fail
pub(crate) fn add_publication(
    record: &PublicationRecord,
    datastore: &mut Database<impl Datastore>,
) -> Result<Vertex, Box<dyn std::error::Error>> {
    use crate::db::schema::PUBLICATION_TYPE;

    let vertex_type = Identifier::new(PUBLICATION_TYPE)?;
    let vertex = Vertex::new(vertex_type);

    datastore.create_vertex(&vertex)?;

    let properties = vec![
        ("title", json!(record.title)),
        ("year", json!(record.year.to_string())),
        ("venue", json!(record.venue.clone().unwrap_or_default())),
        ("publication_id", json!(record.id)),
    ];

    for (name, value) in properties {
        let q = SpecificVertexQuery::single(vertex.id);
        let prop_name = Identifier::new(name)?;
        let json_val = Json::new(value);
        datastore.set_properties(q, prop_name, &json_val)?;
    }

    Ok(vertex)
}

/// Retrieves or creates an author vertex in the database.
///
/// # Arguments
/// * `author_name` - The name of the author
/// * `datastore` - Mutable reference to the IndraDB datastore
///
/// # Returns
/// The existing or newly created Author `Vertex`
///
/// # Errors
/// Returns an error if database operations fail
pub(crate) fn get_or_create_author_vertex(
    author_name: &str,
    datastore: &mut Database<impl Datastore>,
    author_cache: &mut HashMap<String, Vertex>,
) -> Result<Vertex, Box<dyn std::error::Error>> {
    use crate::db::schema::PERSON_TYPE;

    let norm_name = normalize_author(author_name);
    if let Some(vertex) = author_cache.get(&norm_name) {
        return Ok(vertex.clone());
    }

    let person_type = Identifier::new(PERSON_TYPE)?;

    // TRUST THE CACHE:
    // Since we preload ALL authors at startup, if it's not in the cache,
    // it definitely doesn't exist in the DB. Skip the expensive Read check.

    // Create new vertex
    let vertex = Vertex::new(person_type);
    datastore.create_vertex(&vertex)?;

    // Set properties
    let properties = vec![
        ("name", json!(author_name)),
        ("erdos_number", json!("None")),
        ("is_erdos", json!("false")),
        ("aliases", json!("[]")),
        ("updated_at", json!(Utc::now().timestamp().to_string())),
    ];

    for (name, value) in properties {
        let q = SpecificVertexQuery::single(vertex.id);
        let prop_name = Identifier::new(name)?;
        let json_val = Json::new(value);
        datastore.set_properties(q, prop_name, &json_val)?;
    }

    author_cache.insert(norm_name, vertex.clone());
    Ok(vertex)
}

/// Creates an AUTHORED edge between an author and a publication.
///
/// # Arguments
/// * `author` - The Author vertex
/// * `publication` - The Publication vertex
/// * `datastore` - Mutable reference to the IndraDB datastore
///
/// # Returns
/// `Ok(())` on success
///
/// # Errors
/// Returns an error if database operations fail
pub(crate) fn create_authored_edge(
    author: &Vertex,
    publication: &Vertex,
    datastore: &mut Database<impl Datastore>,
) -> Result<(), Box<dyn std::error::Error>> {
    use crate::db::schema::AUTHORED_TYPE;

    let edge_type = Identifier::new(AUTHORED_TYPE)?;
    let edge = Edge::new(author.id, edge_type, publication.id);

    datastore.create_edge(&edge)?;

    Ok(())
}

/// Creates or updates a COAUTHORED_WITH edge between two authors.
///
/// If the edge already exists, its weight is incremented. Otherwise, a new edge
/// is created with weight 1.
///
/// # Arguments
/// * `author1` - The first Author vertex
/// * `author2` - The second Author vertex
/// * `datastore` - Mutable reference to the IndraDB datastore
///
/// # Returns
/// `Ok(())` on success
///
/// # Errors
/// Returns an error if database operations fail
pub(crate) fn create_coauthor_edge(
    author1: &Vertex,
    author2: &Vertex,
    datastore: &mut Database<impl Datastore>,
    edge_cache: &mut HashMap<(Uuid, Uuid), u64>,
) -> Result<(), Box<dyn std::error::Error>> {
    use crate::db::schema::COAUTHORED_WITH_TYPE;

    let edge_type = Identifier::new(COAUTHORED_WITH_TYPE)?;
    let edge = Edge::new(author1.id, edge_type, author2.id);
    let weight_prop = Identifier::new("weight")?;
    let key = (author1.id, author2.id);

    // Check cache first
    if let Some(&weight) = edge_cache.get(&key) {
        let new_weight = weight + 1;
        let q = SpecificEdgeQuery::single(edge.clone());
        datastore.set_properties(q, weight_prop, &Json::new(json!(new_weight)))?;
        edge_cache.insert(key, new_weight);
        return Ok(());
    }

    // Check if edge exists
    let q = SpecificEdgeQuery::single(edge.clone());
    let results = datastore.get(q)?;

    if let Some(QueryOutputValue::Edges(edges)) = results.first()
        && !edges.is_empty()
    {
        // Edge exists, increment weight
        // Get current weight
        let q = SpecificEdgeQuery::single(edge.clone())
            .properties()?
            .name(weight_prop);
        let props_results = datastore.get(q)?;

        let current_weight = props_results
            .first()
            .and_then(|res| match res {
                QueryOutputValue::EdgeProperties(props) => props.first(),
                _ => None,
            })
            .and_then(|prop| prop.props.first())
            .and_then(|p| {
                p.value.as_u64().or_else(|| {
                    serde_json::from_value::<String>(p.value.deref().clone())
                        .ok()
                        .and_then(|s| s.parse::<u64>().ok())
                })
            })
            .unwrap_or(1);

        let new_weight = current_weight + 1;
        let q = SpecificEdgeQuery::single(edge.clone());
        datastore.set_properties(q, weight_prop, &Json::new(json!(new_weight)))?;

        edge_cache.insert(key, new_weight);
        return Ok(());
    }

    // Create new edge
    datastore.create_edge(&edge)?;

    // Initialize properties
    let q = SpecificEdgeQuery::single(edge);
    datastore.set_properties(q, weight_prop, &Json::new(json!(1u64)))?;

    edge_cache.insert(key, 1);

    Ok(())
}
