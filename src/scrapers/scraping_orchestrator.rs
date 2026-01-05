use crate::config::Config;
use crate::db::ingestion::PublicationRecord;
use crate::logger;
use crate::scrapers::{ArxivScraper, DblpScraper, Scraper, ZbmathScraper};
use crate::utilities::thread_safe_queue::{QueueConfig, ThreadSafeQueue};
use bloomfilter::Bloom;
use chrono::{DateTime, Utc};
use indradb::{
    BulkInsertItem, Database, Datastore, Edge, Identifier, Json, QueryExt, QueryOutputValue,
    RangeVertexQuery, SpecificEdgeQuery, SpecificVertexQuery, Vertex,
};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::time::Duration as StdDuration; // Added for timeout logic
use textdistance::Algorithm;
use textdistance::Cosine;
use uuid::Uuid;

use std::time::Instant;

/// Represents a buffered write operation for the database.
#[derive(Debug, Clone)]
pub(crate) enum WriteOperation {
    CreateVertex(Vertex),
    CreateEdge(Edge),
    SetVertexProperties(Uuid, Identifier, Json),
    SetEdgeProperties(Edge, Identifier, Json),
}

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
    /// Bloom filter for edges to reduce DB lookups
    edge_filter: Bloom<String>,
    /// Track if edges have been loaded
    edges_loaded: bool,
}

impl DeduplicationCache {
    pub(crate) fn new(bloom_size: usize) -> Self {
        Self {
            loaded_years: HashSet::new(),
            title_filter: Bloom::new_for_fp_rate(bloom_size, 0.01).unwrap(),
            author_filter: Bloom::new_for_fp_rate(bloom_size, 0.01).unwrap(),
            published_ids: HashSet::new(),
            edge_filter: Bloom::new_for_fp_rate(bloom_size * 10, 0.01).unwrap(),
            edges_loaded: false,
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
    pub write_buffer: Vec<WriteOperation>,
}

impl IngestionContext {
    pub(crate) fn new(bloom_size: usize) -> Self {
        Self {
            dedup_cache: DeduplicationCache::new(bloom_size),
            author_cache: HashMap::new(),
            edge_cache: HashMap::new(),
            write_buffer: Vec::with_capacity(1000),
        }
    }

    /// Preloads all edges into the bloom filter
    fn preload_edge_bloom(
        &mut self,
        datastore: &Database<impl Datastore>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if self.dedup_cache.edges_loaded {
            return Ok(());
        }

        let start = Instant::now();
        logger::info("Preloading edge bloom filter...");

        if let Ok(edge_type) = Identifier::new(crate::db::schema::COAUTHORED_WITH_TYPE) {
            let q = RangeVertexQuery::new();
            let q_out = q.outbound()?;
            let q_edges = q_out.t(edge_type);
            if let Ok(results) = datastore.get(q_edges)
                && let Some(QueryOutputValue::Edges(edges)) = results.first()
            {
                let count = edges.len();
                for edge in edges {
                    let edge_key = format!("{}-{}", edge.outbound_id, edge.inbound_id);
                    self.dedup_cache.edge_filter.set(&edge_key);
                }
                logger::info(&format!(
                    "Preloaded {} edges into bloom filter in {}ms",
                    count,
                    start.elapsed().as_millis()
                ));
            }
        }
        self.dedup_cache.edges_loaded = true;
        Ok(())
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
                        #[allow(clippy::collapsible_if)]
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

                    // Update bloom filter
                    let edge_key = format!("{}-{}", edge.outbound_id, edge.inbound_id);
                    self.dedup_cache.edge_filter.set(&edge_key);
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

    // Preload authors and edges to speed up ingestion
    if let Err(e) = context.preload_authors(datastore) {
        logger::error(&format!("Failed to preload authors: {}", e));
    }
    if let Err(e) = context.preload_edge_bloom(datastore) {
        logger::error(&format!("Failed to preload edge bloom filter: {}", e));
    }

    let loop_start_time = Instant::now();
    let mut last_log_time = Instant::now();
    let mut last_log_count = 0;

    // Batching configuration
    const BATCH_SIZE: usize = 100;
    const BATCH_TIMEOUT: StdDuration = StdDuration::from_millis(200);
    let mut batch: Vec<PublicationRecord> = Vec::with_capacity(BATCH_SIZE);
    let mut last_batch_flush = Instant::now();

    loop {
        // Attempt to dequeue
        let record_opt = queue.dequeue();
        let is_none = record_opt.is_none();

        if let Some(record) = record_opt {
            batch.push(record);
        }

        let should_flush = batch.len() >= BATCH_SIZE
            || (!batch.is_empty() && last_batch_flush.elapsed() >= BATCH_TIMEOUT)
            || (is_none && queue.producers_finished() && !batch.is_empty());

        if should_flush {
            let batch_len = batch.len();
            if let Err(e) =
                ingest_batch(std::mem::take(&mut batch), datastore, config, &mut context).await
            {
                logger::error(&format!("Failed to ingest batch: {}", e));
            }
            ingested_count += batch_len;
            last_batch_flush = Instant::now();

            if ingested_count - last_log_count >= 100 {
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
        }

        if is_none {
            if queue.producers_finished() && batch.is_empty() {
                break;
            }
            // Sleep only if we didn't just flush (to avoid busy loop during batch accumulation if queue is slow)
            if !should_flush {
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await; // Reduced sleep for responsiveness
            }
        }
    }

    // Cleanup
    for task in tasks {
        let _ = task.await;
    }

    logger::info("Scraping orchestration finished for current chunk");

    Ok(())
}

/// Flushes the write buffer to the database using bulk operations.
pub(crate) fn flush_buffer(
    buffer: &mut Vec<WriteOperation>,
    datastore: &mut Database<impl Datastore>,
) -> Result<(), Box<dyn std::error::Error>> {
    if buffer.is_empty() {
        return Ok(());
    }

    let start = Instant::now();
    let count = buffer.len();
    let mut bulk_items = Vec::with_capacity(count);

    for op in buffer.drain(..) {
        match op {
            WriteOperation::CreateVertex(v) => bulk_items.push(BulkInsertItem::Vertex(v)),
            WriteOperation::CreateEdge(e) => bulk_items.push(BulkInsertItem::Edge(e)),
            WriteOperation::SetVertexProperties(id, name, val) => {
                bulk_items.push(BulkInsertItem::VertexProperty(id, name, val));
            }
            WriteOperation::SetEdgeProperties(edge, name, val) => {
                bulk_items.push(BulkInsertItem::EdgeProperty(edge, name, val));
            }
        }
    }

    datastore.bulk_insert(bulk_items)?;

    logger::debug(&format!(
        "Flushed {} operations in {}ms",
        count,
        start.elapsed().as_millis()
    ));

    Ok(())
}

/// Ingests a batch of publication records into the database.
pub(crate) async fn ingest_batch(
    records: Vec<PublicationRecord>,
    datastore: &mut Database<impl Datastore>,
    config: &Config,
    context: &mut IngestionContext,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let records_len = records.len();
    if records.is_empty() {
        return Ok(());
    }

    // 1. Deduplication
    let mut new_records = Vec::with_capacity(records.len());
    let mut batch_seen_ids = HashSet::new();

    for record in records {
        // Check if we've already seen this ID in the current batch
        if batch_seen_ids.contains(&record.id) {
            continue;
        }

        if !publication_exists(&record, datastore, config, &mut context.dedup_cache) {
            new_records.push(record.clone());
            batch_seen_ids.insert(record.id.clone());
        }
    }

    if new_records.is_empty() {
        return Ok(());
    }

    // 2. Author Resolution & Creation
    let mut batch_authors = HashSet::new();
    for record in &new_records {
        for author in &record.authors {
            batch_authors.insert(author.clone());
        }
    }

    // Ensure all authors exist
    // Note: get_or_create_author_vertex checks cache first.
    // We can optimize this further by batch checking, but get_or_create is reasonably fast with cache.
    // Ideally we would filter for unknown authors and bulk create, but existing helper is singular.
    // For now, we iterate, but since cache is preloaded/warm, it should be fast.
    for author in &batch_authors {
        get_or_create_author_vertex(author, &mut context.write_buffer, &mut context.author_cache)?;
    }

    // Collection of author vertices for each record to avoid re-lookup
    let mut record_author_vertices: Vec<Vec<Vertex>> = Vec::with_capacity(new_records.len());
    let mut all_involved_authors: HashSet<Uuid> = HashSet::new();

    // 3. Publication Creation
    for record in &new_records {
        let pub_vertex = add_publication(record, &mut context.write_buffer)?;
        context.dedup_cache.add(
            record.year,
            pub_vertex.id,
            record.title.clone(),
            &record.authors,
            record.id.clone(),
        );

        let mut authors_for_rec = Vec::new();
        for author_name in &record.authors {
            let norm = normalize_author(author_name);
            if let Some(v) = context.author_cache.get(&norm) {
                authors_for_rec.push(v.clone());
                all_involved_authors.insert(v.id);
                create_authored_edge(v, &pub_vertex, &mut context.write_buffer)?;
            }
        }
        record_author_vertices.push(authors_for_rec);
    }

    // 4. Edge Prefetching & Creation
    // We collect all unique authors involved in this batch to prefetch edges between them.
    // Convert back to Vec<Vertex> for prefetch
    let unique_author_vertices: Vec<Vertex> = context
        .author_cache
        .values()
        .filter(|v| all_involved_authors.contains(&v.id))
        .cloned()
        .collect();

    #[allow(clippy::collapsible_if)]
    if !unique_author_vertices.is_empty() {
        if let Err(e) = context.prefetch_coauthor_edges(&unique_author_vertices, datastore) {
            logger::error(&format!("Failed to prefetch batch co-author edges: {}", e));
        }
    }

    // Create co-author edges
    for authors in record_author_vertices {
        for i in 0..authors.len() {
            for j in (i + 1)..authors.len() {
                let author1 = &authors[i];
                let author2 = &authors[j];
                create_coauthor_edge(
                    author1,
                    author2,
                    datastore,
                    &mut context.write_buffer,
                    &mut context.edge_cache,
                    &context.dedup_cache.edge_filter,
                )?;
                create_coauthor_edge(
                    author2,
                    author1,
                    datastore,
                    &mut context.write_buffer,
                    &mut context.edge_cache,
                    &context.dedup_cache.edge_filter,
                )?;
            }
        }
    }

    flush_buffer(&mut context.write_buffer, datastore)?;

    logger::debug(&format!(
        "Ingested batch of {} records ({} new) in {}ms",
        records_len,
        new_records.len(),
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
    write_buffer: &mut Vec<WriteOperation>,
) -> Result<Vertex, Box<dyn std::error::Error>> {
    use crate::db::schema::PUBLICATION_TYPE;

    let vertex_type = Identifier::new(PUBLICATION_TYPE)?;
    let vertex = Vertex::new(vertex_type);

    write_buffer.push(WriteOperation::CreateVertex(vertex.clone()));

    let properties = vec![
        ("title", json!(record.title)),
        ("year", json!(record.year.to_string())),
        ("venue", json!(record.venue.clone().unwrap_or_default())),
        ("publication_id", json!(record.id)),
    ];

    for (name, value) in properties {
        let prop_name = Identifier::new(name)?;
        let json_val = Json::new(value);
        write_buffer.push(WriteOperation::SetVertexProperties(
            vertex.id, prop_name, json_val,
        ));
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
    write_buffer: &mut Vec<WriteOperation>,
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
    write_buffer.push(WriteOperation::CreateVertex(vertex.clone()));

    // Set properties
    let properties = vec![
        ("name", json!(author_name)),
        ("erdos_number", json!("None")),
        ("is_erdos", json!("false")),
        ("aliases", json!("[]")),
        ("updated_at", json!(Utc::now().timestamp().to_string())),
    ];

    for (name, value) in properties {
        let prop_name = Identifier::new(name)?;
        let json_val = Json::new(value);
        write_buffer.push(WriteOperation::SetVertexProperties(
            vertex.id, prop_name, json_val,
        ));
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
    write_buffer: &mut Vec<WriteOperation>,
) -> Result<(), Box<dyn std::error::Error>> {
    use crate::db::schema::AUTHORED_TYPE;

    let edge_type = Identifier::new(AUTHORED_TYPE)?;
    let edge = Edge::new(author.id, edge_type, publication.id);

    write_buffer.push(WriteOperation::CreateEdge(edge));

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
    write_buffer: &mut Vec<WriteOperation>,
    edge_cache: &mut HashMap<(Uuid, Uuid), u64>,
    edge_bloom: &Bloom<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    use crate::db::schema::COAUTHORED_WITH_TYPE;

    let edge_type = Identifier::new(COAUTHORED_WITH_TYPE)?;
    let edge = Edge::new(author1.id, edge_type, author2.id);
    let weight_prop = Identifier::new("weight")?;
    let key = (author1.id, author2.id);

    // Check cache first
    if let Some(&weight) = edge_cache.get(&key) {
        let new_weight = weight + 1;
        write_buffer.push(WriteOperation::SetEdgeProperties(
            edge,
            weight_prop,
            Json::new(json!(new_weight)),
        ));
        edge_cache.insert(key, new_weight);
        return Ok(());
    }

    // Check bloom filter before DB query
    let edge_key = format!("{}-{}", author1.id, author2.id);
    let maybe_exists = edge_bloom.check(&edge_key);

    if !maybe_exists {
        // Definitely doesn't exist, create new edge
        write_buffer.push(WriteOperation::CreateEdge(edge.clone()));
        write_buffer.push(WriteOperation::SetEdgeProperties(
            edge,
            weight_prop,
            Json::new(json!(1u64)),
        ));
        edge_cache.insert(key, 1);
        return Ok(());
    }

    // Might exist, check DB
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
        write_buffer.push(WriteOperation::SetEdgeProperties(
            edge,
            weight_prop,
            Json::new(json!(new_weight)),
        ));

        edge_cache.insert(key, new_weight);
        return Ok(());
    }

    // Create new edge
    write_buffer.push(WriteOperation::CreateEdge(edge.clone()));
    write_buffer.push(WriteOperation::SetEdgeProperties(
        edge.clone(),
        weight_prop,
        Json::new(json!(1u64)),
    ));

    edge_cache.insert(key, 1);

    Ok(())
}
