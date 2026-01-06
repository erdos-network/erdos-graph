use crate::config::Config;
use crate::db::ingestion::PublicationRecord;
use crate::logger;
use crate::scrapers::cache::{DeduplicationCache, EdgeCacheSystem, normalize_author};
use indradb::{
    BulkInsertItem, Database, Datastore, Edge, Identifier, Json, QueryExt, QueryOutputValue,
    RangeVertexQuery, SpecificEdgeQuery, Vertex,
};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::time::Instant;
use uuid::Uuid;

/// Represents a buffered write operation for the database.
#[derive(Debug, Clone)]
pub(crate) enum WriteOperation {
    CreateVertex(Vertex),
    CreateEdge(Edge),
    SetVertexProperties(Uuid, Identifier, Json),
    SetEdgeProperties(Edge, Identifier, Json),
}

/// Context for ingestion to maintain caches
pub(crate) struct IngestionContext {
    pub dedup_cache: DeduplicationCache,
    pub author_cache: HashMap<String, Vertex>,
    pub edge_cache: EdgeCacheSystem,
    pub write_buffer: Vec<WriteOperation>,
}

impl IngestionContext {
    pub(crate) fn new(config: &Config) -> Self {
        Self {
            dedup_cache: DeduplicationCache::new(config.deduplication.bloom_filter_size),
            author_cache: HashMap::new(),
            edge_cache: EdgeCacheSystem::new(config.edge_cache.clone()),
            write_buffer: Vec::with_capacity(1000),
        }
    }

    /// Preloads all edges into the bloom filter
    pub(crate) fn preload_edge_bloom(
        &mut self,
        datastore: &Database<impl Datastore>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if self.edge_cache.edges_loaded {
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
                    // Use tuple key for bloom
                    self.edge_cache
                        .cold_bloom
                        .set(&(edge.outbound_id, edge.inbound_id));
                }
                logger::info(&format!(
                    "Preloaded {} edges into bloom filter in {}ms",
                    count,
                    start.elapsed().as_millis()
                ));
            }
        }
        self.edge_cache.edges_loaded = true;
        Ok(())
    }

    /// Preloads all author vertices into the cache
    pub(crate) fn preload_authors(
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

    /// Prefetch specific edge edges
    pub(crate) fn prefetch_coauthor_edges(
        &mut self,
        edge_keys: &[(Uuid, Uuid)],
        datastore: &Database<impl Datastore>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::db::schema::COAUTHORED_WITH_TYPE;

        if edge_keys.is_empty() {
            return Ok(());
        }

        let edge_type = Identifier::new(COAUTHORED_WITH_TYPE)?;
        let mut edges_to_query = Vec::with_capacity(edge_keys.len());

        for &(u, v) in edge_keys {
            // Check cache before query
            // Assume unique keys
            // Check cache to save DB
            if self.edge_cache.get((u, v)).is_none() {
                edges_to_query.push(Edge::new(u, edge_type, v));
            }
        }

        if edges_to_query.is_empty() {
            return Ok(());
        }

        // Fetch properties (weight) for these edges
        let weight_prop = Identifier::new("weight")?;

        // Query edge properties
        let q_props = SpecificEdgeQuery::new(edges_to_query.clone())
            .properties()?
            .name(weight_prop);

        let prop_results = datastore.get(q_props)?;

        if let Some(QueryOutputValue::EdgeProperties(props_list)) = prop_results.first() {
            for edge_prop in props_list {
                let weight = edge_prop
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

                self.edge_cache.put(
                    (edge_prop.edge.outbound_id, edge_prop.edge.inbound_id),
                    weight,
                );
            }
        }

        Ok(())
    }
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
    let _start = Instant::now();
    if records.is_empty() {
        return Ok(());
    }

    // Filter out duplicates
    let mut new_records = Vec::with_capacity(records.len());
    let mut batch_seen_ids = HashSet::new();

    for record in records {
        // Check if we've already seen this ID in the current batch
        if batch_seen_ids.contains(&record.id) {
            continue;
        }

        if !context
            .dedup_cache
            .check_exists_and_cache(&record, datastore, config)
        {
            new_records.push(record.clone());
            batch_seen_ids.insert(record.id.clone());
        }
    }

    if new_records.is_empty() {
        return Ok(());
    }

    // Resolve author vertices
    let mut batch_authors = HashSet::new();
    for record in &new_records {
        for author in &record.authors {
            batch_authors.insert(author.clone());
        }
    }

    // Ensure all authors exist using cache or create new vertices
    for author in &batch_authors {
        get_or_create_author_vertex(author, &mut context.write_buffer, &mut context.author_cache)?;
    }

    // Collect author vertices for each record to avoid re-lookup
    let mut record_author_vertices: Vec<Vec<Vertex>> = Vec::with_capacity(new_records.len());
    let mut all_involved_authors: HashSet<Uuid> = HashSet::new();

    // Create publication vertices
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

    // Handle co-author edges
    // Collect prefetch pairs
    let mut edges_to_prefetch = HashSet::new();

    for authors in &record_author_vertices {
        for i in 0..authors.len() {
            for j in (i + 1)..authors.len() {
                let id1 = authors[i].id;
                let id2 = authors[j].id;
                edges_to_prefetch.insert((id1, id2));
                edges_to_prefetch.insert((id2, id1));
            }
        }
    }

    let edges_vec: Vec<(Uuid, Uuid)> = edges_to_prefetch.into_iter().collect();

    if !edges_vec.is_empty()
        && let Err(e) = context.prefetch_coauthor_edges(&edges_vec, datastore)
    {
        logger::error(&format!("Failed to prefetch batch co-author edges: {}", e));
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
                    &mut context.write_buffer,
                    &mut context.edge_cache,
                )?;
            }
        }
    }

    flush_buffer(&mut context.write_buffer, datastore)?;

    Ok(())
}

/// Helper to check if publication exists in DB (using Bloom filter as first line of defense)
#[allow(dead_code)]
pub(crate) fn publication_exists(
    record: &PublicationRecord,
    datastore: &Database<impl Datastore>,
    config: &Config,
    cache: &mut DeduplicationCache,
) -> bool {
    cache.check_exists_and_cache(record, datastore, config)
}

/// Helper to get or create author vertex
pub(crate) fn get_or_create_author_vertex(
    name: &str,
    write_buffer: &mut Vec<WriteOperation>,
    author_cache: &mut HashMap<String, Vertex>,
) -> Result<Vertex, Box<dyn std::error::Error>> {
    let norm_name = normalize_author(name);

    if let Some(v) = author_cache.get(&norm_name) {
        return Ok(v.clone());
    }

    let person_type = Identifier::new(crate::db::schema::PERSON_TYPE)?;
    let v = Vertex::new(person_type);
    let name_prop = Identifier::new("name")?;

    write_buffer.push(WriteOperation::CreateVertex(v.clone()));
    write_buffer.push(WriteOperation::SetVertexProperties(
        v.id,
        name_prop,
        Json::new(json!(name)),
    ));

    author_cache.insert(norm_name, v.clone());
    Ok(v)
}

/// Helper to add publication vertex
pub(crate) fn add_publication(
    record: &PublicationRecord,
    write_buffer: &mut Vec<WriteOperation>,
) -> Result<Vertex, Box<dyn std::error::Error>> {
    let type_id = Identifier::new(crate::db::schema::PUBLICATION_TYPE)?;
    let v = Vertex::new(type_id);

    // Set properties
    let title_prop = Identifier::new("title")?;
    let year_prop = Identifier::new("year")?;
    let id_prop = Identifier::new("publication_id")?;

    write_buffer.push(WriteOperation::CreateVertex(v.clone()));
    write_buffer.push(WriteOperation::SetVertexProperties(
        v.id,
        title_prop,
        Json::new(json!(record.title)),
    ));
    write_buffer.push(WriteOperation::SetVertexProperties(
        v.id,
        year_prop,
        Json::new(json!(record.year.to_string())),
    ));
    write_buffer.push(WriteOperation::SetVertexProperties(
        v.id,
        id_prop,
        Json::new(json!(record.id)),
    ));

    let source_prop = Identifier::new("source")?;
    write_buffer.push(WriteOperation::SetVertexProperties(
        v.id,
        source_prop,
        Json::new(json!(record.source)),
    ));

    if let Some(venue) = &record.venue {
        let venue_prop = Identifier::new("venue")?;
        write_buffer.push(WriteOperation::SetVertexProperties(
            v.id,
            venue_prop,
            Json::new(json!(venue)),
        ));
    }

    Ok(v)
}

/// Create edge between author and publication
pub(crate) fn create_authored_edge(
    author: &Vertex,
    publication: &Vertex,
    write_buffer: &mut Vec<WriteOperation>,
) -> Result<(), Box<dyn std::error::Error>> {
    let edge_type = Identifier::new(crate::db::schema::AUTHORED_TYPE)?;
    let edge = Edge::new(author.id, edge_type, publication.id);
    write_buffer.push(WriteOperation::CreateEdge(edge));
    Ok(())
}

/// Create co-author edge with weight
pub(crate) fn create_coauthor_edge(
    author1: &Vertex,
    author2: &Vertex,
    write_buffer: &mut Vec<WriteOperation>,
    edge_cache: &mut EdgeCacheSystem,
) -> Result<(), Box<dyn std::error::Error>> {
    use crate::db::schema::COAUTHORED_WITH_TYPE;

    let edge_type = Identifier::new(COAUTHORED_WITH_TYPE)?;
    // Ensure consistent ordering to avoid duplicate edges in undirected graph sense
    let (u, v) = if author1.id < author2.id {
        (author1, author2)
    } else {
        (author2, author1)
    };

    let key = (u.id, v.id);

    // Helper to add operations for both directions
    let mut add_ops = |weight: u64, is_new: bool| -> Result<(), Box<dyn std::error::Error>> {
        let weight_prop = Identifier::new("weight")?;
        let weight_val = Json::new(json!(weight));

        // Forward edge (u -> v)
        if is_new {
            let edge = Edge::new(u.id, edge_type, v.id);
            write_buffer.push(WriteOperation::CreateEdge(edge.clone()));
            write_buffer.push(WriteOperation::SetEdgeProperties(
                edge,
                weight_prop,
                weight_val.clone(),
            ));
        } else {
            let edge = Edge::new(u.id, edge_type, v.id);
            write_buffer.push(WriteOperation::SetEdgeProperties(
                edge,
                weight_prop,
                weight_val.clone(),
            ));
        }

        // Backward edge (v -> u)
        if is_new {
            let edge = Edge::new(v.id, edge_type, u.id);
            write_buffer.push(WriteOperation::CreateEdge(edge.clone()));
            write_buffer.push(WriteOperation::SetEdgeProperties(
                edge,
                weight_prop,
                weight_val,
            ));
        } else {
            let edge = Edge::new(v.id, edge_type, u.id);
            write_buffer.push(WriteOperation::SetEdgeProperties(
                edge,
                weight_prop,
                weight_val,
            ));
        }
        Ok(())
    };

    // Check cache
    if let Some(current_weight) = edge_cache.get(key) {
        let new_weight = current_weight + 1;
        edge_cache.put(key, new_weight);
        add_ops(new_weight, false)?;
        return Ok(());
    }

    // Not in hot/warm cache. Check bloom (cold cache).
    if edge_cache.exists(key) {
        edge_cache.put(key, 2);
        add_ops(2, false)?;
        return Ok(());
    }

    // New edge
    edge_cache.put(key, 1);
    add_ops(1, true)?;

    Ok(())
}
