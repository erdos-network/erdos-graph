use crate::config::Config;
use crate::db::ingestion::PublicationRecord;
use crate::db::schema::{GraphEdge, GraphVertex};
use crate::logger;
use crate::scrapers::cache::{DeduplicationCache, EdgeCacheSystem, normalize_author};
use bumpalo::Bump;
use heed3::PutFlags;
use helix_db::helix_engine::storage_core::HelixGraphStorage;
use helix_db::helix_engine::traversal_core::HelixGraphEngine;
use helix_db::protocol::value::Value as HelixValue;
use helix_db::utils::items::{Edge, Node};
use helix_db::utils::properties::ImmutablePropertiesMap;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

/// Represents a buffered write operation for the database.
#[derive(Debug, Clone)]
pub(crate) enum WriteOperation {
    CreateVertex(GraphVertex),
    CreateEdge(GraphEdge),
}

/// Context for ingestion to maintain caches
pub(crate) struct IngestionContext {
    pub dedup_cache: DeduplicationCache,
    pub author_cache: HashMap<String, GraphVertex>,
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
        _engine: &Arc<HelixGraphEngine>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if self.edge_cache.edges_loaded {
            return Ok(());
        }
        logger::info("Skipping full edge preloading for HelixDB (optimization pending).");
        self.edge_cache.edges_loaded = true;
        Ok(())
    }

    /// Preloads all author vertices into the cache
    pub(crate) fn preload_authors(
        &mut self,
        _engine: &Arc<HelixGraphEngine>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        logger::info("Skipping full author preloading for HelixDB (optimization pending).");
        Ok(())
    }

    /// Prefetch specific edge edges
    pub(crate) fn prefetch_coauthor_edges(
        &mut self,
        edge_keys: &[(u128, u128)],
        engine: &Arc<HelixGraphEngine>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if edge_keys.is_empty() {
            return Ok(());
        }

        let txn = engine.storage.graph_env.read_txn()?;
        let arena = Bump::new();

        use crate::db::schema::COAUTHORED_WITH_TYPE;
        let label_hash = helix_db::utils::label_hash::hash_label(COAUTHORED_WITH_TYPE, None);

        // Optimization: Group by source to avoid creating an iterator for every pair
        let mut adjacency_requests: HashMap<u128, HashSet<u128>> = HashMap::new();
        for &(u, v) in edge_keys {
            if self.edge_cache.get((u, v)).is_none() {
                adjacency_requests.entry(u).or_default().insert(v);
            }
        }

        for (u, targets) in adjacency_requests {
            let out_key = HelixGraphStorage::out_edge_key(&u, &label_hash);

            if let Ok(iter) = engine.storage.out_edges_db.prefix_iter(&txn, &out_key) {
                for (_, val_bytes) in iter.flatten() {
                    if val_bytes.len() >= 32 {
                        let mut to_node_bytes = [0u8; 16];
                        to_node_bytes.copy_from_slice(&val_bytes[16..32]);
                        let to_node_id = u128::from_be_bytes(to_node_bytes);

                        if targets.contains(&to_node_id) {
                            let mut edge_id_bytes = [0u8; 16];
                            edge_id_bytes.copy_from_slice(&val_bytes[0..16]);
                            let edge_id = u128::from_be_bytes(edge_id_bytes);

                            if let Ok(Some(edge_bytes)) =
                                engine.storage.edges_db.get(&txn, &edge_id)
                                && let Ok(edge) =
                                    Edge::from_bincode_bytes(edge_id, edge_bytes, &arena)
                                && let Some(props) = edge.properties
                                && let Some(w_val) = props.get("weight")
                            {
                                let weight = match w_val {
                                    HelixValue::U64(w) => *w,
                                    HelixValue::I64(w) => *w as u64,
                                    HelixValue::F64(w) => *w as u64,
                                    _ => 1,
                                };
                                self.edge_cache.put((u, to_node_id), weight);
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

fn json_to_helix_value(v: &Value) -> HelixValue {
    match v {
        Value::Null => HelixValue::Empty,
        Value::Bool(b) => HelixValue::Boolean(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                HelixValue::I64(i)
            } else if let Some(u) = n.as_u64() {
                HelixValue::U64(u)
            } else if let Some(f) = n.as_f64() {
                HelixValue::F64(f)
            } else {
                HelixValue::F64(0.0)
            }
        }
        Value::String(s) => HelixValue::String(s.clone()),
        Value::Array(arr) => {
            let vec: Vec<HelixValue> = arr.iter().map(json_to_helix_value).collect();
            HelixValue::Array(vec)
        }
        Value::Object(obj) => {
            let map: HashMap<String, HelixValue> = obj
                .iter()
                .map(|(k, v)| (k.clone(), json_to_helix_value(v)))
                .collect();
            HelixValue::Object(map)
        }
    }
}

fn value_to_string(v: &HelixValue) -> String {
    match v {
        HelixValue::String(s) => s.clone(),
        HelixValue::I64(i) => i.to_string(),
        HelixValue::U64(u) => u.to_string(),
        HelixValue::F64(f) => f.to_string(),
        HelixValue::Boolean(b) => b.to_string(),
        _ => String::new(),
    }
}

/// Helper to convert our generic GraphVertex to Helix Node
fn to_helix_node<'arena>(v: &GraphVertex, arena: &'arena Bump) -> Node<'arena> {
    let items = v
        .props
        .iter()
        .map(|(k, val)| (arena.alloc_str(k) as &str, json_to_helix_value(val)));

    let props = if v.props.is_empty() {
        None
    } else {
        Some(ImmutablePropertiesMap::new(v.props.len(), items, arena))
    };

    Node {
        id: v.id.as_u128(),
        label: arena.alloc_str(&v.t),
        version: 1,
        properties: props,
    }
}

/// Helper to convert our generic GraphEdge to Helix Edge
fn to_helix_edge<'arena>(e: &GraphEdge, id: u128, arena: &'arena Bump) -> Edge<'arena> {
    let items = e
        .props
        .iter()
        .map(|(k, val)| (arena.alloc_str(k) as &str, json_to_helix_value(val)));

    let props = if e.props.is_empty() {
        None
    } else {
        Some(ImmutablePropertiesMap::new(e.props.len(), items, arena))
    };

    Edge {
        id,
        label: arena.alloc_str(&e.t),
        version: 1,
        from_node: e.source_id.as_u128(),
        to_node: e.target_id.as_u128(),
        properties: props,
    }
}

/// Flushes the write buffer to the database using bulk operations.
pub(crate) fn flush_buffer(
    buffer: &mut Vec<WriteOperation>,
    engine: &Arc<HelixGraphEngine>,
) -> Result<(), Box<dyn std::error::Error>> {
    if buffer.is_empty() {
        return Ok(());
    }

    let start = Instant::now();
    let count = buffer.len();
    let mut wtxn = engine.storage.graph_env.write_txn()?;
    let arena = Bump::new();

    for op in buffer.drain(..) {
        match op {
            WriteOperation::CreateVertex(v) => {
                let node = to_helix_node(&v, &arena);
                if let Ok(bytes) = bincode::serialize(&node) {
                    // Use APPEND flag for sorted inserts (LMDB optimization)
                    engine.storage.nodes_db.put_with_flags(
                        &mut wtxn,
                        PutFlags::APPEND,
                        &node.id,
                        &bytes,
                    )?;

                    for (idx_name, (db, _)) in &engine.storage.secondary_indices {
                        if let Some(val) = node.get_property(idx_name) {
                            let val_str = value_to_string(val);
                            if let Ok(key_bytes) = bincode::serialize(&HelixValue::String(val_str))
                            {
                                // For secondary indices (DUP_SORT), empty flags adds to the set
                                db.put_with_flags(
                                    &mut wtxn,
                                    PutFlags::empty(),
                                    &key_bytes,
                                    &node.id,
                                )?;
                            }
                        }
                    }
                }
            }
            WriteOperation::CreateEdge(e) => {
                let edge_id = uuid::Uuid::now_v7().as_u128();
                let edge = to_helix_edge(&e, edge_id, &arena);

                if let Ok(bytes) = bincode::serialize(&edge) {
                    // Use APPEND flag for sorted inserts (LMDB optimization)
                    engine.storage.edges_db.put_with_flags(
                        &mut wtxn,
                        PutFlags::APPEND,
                        &edge.id,
                        &bytes,
                    )?;

                    let label_hash = helix_db::utils::label_hash::hash_label(edge.label, None);

                    // Use APPEND_DUP because edge_id (part of value) is monotonic
                    engine.storage.out_edges_db.put_with_flags(
                        &mut wtxn,
                        PutFlags::APPEND_DUP,
                        &HelixGraphStorage::out_edge_key(&edge.from_node, &label_hash),
                        &HelixGraphStorage::pack_edge_data(&edge.id, &edge.to_node),
                    )?;

                    engine.storage.in_edges_db.put_with_flags(
                        &mut wtxn,
                        PutFlags::APPEND_DUP,
                        &HelixGraphStorage::in_edge_key(&edge.to_node, &label_hash),
                        &HelixGraphStorage::pack_edge_data(&edge.id, &edge.from_node),
                    )?;
                }
            }
        }
    }

    wtxn.commit()?;

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
    engine: Arc<HelixGraphEngine>,
    config: &Config,
    context: &mut IngestionContext,
) -> Result<(), Box<dyn std::error::Error>> {
    let _start = Instant::now();
    if records.is_empty() {
        return Ok(());
    }

    let mut new_records = Vec::with_capacity(records.len());
    let mut batch_seen_ids = HashSet::new();

    for record in records {
        if batch_seen_ids.contains(&record.id) {
            continue;
        }

        if !context
            .dedup_cache
            .check_exists_and_cache(&record, &engine, config)
        {
            new_records.push(record.clone());
            batch_seen_ids.insert(record.id.clone());
        }
    }

    if new_records.is_empty() {
        return Ok(());
    }

    let mut batch_authors = HashSet::new();
    for record in &new_records {
        for author in &record.authors {
            batch_authors.insert(author.clone());
        }
    }

    for author in &batch_authors {
        get_or_create_author_vertex(author, &mut context.write_buffer, &mut context.author_cache)?;
    }

    let mut record_author_vertices: Vec<Vec<GraphVertex>> = Vec::with_capacity(new_records.len());
    let mut all_involved_authors: HashSet<Uuid> = HashSet::new();

    for record in &new_records {
        let pub_vertex = add_publication(record, &mut context.write_buffer)?;
        context.dedup_cache.add(
            record.year,
            pub_vertex.id.to_string(),
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

    let mut edges_to_prefetch = HashSet::new();
    for authors in &record_author_vertices {
        for i in 0..authors.len() {
            for j in (i + 1)..authors.len() {
                let id1 = authors[i].id.as_u128();
                let id2 = authors[j].id.as_u128();
                edges_to_prefetch.insert((id1, id2));
                edges_to_prefetch.insert((id2, id1));
            }
        }
    }

    let edges_vec: Vec<(u128, u128)> = edges_to_prefetch.into_iter().collect();

    if !edges_vec.is_empty()
        && let Err(e) = context.prefetch_coauthor_edges(&edges_vec, &engine)
    {
        logger::error(&format!("Failed to prefetch batch co-author edges: {}", e));
    }

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

    flush_buffer(&mut context.write_buffer, &engine)?;

    Ok(())
}

/// Helper to get or create author vertex
pub(crate) fn get_or_create_author_vertex(
    name: &str,
    write_buffer: &mut Vec<WriteOperation>,
    author_cache: &mut HashMap<String, GraphVertex>,
) -> Result<GraphVertex, Box<dyn std::error::Error>> {
    let norm_name = normalize_author(name);

    if let Some(v) = author_cache.get(&norm_name) {
        return Ok(v.clone());
    }

    let person_type = crate::db::schema::PERSON_TYPE;
    let v = GraphVertex::new(person_type).property("name", name.to_string());

    write_buffer.push(WriteOperation::CreateVertex(v.clone()));

    author_cache.insert(norm_name, v.clone());
    Ok(v)
}

/// Helper to add publication vertex
pub(crate) fn add_publication(
    record: &PublicationRecord,
    write_buffer: &mut Vec<WriteOperation>,
) -> Result<GraphVertex, Box<dyn std::error::Error>> {
    let type_id = crate::db::schema::PUBLICATION_TYPE;
    let mut v = GraphVertex::new(type_id)
        .property("title", record.title.clone())
        .property("year", record.year.to_string())
        .property("publication_id", record.id.clone())
        .property("source", record.source.clone());

    if let Some(venue) = &record.venue {
        v = v.property("venue", venue.clone());
    }

    write_buffer.push(WriteOperation::CreateVertex(v.clone()));

    Ok(v)
}

/// Create edge between author and publication
pub(crate) fn create_authored_edge(
    author: &GraphVertex,
    publication: &GraphVertex,
    write_buffer: &mut Vec<WriteOperation>,
) -> Result<(), Box<dyn std::error::Error>> {
    let edge_type = crate::db::schema::AUTHORED_TYPE;
    let edge = GraphEdge::new(author.id, publication.id, edge_type);
    write_buffer.push(WriteOperation::CreateEdge(edge));
    Ok(())
}

/// Create co-author edge with weight
pub(crate) fn create_coauthor_edge(
    author1: &GraphVertex,
    author2: &GraphVertex,
    write_buffer: &mut Vec<WriteOperation>,
    edge_cache: &mut EdgeCacheSystem,
) -> Result<(), Box<dyn std::error::Error>> {
    use crate::db::schema::COAUTHORED_WITH_TYPE;

    let edge_type = COAUTHORED_WITH_TYPE;
    let (u, v) = if author1.id < author2.id {
        (author1, author2)
    } else {
        (author2, author1)
    };

    let key = (u.id.as_u128(), v.id.as_u128());

    let mut add_ops = |weight: u64| -> Result<(), Box<dyn std::error::Error>> {
        let edge1 = GraphEdge::new(u.id, v.id, edge_type).property("weight", weight);
        write_buffer.push(WriteOperation::CreateEdge(edge1));

        let edge2 = GraphEdge::new(v.id, u.id, edge_type).property("weight", weight);
        write_buffer.push(WriteOperation::CreateEdge(edge2));
        Ok(())
    };

    if let Some(current_weight) = edge_cache.get(key) {
        let new_weight = current_weight + 1;
        edge_cache.put(key, new_weight);
        add_ops(new_weight)?;
        return Ok(());
    }

    if edge_cache.exists(key) {
        edge_cache.put(key, 2);
        add_ops(2)?;
        return Ok(());
    }

    edge_cache.put(key, 1);
    add_ops(1)?;

    Ok(())
}

/// Helper to check if publication exists in DB (using Bloom filter as first line of defense)
#[allow(dead_code)]
pub(crate) fn publication_exists(
    record: &PublicationRecord,
    engine: &Arc<HelixGraphEngine>,
    config: &Config,
    cache: &mut DeduplicationCache,
) -> bool {
    cache.check_exists_and_cache(record, engine, config)
}
