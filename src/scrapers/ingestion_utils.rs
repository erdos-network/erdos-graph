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

/// Statistics for analyzing mega-paper edge processing.
#[derive(Default)]
struct MegaPaperStats {
    total_pairs: usize,
    bloom_hits: usize,
    cache_hits: usize,
}

/// Context for ingestion to maintain caches
pub(crate) struct IngestionContext {
    pub dedup_cache: DeduplicationCache,
    pub author_cache: HashMap<String, GraphVertex>,
    pub edge_cache: EdgeCacheSystem,
    pub write_buffer: Vec<WriteOperation>,
    pub pending_edge_updates: HashMap<(u128, u128), u64>,
}

impl IngestionContext {
    pub(crate) fn new(config: &Config) -> Self {
        Self {
            dedup_cache: DeduplicationCache::new(config.deduplication.bloom_filter_size),
            author_cache: HashMap::new(),
            edge_cache: EdgeCacheSystem::new(config.edge_cache.clone()),
            write_buffer: Vec::with_capacity(config.ingestion.write_buffer_capacity),
            pending_edge_updates: HashMap::new(),
        }
    }

    /// Preloads all edges into the bloom filter
    pub(crate) fn preload_edge_bloom(
        &mut self,
        engine: &Arc<HelixGraphEngine>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if self.edge_cache.edges_loaded {
            return Ok(());
        }
        let start = Instant::now();
        let mut count = 0;
        let txn = engine.storage.graph_env.read_txn()?;
        let arena = Bump::new();

        if let Ok(iter) = engine.storage.edges_db.iter(&txn) {
            for (edge_id, edge_bytes) in iter.flatten() {
                if let Ok(edge) = Edge::from_bincode_bytes(edge_id, edge_bytes, &arena)
                    && edge.label == crate::db::schema::COAUTHORED_WITH_TYPE
                {
                    let u = edge.from_node;
                    let v = edge.to_node;
                    self.edge_cache.cold_bloom.set(&(u, v));
                    self.edge_cache.cold_bloom.set(&(v, u));
                    count += 1;
                }
            }
        }

        self.edge_cache.edges_loaded = true;
        logger::info(&format!(
            "Preloaded {} edges into Bloom filter in {}ms",
            count,
            start.elapsed().as_millis()
        ));
        Ok(())
    }

    /// Preloads all author vertices into the cache
    pub(crate) fn preload_authors(
        &mut self,
        engine: &Arc<HelixGraphEngine>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let start = Instant::now();
        let mut count = 0;
        let txn = engine.storage.graph_env.read_txn()?;

        if let Some((name_index_db, _)) = engine.storage.secondary_indices.get("name")
            && let Ok(iter) = name_index_db.iter(&txn)
        {
            for (key_bytes, node_id) in iter.flatten() {
                if let Ok(HelixValue::String(name)) = bincode::deserialize(key_bytes) {
                    let uuid = Uuid::from_u128(node_id);
                    let norm_name = normalize_author(&name);
                    let v = GraphVertex::with_id(uuid, crate::db::schema::PERSON_TYPE)
                        .property("name", name);
                    self.author_cache.insert(norm_name, v);
                    count += 1;
                }
            }
        }

        logger::info(&format!(
            "Preloaded {} authors in {}ms",
            count,
            start.elapsed().as_millis()
        ));
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

/// Flushes the write buffer and pending edge updates to the database.
pub(crate) fn flush_buffer(
    context: &mut IngestionContext,
    engine: &Arc<HelixGraphEngine>,
) -> Result<(), Box<dyn std::error::Error>> {
    if context.write_buffer.is_empty() && context.pending_edge_updates.is_empty() {
        return Ok(());
    }

    let start = Instant::now();
    let mut count = context.write_buffer.len();
    let mut wtxn = engine.storage.graph_env.write_txn()?;
    let arena = Bump::new();

    // Convert pending edge updates to bidirectional write operations
    for ((u_id, v_id), final_weight) in context.pending_edge_updates.drain() {
        let u_uuid = Uuid::from_u128(u_id);
        let v_uuid = Uuid::from_u128(v_id);

        use crate::db::schema::COAUTHORED_WITH_TYPE;
        let edge1 =
            GraphEdge::new(u_uuid, v_uuid, COAUTHORED_WITH_TYPE).property("weight", final_weight);
        context.write_buffer.push(WriteOperation::CreateEdge(edge1));

        let edge2 =
            GraphEdge::new(v_uuid, u_uuid, COAUTHORED_WITH_TYPE).property("weight", final_weight);
        context.write_buffer.push(WriteOperation::CreateEdge(edge2));

        count += 2;
    }

    for op in context.write_buffer.drain(..) {
        match op {
            WriteOperation::CreateVertex(v) => {
                let node = to_helix_node(&v, &arena);
                if let Ok(bytes) = bincode::serialize(&node) {
                    engine.storage.nodes_db.put_with_flags(
                        &mut wtxn,
                        PutFlags::empty(),
                        &node.id,
                        &bytes,
                    )?;

                    for (idx_name, (db, _)) in &engine.storage.secondary_indices {
                        if let Some(val) = node.get_property(idx_name) {
                            let val_str = value_to_string(val);
                            if let Ok(key_bytes) = bincode::serialize(&HelixValue::String(val_str))
                            {
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
                    engine.storage.edges_db.put_with_flags(
                        &mut wtxn,
                        PutFlags::APPEND,
                        &edge.id,
                        &bytes,
                    )?;

                    let label_hash = helix_db::utils::label_hash::hash_label(edge.label, None);

                    engine.storage.out_edges_db.put(
                        &mut wtxn,
                        &HelixGraphStorage::out_edge_key(&edge.from_node, &label_hash),
                        &HelixGraphStorage::pack_edge_data(&edge.id, &edge.to_node),
                    )?;

                    engine.storage.in_edges_db.put(
                        &mut wtxn,
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

/// Bulk prefetch co-author edges for a set of authors to warm the cache.
fn bulk_prefetch_author_edges(
    author_ids: &HashSet<u128>,
    engine: &Arc<HelixGraphEngine>,
    edge_cache: &mut EdgeCacheSystem,
    max_edges_per_author: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    if author_ids.is_empty() {
        return Ok(());
    }

    let txn = engine.storage.graph_env.read_txn()?;
    let arena = Bump::new();

    use crate::db::schema::COAUTHORED_WITH_TYPE;
    let label_hash = helix_db::utils::label_hash::hash_label(COAUTHORED_WITH_TYPE, None);

    for &author_id in author_ids {
        let out_key = HelixGraphStorage::out_edge_key(&author_id, &label_hash);
        let mut edges_fetched = 0;

        if let Ok(iter) = engine.storage.out_edges_db.prefix_iter(&txn, &out_key) {
            for (_, val_bytes) in iter.flatten() {
                if edges_fetched >= max_edges_per_author {
                    break;
                }

                if val_bytes.len() >= 32 {
                    let mut to_node_bytes = [0u8; 16];
                    to_node_bytes.copy_from_slice(&val_bytes[16..32]);
                    let to_node_id = u128::from_be_bytes(to_node_bytes);

                    let mut edge_id_bytes = [0u8; 16];
                    edge_id_bytes.copy_from_slice(&val_bytes[0..16]);
                    let edge_id = u128::from_be_bytes(edge_id_bytes);

                    if let Ok(Some(edge_bytes)) = engine.storage.edges_db.get(&txn, &edge_id)
                        && let Ok(edge) = Edge::from_bincode_bytes(edge_id, edge_bytes, &arena)
                        && let Some(props) = edge.properties
                        && let Some(w_val) = props.get("weight")
                    {
                        let weight = match w_val {
                            HelixValue::U64(w) => *w,
                            HelixValue::I64(w) => *w as u64,
                            HelixValue::F64(w) => *w as u64,
                            _ => 1,
                        };

                        let key = if author_id < to_node_id {
                            (author_id, to_node_id)
                        } else {
                            (to_node_id, author_id)
                        };
                        edge_cache.put(key, weight);
                        edges_fetched += 1;
                    }
                }
            }
        }
    }

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

    use bloomfilter::Bloom;

    // Collect all unique authors and identify mega-papers
    let mut batch_author_ids: HashSet<u128> = HashSet::new();
    let mut has_mega_paper = false;
    let mega_threshold = config.ingestion.mega_paper_threshold;

    for authors in &record_author_vertices {
        if authors.len() > mega_threshold {
            has_mega_paper = true;
        }
        for author in authors {
            batch_author_ids.insert(author.id.as_u128());
        }
    }

    // Always prefetch to warm cache for subsequent normal papers
    let prefetch_start = Instant::now();
    if let Err(e) = bulk_prefetch_author_edges(
        &batch_author_ids,
        &engine,
        &mut context.edge_cache,
        config.ingestion.max_edges_per_author,
    ) {
        logger::error(&format!("Failed to bulk prefetch author edges: {}", e));
    }
    let prefetch_time = prefetch_start.elapsed().as_millis();

    // Generate edge pairs with cache warmed
    let estimated_edges = new_records.len() * config.ingestion.estimated_edges_per_paper;
    let mut batch_edge_bloom = Bloom::new_for_fp_rate(estimated_edges, 0.01)
        .unwrap_or_else(|_| Bloom::new_for_fp_rate(10000, 0.01).unwrap());

    let mut batch_edge_weights: HashMap<(u128, u128), u64> = HashMap::new();

    // Track statistics for mega-papers
    let mut mega_paper_stats = MegaPaperStats::default();

    let edge_gen_start = Instant::now();
    for authors in &record_author_vertices {
        let is_mega = authors.len() > mega_threshold;

        for i in 0..authors.len() {
            for j in (i + 1)..authors.len() {
                let id1 = authors[i].id.as_u128();
                let id2 = authors[j].id.as_u128();
                let key = if id1 < id2 { (id1, id2) } else { (id2, id1) };

                if is_mega {
                    mega_paper_stats.total_pairs += 1;

                    // Check bloom first (cheap)
                    if context.edge_cache.cold_bloom.check(&key) {
                        mega_paper_stats.bloom_hits += 1;
                        // Check cache second (more expensive)
                        if context.edge_cache.get(key).is_some() {
                            mega_paper_stats.cache_hits += 1;
                        }
                    }
                }

                if batch_edge_bloom.check(&key) {
                    *batch_edge_weights.entry(key).or_insert(0) += 1;
                } else {
                    batch_edge_bloom.set(&key);
                    batch_edge_weights.insert(key, 1);
                }
            }
        }
    }
    let edge_gen_time = edge_gen_start.elapsed().as_millis();

    if has_mega_paper {
        let bloom_hit_pct = if mega_paper_stats.total_pairs > 0 {
            (mega_paper_stats.bloom_hits as f64 / mega_paper_stats.total_pairs as f64) * 100.0
        } else {
            0.0
        };
        let cache_hit_pct = if mega_paper_stats.total_pairs > 0 {
            (mega_paper_stats.cache_hits as f64 / mega_paper_stats.total_pairs as f64) * 100.0
        } else {
            0.0
        };

        logger::info(&format!(
            "Mega-paper metrics: {} pairs, bloom hits: {} ({:.1}%), cache hits: {} ({:.1}%), prefetch: {}ms, generation: {}ms",
            mega_paper_stats.total_pairs,
            mega_paper_stats.bloom_hits,
            bloom_hit_pct,
            mega_paper_stats.cache_hits,
            cache_hit_pct,
            prefetch_time,
            edge_gen_time
        ));
    }

    // Calculate final weights and prepare for flush
    for (key, batch_increment) in batch_edge_weights {
        let current_weight = context.edge_cache.get(key).unwrap_or(0);
        let final_weight = current_weight + batch_increment;

        context.edge_cache.put(key, final_weight);
        *context.pending_edge_updates.entry(key).or_insert(0) = final_weight;
    }

    flush_buffer(context, &engine)?;

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
