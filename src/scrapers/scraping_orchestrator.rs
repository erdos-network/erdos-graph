//! Orchestrates the scraping process across multiple publication sources.
//!
//! This module coordinates scraping from ArXiv, DBLP, and zbMATH, managing
//! checkpointing, chunking large date ranges, and ingesting results into the database.

use crate::config::load_config;
use crate::db::ingestion::PublicationRecord;
use crate::thread_safe_queue::{QueueConfig, ThreadSafeQueue};
use chrono::{DateTime, Utc};
use indradb::{
    Database, Datastore, Edge, Identifier, Json, QueryExt, QueryOutputValue, RangeVertexQuery,
    SpecificEdgeQuery, SpecificVertexQuery, Vertex,
};
use serde_json::json;
use std::collections::HashMap;
use std::ops::Deref;
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::Mutex;
use tokio::time::{self, Instant};

/// Orchestrates the complete scraping process for one or more publication sources.
///
/// This function handles:
/// - Spawning child processes to scrape from specified sources
/// - Collecting scraped publication records via a thread-safe queue
/// - Ingesting records into the database
///
/// # Arguments
/// * `start_date` - Start date for scraping
/// * `end_date` - End date for scraping
/// * `sources` - List of sources to scrape ("arxiv", "dblp", or "zbmath")
/// * `datastore` - Mutable reference to the IndraDB datastore
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
) -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration for heartbeat timeout and polling interval
    let config = load_config()?;

    // Define queue
    let queue = ThreadSafeQueue::new(QueueConfig::default());

    // Define heartbeat tracking
    let heartbeat_times = Arc::new(Mutex::new(HashMap::new()));
    let heartbeat_timeout = time::Duration::from_secs(config.heartbeat_timeout_s);

    // Define vector of sources to process
    let mut child_handles = vec![];

    for src in &sources {
        // Spawn child processes
        let exe = std::env::current_exe()?;
        let mut cmd = Command::new(exe);

        cmd.arg("scrape-child")
            .arg("--source")
            .arg(src)
            .arg("--start")
            .arg(start_date.to_rfc3339())
            .arg("--end")
            .arg(end_date.to_rfc3339())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = cmd.spawn()?;
        let stdout = child.stdout.take().ok_or("Failed to capture stdout")?;
        let stderr = child.stderr.take().ok_or("Failed to capture stderr")?;

        // Init heartbeat time
        {
            let mut hb_times = heartbeat_times.lock().await;
            hb_times.insert(src.clone(), Instant::now());
        }

        // Create producer task for each child process
        let producer = queue.create_producer();
        let src_clone = src.clone();

        let handle = tokio::spawn(async move {
            let mut reader = BufReader::new(stdout).lines();

            while let Ok(Some(line)) = reader.next_line().await {
                if let Ok(record) = serde_json::from_str::<PublicationRecord>(&line) {
                    let _ = producer.submit(record);
                } else {
                    eprintln!("Failed to parse line from {}: {}", src_clone, line);
                }
            }
        });

        // Create hearbeat reader task
        let hb_times_clone = heartbeat_times.clone();
        let src_clone2 = src.clone();
        let heartbeat_handle = tokio::spawn(async move {
            let mut reader = BufReader::new(stderr).lines();

            while let Ok(Some(line)) = reader.next_line().await {
                if line.contains("HEARTBEAT") {
                    let mut hb_times = hb_times_clone.lock().await;
                    hb_times.insert(src_clone2.clone(), Instant::now());
                }
            }
        });

        child_handles.push((child, handle, heartbeat_handle, src.clone(), end_date));
    }

    // Consumer loop
    loop {
        if let Some(record) = queue.dequeue() {
            // Ingest publication into db
            ingest_publication(record, datastore).await?;
        } else {
            if queue.producers_finished() {
                break;
            }

            // Check for heartbeat timeouts
            let hb_times = heartbeat_times.lock().await;
            let now = Instant::now();
            for (src, last_heartbeat) in hb_times.iter() {
                if now.duration_since(*last_heartbeat) > heartbeat_timeout {
                    eprintln!(
                        "WARNING: Source {} has not sent a heartbeat in {:?}",
                        src, heartbeat_timeout
                    );
                }
            }
            drop(hb_times);

            tokio::time::sleep(tokio::time::Duration::from_millis(
                config.polling_interval_ms,
            ))
            .await;
        }
    }

    // Cleanup
    for (mut child, handle, heartbeat_handle, _src, _end_date) in child_handles {
        let _ = handle.await;
        let _ = heartbeat_handle.await;
        let _ = child.wait().await?;
    }

    Ok(())
}

/// Ingests a single publication record into the database.
async fn ingest_publication(
    record: PublicationRecord,
    datastore: &mut Database<impl Datastore>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Check if record already exists in the database
    if publication_exists(&record, datastore) {
        return Ok(());
    }

    // Add publication vertex to database
    let pub_vertex = add_publication(&record, datastore)?;

    // Add new author vertices and create AUTHORED edges
    for author in &record.authors {
        let author_vertex = get_or_create_author_vertex(author, datastore)?;
        create_authored_edge(&author_vertex, &pub_vertex, datastore)?;
    }

    // Create COAUTHORED_WITH edges between authors
    for i in 0..record.authors.len() {
        for j in (i + 1)..record.authors.len() {
            // Retrieve author vertices again since we need their IDs
            // Optimally we'd cache these but for now we look them up
            let author1 = get_or_create_author_vertex(&record.authors[i], datastore)?;
            let author2 = get_or_create_author_vertex(&record.authors[j], datastore)?;

            // Create COAUTHORED_WITH edge between record.authors[i] and record.authors[j]
            create_coauthor_edge(&author1, &author2, datastore)?;
            // Also create reverse edge for undirected graph simulation
            create_coauthor_edge(&author2, &author1, datastore)?;
        }
    }

    Ok(())
}

/// Checks if a publication record already exists in the database.
fn publication_exists(record: &PublicationRecord, _datastore: &Database<impl Datastore>) -> bool {
    // Check DOI or ArXiV ID existence

    // Check tokenized cosine similarity for title and jaccard similarity for authors

    return false;
}

/// Adds a publication record to the database.
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
pub(crate) fn get_or_create_author_vertex(
    author_name: &str,
    datastore: &mut Database<impl Datastore>,
) -> Result<Vertex, Box<dyn std::error::Error>> {
    use crate::db::schema::PERSON_TYPE;

    let person_type = Identifier::new(PERSON_TYPE)?;

    // Check if author exists by looking up name property
    let name_prop = Identifier::new("name")?;
    let name_val = Json::new(json!(author_name));

    let q = RangeVertexQuery::new()
        .t(person_type)
        .with_property_equal_to(name_prop, name_val.clone())?;

    let results = datastore.get(q)?;

    if let Some(QueryOutputValue::Vertices(vertices)) = results.first() {
        if let Some(vertex) = vertices.first() {
            return Ok(vertex.clone());
        }
    }

    // If not found, create new vertex
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

    Ok(vertex)
}

/// Creates an AUTHORED edge between an author and a publication.
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

/// Creates a COAUTHORED_WITH edge between two authors.
pub(crate) fn create_coauthor_edge(
    author1: &Vertex,
    author2: &Vertex,
    datastore: &mut Database<impl Datastore>,
) -> Result<(), Box<dyn std::error::Error>> {
    use crate::db::schema::COAUTHORED_WITH_TYPE;

    let edge_type = Identifier::new(COAUTHORED_WITH_TYPE)?;
    let edge = Edge::new(author1.id, edge_type, author2.id);

    // Check if edge exists
    let q = SpecificEdgeQuery::single(edge.clone());
    let results = datastore.get(q)?;

    if let Some(QueryOutputValue::Edges(edges)) = results.first() {
        if !edges.is_empty() {
            // Edge exists, increment weight
            let weight_prop = Identifier::new("weight")?;

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

            return Ok(());
        }
    }

    // Create new edge
    datastore.create_edge(&edge)?;

    // Initialize properties
    let weight_prop = Identifier::new("weight")?;
    let q = SpecificEdgeQuery::single(edge);
    datastore.set_properties(q, weight_prop, &Json::new(json!(1u64)))?;

    Ok(())
}
