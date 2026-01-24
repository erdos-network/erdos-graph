//! Graph database schema definition.
//!
//! This module defines the vertex types, edge types, and property schemas
//! used in the Erdős Graph database. It also provides the internal
//! data structures (`GraphVertex`, `GraphEdge`) used to decouple the
//! application logic from the underlying database driver.
//!
//! # Graph Structure
//! - **Person vertices**: Represent authors with name, Erdős number, and aliases
//! - **Publication vertices**: Represent papers with title, year, venue
//! - **AUTHORED edges**: Connect Person vertices to their publications
//! - **COAUTHORED_WITH edges**: Connect co-authors with weights representing collaboration count

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use uuid::Uuid;

/// Vertex type identifier for Person nodes
pub const PERSON_TYPE: &str = "Person";

/// Vertex type identifier for Publication nodes
pub const PUBLICATION_TYPE: &str = "Publication";

/// Edge type for authorship relationships (Person -> Publication)
pub const AUTHORED_TYPE: &str = "AUTHORED";

/// Edge type for co-authorship relationships (Person -> Person)
pub const COAUTHORED_WITH_TYPE: &str = "COAUTHORED_WITH";

/// Represents a generic Vertex in the graph, independent of the underlying DB.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphVertex {
    pub id: Uuid,
    pub t: String,
    pub props: HashMap<String, Value>,
}

impl GraphVertex {
    pub fn new(t: &str) -> Self {
        Self {
            id: Uuid::new_v4(),
            t: t.to_string(),
            props: HashMap::new(),
        }
    }

    pub fn with_id(id: Uuid, t: &str) -> Self {
        Self {
            id,
            t: t.to_string(),
            props: HashMap::new(),
        }
    }

    pub fn property<V: Into<Value>>(mut self, key: &str, value: V) -> Self {
        self.props.insert(key.to_string(), value.into());
        self
    }
}

/// Represents a generic Edge in the graph, independent of the underlying DB.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphEdge {
    pub source_id: Uuid,
    pub target_id: Uuid,
    pub t: String,
    pub props: HashMap<String, Value>,
}

impl GraphEdge {
    pub fn new(source_id: Uuid, target_id: Uuid, t: &str) -> Self {
        Self {
            source_id,
            target_id,
            t: t.to_string(),
            props: HashMap::new(),
        }
    }

    pub fn property<V: Into<Value>>(mut self, key: &str, value: V) -> Self {
        self.props.insert(key.to_string(), value.into());
        self
    }
}

/// Returns the property schema for Person vertices.
///
/// # Properties
/// - `name`: The person's canonical name
/// - `erdos_number`: String representation of Erdős number ("None" if not computed)
/// - `is_erdos`: "true" for Paul Erdős himself, "false" otherwise
/// - `aliases`: JSON array of alternative name spellings/forms
/// - `updated_at`: Unix timestamp of last update
///
/// # Returns
/// A HashMap with default/placeholder values for all properties
#[allow(unused)]
pub fn person_properties() -> HashMap<String, String> {
    let mut props = HashMap::new();
    props.insert("name".to_string(), "".to_string());
    props.insert("erdos_number".to_string(), "None".to_string());
    props.insert("is_erdos".to_string(), "false".to_string());
    props.insert("aliases".to_string(), "[]".to_string());
    props.insert("updated_at".to_string(), "0".to_string());
    props
}

/// Returns the property schema for Publication vertices.
///
/// # Properties
/// - `title`: Full title of the publication
/// - `year`: Publication year as a string
/// - `venue`: Journal, conference, or other publication venue
/// - `publication_id`: Source-specific identifier (e.g., ArXiv ID, DBLP key)
///
/// # Returns
/// A HashMap with default/placeholder values for all properties
#[allow(unused)]
pub fn publication_properties() -> HashMap<String, String> {
    let mut props = HashMap::new();
    props.insert("title".to_string(), "".to_string());
    props.insert("year".to_string(), "0".to_string());
    props.insert("venue".to_string(), "".to_string());
    props.insert("publication_id".to_string(), "".to_string());
    props
}

/// Returns the property schema for COAUTHORED_WITH edges.
///
/// # Properties
/// - `weight`: Number of papers co-authored (starts at 1, increments with each collaboration)
/// - `publication_ids`: JSON array of publication IDs representing the collaborations
///
/// # Returns
/// A HashMap with default/placeholder values for all properties
#[allow(unused)]
pub fn coauthored_with_properties() -> HashMap<String, String> {
    let mut props = HashMap::new();
    props.insert("weight".to_string(), "1".to_string());
    props.insert("publication_ids".to_string(), "[]".to_string());
    props
}

/// Initializes type constraints in the datastore (if needed).
///
/// IndraDB doesn't require explicit type registration, but this function
/// is provided for future schema initialization tasks.
///
/// # Arguments
/// * `datastore` - Mutable reference to the graph database
///
/// # Returns
/// Always returns `Ok(())` for now
pub fn create_types<T>(_datastore: &mut T) -> Result<(), Box<dyn std::error::Error>> {
    // IndraDB doesn't require type registration
    // This function is a no-op but kept for future extensibility
    Ok(())
}
