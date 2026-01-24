use helix_db::helix_engine::storage_core::version_info::VersionInfo;
use helix_db::helix_engine::traversal_core::config::GraphConfig;
use helix_db::helix_engine::traversal_core::{HelixGraphEngine, HelixGraphEngineOpts};
use helix_db::helix_engine::types::SecondaryIndex;
use std::path::Path;
use std::sync::Arc;

/// Initialize HelixDB database
#[coverage(off)]
#[allow(clippy::field_reassign_with_default)]
pub fn init_datastore(path: &Path) -> Result<Arc<HelixGraphEngine>, Box<dyn std::error::Error>> {
    // Configure secondary indices
    let indices = vec![
        SecondaryIndex::Index("year".to_string()),
        SecondaryIndex::Index("title".to_string()),
        SecondaryIndex::Index("publication_id".to_string()),
        SecondaryIndex::Index("name".to_string()),
    ];

    let mut graph_config = GraphConfig::default();
    graph_config.secondary_indices = Some(indices);

    let mut opts = HelixGraphEngineOpts::default();
    opts.path = path.to_string_lossy().to_string();
    opts.config.graph_config = Some(graph_config);
    opts.version_info = VersionInfo::default();

    // Ensure the directory exists
    if !path.exists() {
        std::fs::create_dir_all(path)?;
    }

    let engine = HelixGraphEngine::new(opts)?;
    Ok(Arc::new(engine))
}
