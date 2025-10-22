use indradb::{Database, RocksdbDatastore};
use std::path::Path;

/// Initialize RocksDB-backed IndraDB database
pub fn init_datastore(path: &Path) -> Result<Database<RocksdbDatastore>, indradb::Error> {
    RocksdbDatastore::new_db(path)
}
