use indradb::{Database, Identifier, RocksdbDatastore};
use std::path::Path;

/// Initialize RocksDB-backed IndraDB database with required indices
#[coverage(off)]
pub fn init_datastore(path: &Path) -> Result<Database<RocksdbDatastore>, indradb::Error> {
    let db = RocksdbDatastore::new_db(path)?;

    // Create indices for efficient property queries
    db.index_property(Identifier::new("name")?)?;
    db.index_property(Identifier::new("publication_id")?)?;
    db.index_property(Identifier::new("year")?)?;

    Ok(db)
}
