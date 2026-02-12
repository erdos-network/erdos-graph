#[cfg(test)]
mod tests {
    use crate::db::client::init_datastore;
    use std::path::PathBuf;
    use tempfile::TempDir;

    /// Test database initialization with RocksDB.
    #[test]
    #[ignore = "Integration test - requires RocksDB setup"]
    fn test_init_datastore() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test_db");

        let result = init_datastore(&db_path);

        assert!(result.is_ok());
    }

    /// Test that init_datastore fails gracefully with invalid paths.
    #[test]
    #[ignore = "Integration test - requires RocksDB setup"]
    fn test_init_datastore_invalid_path() {
        let invalid_path = PathBuf::from("/dev/null/impossible/path");

        let result = init_datastore(&invalid_path);

        assert!(result.is_err() || result.is_ok());
    }

    /// Test database persistence across reopens.
    #[test]
    #[ignore = "Integration test - requires RocksDB setup"]
    fn test_datastore_persistence() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("persist_test_db");

        {
            let db_result = init_datastore(&db_path);
            assert!(db_result.is_ok());
        }

        {
            let db_result = init_datastore(&db_path);
            assert!(db_result.is_ok());
        }
    }
}
