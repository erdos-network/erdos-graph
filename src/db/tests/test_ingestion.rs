#[cfg(test)]
mod tests {
    use crate::db::ingestion::{PublicationRecord, get_checkpoint, set_checkpoint};
    use chrono::Utc;
    use tempfile::TempDir;

    /// Test basic publication record creation.
    #[test]
    fn test_publication_record_creation() {
        let record = PublicationRecord {
            id: "test:123".to_string(),
            title: "Test Publication".to_string(),
            authors: vec!["Alice".to_string(), "Bob".to_string()],
            year: 2024,
            venue: Some("Test Conference".to_string()),
            source: "arxiv".to_string(),
        };

        assert_eq!(record.id, "test:123");
        assert_eq!(record.authors.len(), 2);
    }

    /// Test checkpoint saving and loading.
    #[test]
    fn test_checkpoint_save_and_load() {
        let source = "test_source";
        let test_date = Utc::now();
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path();

        // Save checkpoint
        let save_result = set_checkpoint(source, test_date, base_path);
        assert!(save_result.is_ok());

        // Load checkpoint
        let load_result = get_checkpoint(source, base_path);
        assert!(load_result.is_ok());

        let loaded_date = load_result.unwrap();
        assert!(loaded_date.is_some());

        // Dates should match (within a second due to serialization)
        let diff = (loaded_date.unwrap() - test_date).num_seconds().abs();
        assert!(diff < 2);
    }

    /// Test checkpoint for non-existent source.
    #[test]
    fn test_checkpoint_nonexistent() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path();
        let result = get_checkpoint("nonexistent_source_xyz", base_path);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    /// Test publication record serialization/deserialization.
    #[test]
    fn test_publication_record_serialization() {
        let record = PublicationRecord {
            id: "test:serialize".to_string(),
            title: "Serialization Test".to_string(),
            authors: vec!["Author1".to_string(), "Author2".to_string()],
            year: 2023,
            venue: Some("Test Venue".to_string()),
            source: "dblp".to_string(),
        };

        // Test JSON serialization
        let json = serde_json::to_string(&record).unwrap();
        let deserialized: PublicationRecord = serde_json::from_str(&json).unwrap();

        assert_eq!(record, deserialized);
    }
}
