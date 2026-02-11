#[cfg(test)]
mod tests {
    use crate::config::Config;
    use crate::db::ingestion::PublicationRecord;
    use crate::db::schema::{AUTHORED_TYPE, GraphEdge, GraphVertex, PERSON_TYPE, PUBLICATION_TYPE};
    use crate::scrapers::ingestion_utils::{
        IngestionContext, WriteOperation, add_publication, create_authored_edge,
        get_or_create_author_vertex,
    };
    use std::collections::HashMap;

    fn create_test_record(id: &str, title: &str, year: u32) -> PublicationRecord {
        PublicationRecord {
            id: id.to_string(),
            title: title.to_string(),
            authors: vec!["Alice Smith".to_string(), "Bob Jones".to_string()],
            year,
            venue: Some("Test Conference".to_string()),
            source: "test".to_string(),
        }
    }

    #[test]
    fn test_ingestion_context_new() {
        let config = Config::default();
        let context = IngestionContext::new(&config);

        assert_eq!(context.write_buffer.len(), 0);
        assert_eq!(context.author_cache.len(), 0);
        assert_eq!(context.pending_edge_updates.len(), 0);
        assert!(!context.edge_cache.edges_loaded);
    }

    #[test]
    fn test_write_operation_create_vertex() {
        let vertex = GraphVertex::new(PERSON_TYPE).property("name", "Test Author".to_string());
        let op = WriteOperation::CreateVertex(vertex.clone());

        match op {
            WriteOperation::CreateVertex(v) => {
                assert_eq!(v.t, PERSON_TYPE);
                assert_eq!(v.props.get("name").unwrap(), "Test Author");
            }
            _ => panic!("Expected CreateVertex"),
        }
    }

    #[test]
    fn test_write_operation_create_edge() {
        let source_id = uuid::Uuid::new_v4();
        let target_id = uuid::Uuid::new_v4();
        let edge = GraphEdge::new(source_id, target_id, AUTHORED_TYPE);
        let op = WriteOperation::CreateEdge(edge.clone());

        match op {
            WriteOperation::CreateEdge(e) => {
                assert_eq!(e.source_id, source_id);
                assert_eq!(e.target_id, target_id);
                assert_eq!(e.t, AUTHORED_TYPE);
            }
            _ => panic!("Expected CreateEdge"),
        }
    }

    #[test]
    fn test_write_operation_clone() {
        let vertex = GraphVertex::new(PERSON_TYPE).property("name", "Clone Test".to_string());
        let op1 = WriteOperation::CreateVertex(vertex.clone());
        let op2 = op1.clone();

        match (op1, op2) {
            (WriteOperation::CreateVertex(v1), WriteOperation::CreateVertex(v2)) => {
                assert_eq!(v1.id, v2.id);
                assert_eq!(v1.t, v2.t);
            }
            _ => panic!("Expected both to be CreateVertex"),
        }
    }

    #[test]
    fn test_get_or_create_author_vertex_new_author() {
        let mut write_buffer = Vec::new();
        let mut author_cache = HashMap::new();

        let result = get_or_create_author_vertex("John Doe", &mut write_buffer, &mut author_cache);

        assert!(result.is_ok());
        let vertex = result.unwrap();
        assert_eq!(vertex.t, PERSON_TYPE);
        assert_eq!(vertex.props.get("name").unwrap(), "John Doe");

        // Check that it was added to the cache
        assert_eq!(author_cache.len(), 1);

        // Check that it was added to the write buffer
        assert_eq!(write_buffer.len(), 1);
    }

    #[test]
    fn test_get_or_create_author_vertex_cached_author() {
        let mut write_buffer = Vec::new();
        let mut author_cache = HashMap::new();

        // First creation
        let first_result =
            get_or_create_author_vertex("Jane Smith", &mut write_buffer, &mut author_cache);
        assert!(first_result.is_ok());
        let first_vertex = first_result.unwrap();

        // Clear write buffer to check it doesn't add again
        write_buffer.clear();

        // Second creation (should use cache)
        let second_result =
            get_or_create_author_vertex("Jane Smith", &mut write_buffer, &mut author_cache);
        assert!(second_result.is_ok());
        let second_vertex = second_result.unwrap();

        // Should return the same vertex
        assert_eq!(first_vertex.id, second_vertex.id);

        // Should not add to write buffer again
        assert_eq!(write_buffer.len(), 0);

        // Cache should still have only one entry
        assert_eq!(author_cache.len(), 1);
    }

    #[test]
    fn test_get_or_create_author_vertex_normalization() {
        let mut write_buffer = Vec::new();
        let mut author_cache = HashMap::new();

        // Add with different casing and spacing
        get_or_create_author_vertex("John  Smith", &mut write_buffer, &mut author_cache).unwrap();
        write_buffer.clear();

        // Try to add similar name with different formatting
        let result =
            get_or_create_author_vertex("JOHN   SMITH", &mut write_buffer, &mut author_cache);
        assert!(result.is_ok());

        // Should use cached version (normalized)
        assert_eq!(write_buffer.len(), 0);
        assert_eq!(author_cache.len(), 1);
    }

    #[test]
    fn test_get_or_create_author_vertex_empty_name() {
        let mut write_buffer = Vec::new();
        let mut author_cache = HashMap::new();

        let result = get_or_create_author_vertex("", &mut write_buffer, &mut author_cache);
        assert!(result.is_ok());

        // Should still create a vertex
        assert_eq!(write_buffer.len(), 1);
        assert_eq!(author_cache.len(), 1);
    }

    #[test]
    fn test_get_or_create_author_vertex_special_characters() {
        let mut write_buffer = Vec::new();
        let mut author_cache = HashMap::new();

        let result =
            get_or_create_author_vertex("José García-Pérez", &mut write_buffer, &mut author_cache);
        assert!(result.is_ok());

        let vertex = result.unwrap();
        assert_eq!(vertex.props.get("name").unwrap(), "José García-Pérez");
    }

    #[test]
    fn test_add_publication_basic() {
        let record = create_test_record("pub-123", "Test Paper", 2024);
        let mut write_buffer = Vec::new();

        let result = add_publication(&record, &mut write_buffer);

        assert!(result.is_ok());
        let vertex = result.unwrap();

        assert_eq!(vertex.t, PUBLICATION_TYPE);
        assert_eq!(vertex.props.get("title").unwrap(), "Test Paper");
        assert_eq!(vertex.props.get("year").unwrap(), "2024");
        assert_eq!(vertex.props.get("publication_id").unwrap(), "pub-123");
        assert_eq!(vertex.props.get("source").unwrap(), "test");
        assert_eq!(vertex.props.get("venue").unwrap(), "Test Conference");

        // Check write buffer
        assert_eq!(write_buffer.len(), 1);
    }

    #[test]
    fn test_add_publication_without_venue() {
        let mut record = create_test_record("pub-456", "Another Paper", 2023);
        record.venue = None;
        let mut write_buffer = Vec::new();

        let result = add_publication(&record, &mut write_buffer);

        assert!(result.is_ok());
        let vertex = result.unwrap();

        assert_eq!(vertex.t, PUBLICATION_TYPE);
        assert!(!vertex.props.contains_key("venue"));
        assert_eq!(vertex.props.get("title").unwrap(), "Another Paper");
    }

    #[test]
    fn test_add_publication_year_conversion() {
        let record = create_test_record("pub-789", "Year Test", 1999);
        let mut write_buffer = Vec::new();

        let result = add_publication(&record, &mut write_buffer);

        assert!(result.is_ok());
        let vertex = result.unwrap();
        assert_eq!(vertex.props.get("year").unwrap(), "1999");
    }

    #[test]
    fn test_add_publication_creates_unique_ids() {
        let record1 = create_test_record("pub-001", "Paper 1", 2024);
        let record2 = create_test_record("pub-002", "Paper 2", 2024);
        let mut write_buffer = Vec::new();

        let vertex1 = add_publication(&record1, &mut write_buffer).unwrap();
        let vertex2 = add_publication(&record2, &mut write_buffer).unwrap();

        // Each publication should get a unique ID
        assert_ne!(vertex1.id, vertex2.id);
    }

    #[test]
    fn test_create_authored_edge_basic() {
        let author = GraphVertex::new(PERSON_TYPE).property("name", "Test Author".to_string());
        let publication =
            GraphVertex::new(PUBLICATION_TYPE).property("title", "Test Paper".to_string());
        let mut write_buffer = Vec::new();

        let result = create_authored_edge(&author, &publication, &mut write_buffer);

        assert!(result.is_ok());
        assert_eq!(write_buffer.len(), 1);

        match &write_buffer[0] {
            WriteOperation::CreateEdge(edge) => {
                assert_eq!(edge.source_id, author.id);
                assert_eq!(edge.target_id, publication.id);
                assert_eq!(edge.t, AUTHORED_TYPE);
            }
            _ => panic!("Expected CreateEdge in write buffer"),
        }
    }

    #[test]
    fn test_create_authored_edge_multiple() {
        let author1 = GraphVertex::new(PERSON_TYPE).property("name", "Author 1".to_string());
        let author2 = GraphVertex::new(PERSON_TYPE).property("name", "Author 2".to_string());
        let publication =
            GraphVertex::new(PUBLICATION_TYPE).property("title", "Collaborative Paper".to_string());
        let mut write_buffer = Vec::new();

        create_authored_edge(&author1, &publication, &mut write_buffer).unwrap();
        create_authored_edge(&author2, &publication, &mut write_buffer).unwrap();

        assert_eq!(write_buffer.len(), 2);

        // Verify both edges point to the same publication
        match (&write_buffer[0], &write_buffer[1]) {
            (WriteOperation::CreateEdge(edge1), WriteOperation::CreateEdge(edge2)) => {
                assert_eq!(edge1.target_id, publication.id);
                assert_eq!(edge2.target_id, publication.id);
                assert_ne!(edge1.source_id, edge2.source_id);
            }
            _ => panic!("Expected CreateEdge operations"),
        }
    }

    #[test]
    fn test_ingestion_context_write_buffer_capacity() {
        let config = Config::default();
        let context = IngestionContext::new(&config);

        // Verify the buffer has the configured capacity
        assert!(context.write_buffer.capacity() >= config.ingestion.write_buffer_capacity);
    }

    #[test]
    fn test_multiple_authors_workflow() {
        let mut write_buffer = Vec::new();
        let mut author_cache = HashMap::new();

        let authors = vec!["Alice Smith", "Bob Jones", "Carol White"];

        let mut author_vertices = Vec::new();
        for author in &authors {
            let vertex =
                get_or_create_author_vertex(author, &mut write_buffer, &mut author_cache).unwrap();
            author_vertices.push(vertex);
        }

        assert_eq!(author_cache.len(), 3);
        assert_eq!(write_buffer.len(), 3);

        // Create publication
        let record = create_test_record("pub-multi", "Multi Author Paper", 2024);
        let pub_vertex = add_publication(&record, &mut write_buffer).unwrap();

        // Create edges
        for author in &author_vertices {
            create_authored_edge(author, &pub_vertex, &mut write_buffer).unwrap();
        }

        // Total operations: 3 authors + 1 publication + 3 edges = 7
        assert_eq!(write_buffer.len(), 7);
    }

    #[test]
    fn test_author_cache_across_publications() {
        let mut write_buffer = Vec::new();
        let mut author_cache = HashMap::new();

        // Create first publication with author
        let record1 = create_test_record("pub-1", "Paper 1", 2024);
        let author1 =
            get_or_create_author_vertex("Shared Author", &mut write_buffer, &mut author_cache)
                .unwrap();
        let pub1 = add_publication(&record1, &mut write_buffer).unwrap();
        create_authored_edge(&author1, &pub1, &mut write_buffer).unwrap();

        let ops_after_first = write_buffer.len();

        // Create second publication with same author
        let record2 = create_test_record("pub-2", "Paper 2", 2024);
        let author2 =
            get_or_create_author_vertex("Shared Author", &mut write_buffer, &mut author_cache)
                .unwrap();
        let pub2 = add_publication(&record2, &mut write_buffer).unwrap();
        create_authored_edge(&author2, &pub2, &mut write_buffer).unwrap();

        // Should reuse cached author
        assert_eq!(author1.id, author2.id);
        assert_eq!(author_cache.len(), 1);

        // Second publication should only add: 1 publication + 1 edge (not a new author)
        assert_eq!(write_buffer.len(), ops_after_first + 2);
    }

    #[test]
    fn test_publication_with_empty_authors_list() {
        let mut record = create_test_record("pub-no-authors", "Solo Paper", 2024);
        record.authors = vec![];
        let mut write_buffer = Vec::new();

        let result = add_publication(&record, &mut write_buffer);
        assert!(result.is_ok());

        // Should still create the publication
        assert_eq!(write_buffer.len(), 1);
    }

    #[test]
    fn test_write_operations_debug() {
        let vertex = GraphVertex::new(PERSON_TYPE);
        let op = WriteOperation::CreateVertex(vertex);

        // Should be able to debug print
        let debug_str = format!("{:?}", op);
        assert!(debug_str.contains("CreateVertex"));
    }

    #[test]
    fn test_get_or_create_author_vertex_long_name() {
        let mut write_buffer = Vec::new();
        let mut author_cache = HashMap::new();

        let long_name = "Dr. Professor Johann Wolfgang von Goethe III, PhD, MD, Esq.";
        let result = get_or_create_author_vertex(long_name, &mut write_buffer, &mut author_cache);

        assert!(result.is_ok());
        let vertex = result.unwrap();
        assert_eq!(vertex.props.get("name").unwrap(), long_name);
    }

    #[test]
    fn test_add_publication_with_special_characters() {
        let mut record = create_test_record("pub-special", "Test Paper", 2024);
        record.title = "Título con caractères spéciaux & símbolos!".to_string();
        let mut write_buffer = Vec::new();

        let result = add_publication(&record, &mut write_buffer);
        assert!(result.is_ok());

        let vertex = result.unwrap();
        assert_eq!(
            vertex.props.get("title").unwrap(),
            "Título con caractères spéciaux & símbolos!"
        );
    }

    #[test]
    fn test_edge_creation_preserves_vertex_ids() {
        let author_id = uuid::Uuid::new_v4();
        let pub_id = uuid::Uuid::new_v4();

        let author = GraphVertex::with_id(author_id, PERSON_TYPE);
        let publication = GraphVertex::with_id(pub_id, PUBLICATION_TYPE);
        let mut write_buffer = Vec::new();

        create_authored_edge(&author, &publication, &mut write_buffer).unwrap();

        match &write_buffer[0] {
            WriteOperation::CreateEdge(edge) => {
                assert_eq!(edge.source_id, author_id);
                assert_eq!(edge.target_id, pub_id);
            }
            _ => panic!("Expected CreateEdge"),
        }
    }

    #[test]
    fn test_ingestion_context_pending_edge_updates_empty() {
        let config = Config::default();
        let context = IngestionContext::new(&config);

        assert_eq!(context.pending_edge_updates.len(), 0);
    }

    #[test]
    fn test_publication_year_zero() {
        let record = create_test_record("pub-year-zero", "Ancient Paper", 0);
        let mut write_buffer = Vec::new();

        let result = add_publication(&record, &mut write_buffer);
        assert!(result.is_ok());

        let vertex = result.unwrap();
        assert_eq!(vertex.props.get("year").unwrap(), "0");
    }

    #[test]
    fn test_publication_future_year() {
        let record = create_test_record("pub-future", "Future Paper", 9999);
        let mut write_buffer = Vec::new();

        let result = add_publication(&record, &mut write_buffer);
        assert!(result.is_ok());

        let vertex = result.unwrap();
        assert_eq!(vertex.props.get("year").unwrap(), "9999");
    }

    #[test]
    fn test_author_vertex_has_correct_type() {
        let mut write_buffer = Vec::new();
        let mut author_cache = HashMap::new();

        let vertex =
            get_or_create_author_vertex("Type Test", &mut write_buffer, &mut author_cache).unwrap();

        assert_eq!(vertex.t, PERSON_TYPE);
    }

    #[test]
    fn test_publication_vertex_has_correct_type() {
        let record = create_test_record("pub-type-test", "Type Test Paper", 2024);
        let mut write_buffer = Vec::new();

        let vertex = add_publication(&record, &mut write_buffer).unwrap();

        assert_eq!(vertex.t, PUBLICATION_TYPE);
    }

    #[test]
    fn test_authored_edge_has_correct_type() {
        let author = GraphVertex::new(PERSON_TYPE);
        let publication = GraphVertex::new(PUBLICATION_TYPE);
        let mut write_buffer = Vec::new();

        create_authored_edge(&author, &publication, &mut write_buffer).unwrap();

        match &write_buffer[0] {
            WriteOperation::CreateEdge(edge) => {
                assert_eq!(edge.t, AUTHORED_TYPE);
            }
            _ => panic!("Expected CreateEdge"),
        }
    }

    #[test]
    fn test_publication_preserves_all_fields() {
        let record = PublicationRecord {
            id: "full-test-123".to_string(),
            title: "Full Fields Test".to_string(),
            authors: vec!["Author One".to_string()],
            year: 2024,
            venue: Some("Test Venue".to_string()),
            source: "test_source".to_string(),
        };
        let mut write_buffer = Vec::new();

        let vertex = add_publication(&record, &mut write_buffer).unwrap();

        assert_eq!(vertex.props.get("title").unwrap(), "Full Fields Test");
        assert_eq!(vertex.props.get("year").unwrap(), "2024");
        assert_eq!(vertex.props.get("publication_id").unwrap(), "full-test-123");
        assert_eq!(vertex.props.get("source").unwrap(), "test_source");
        assert_eq!(vertex.props.get("venue").unwrap(), "Test Venue");
    }

    #[test]
    fn test_write_buffer_accumulation() {
        let mut write_buffer = Vec::new();
        let mut author_cache = HashMap::new();

        // Add multiple operations
        for i in 0..10 {
            get_or_create_author_vertex(
                &format!("Author {}", i),
                &mut write_buffer,
                &mut author_cache,
            )
            .unwrap();
        }

        assert_eq!(write_buffer.len(), 10);
        assert_eq!(author_cache.len(), 10);
    }

    #[test]
    fn test_author_normalization_whitespace_variations() {
        let mut write_buffer = Vec::new();
        let mut author_cache = HashMap::new();

        let variations = vec![
            "John Smith",
            "  John Smith  ",
            "John  Smith",
            "JOHN SMITH",
            "john smith",
        ];

        for variation in variations {
            get_or_create_author_vertex(variation, &mut write_buffer, &mut author_cache).unwrap();
        }

        // All variations should map to the same normalized author
        // So we should only have 1 author in cache and potentially multiple in buffer
        // depending on first insertion
        assert_eq!(author_cache.len(), 1);
    }
}
