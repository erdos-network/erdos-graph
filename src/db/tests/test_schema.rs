#[cfg(test)]
mod tests {
    use crate::db::schema;
    use crate::db::schema::{
        AUTHORED_TYPE, COAUTHORED_WITH_TYPE, GraphEdge, GraphVertex, PERSON_TYPE, PUBLICATION_TYPE,
    };
    use serde_json::json;

    #[test]
    fn test_person_properties() {
        let props = schema::person_properties();
        assert!(props.contains_key("name"));
        assert!(props.contains_key("erdos_number"));
    }

    #[test]
    fn test_publication_properties() {
        let props = schema::publication_properties();
        assert!(props.contains_key("title"));
        assert!(props.contains_key("year"));
    }

    #[test]
    fn test_coauthored_with_properties() {
        let props = schema::coauthored_with_properties();
        assert!(props.contains_key("weight"));
    }

    #[test]
    fn test_create_types_noop() {
        // create_types is a no-op for now; pass a dummy datastore value
        let mut dummy = ();
        assert!(schema::create_types(&mut dummy).is_ok());
    }

    #[test]
    fn test_graph_vertex_new() {
        let vertex = GraphVertex::new(PERSON_TYPE);
        assert_eq!(vertex.t, PERSON_TYPE);
        assert!(vertex.props.is_empty());
    }

    #[test]
    fn test_graph_vertex_with_id() {
        let id = uuid::Uuid::new_v4();
        let vertex = GraphVertex::with_id(id, PUBLICATION_TYPE);
        assert_eq!(vertex.id, id);
        assert_eq!(vertex.t, PUBLICATION_TYPE);
    }

    #[test]
    fn test_graph_vertex_property() {
        let vertex = GraphVertex::new(PERSON_TYPE)
            .property("name", "John Doe".to_string())
            .property("erdos_number", 2);

        assert_eq!(vertex.props.get("name").unwrap(), &json!("John Doe"));
        assert_eq!(vertex.props.get("erdos_number").unwrap(), &json!(2));
    }

    #[test]
    fn test_graph_vertex_chaining() {
        let vertex = GraphVertex::new(PUBLICATION_TYPE)
            .property("title", "Test Paper".to_string())
            .property("year", 2024)
            .property("venue", "Test Conference".to_string());

        assert_eq!(vertex.props.len(), 3);
    }

    #[test]
    fn test_graph_edge_new() {
        let source_id = uuid::Uuid::new_v4();
        let target_id = uuid::Uuid::new_v4();
        let edge = GraphEdge::new(source_id, target_id, AUTHORED_TYPE);

        assert_eq!(edge.source_id, source_id);
        assert_eq!(edge.target_id, target_id);
        assert_eq!(edge.t, AUTHORED_TYPE);
        assert!(edge.props.is_empty());
    }

    #[test]
    fn test_graph_edge_property() {
        let source_id = uuid::Uuid::new_v4();
        let target_id = uuid::Uuid::new_v4();
        let edge = GraphEdge::new(source_id, target_id, COAUTHORED_WITH_TYPE).property("weight", 5);

        assert_eq!(edge.props.get("weight").unwrap(), &json!(5));
    }

    #[test]
    fn test_graph_edge_multiple_properties() {
        let source_id = uuid::Uuid::new_v4();
        let target_id = uuid::Uuid::new_v4();
        let edge = GraphEdge::new(source_id, target_id, COAUTHORED_WITH_TYPE)
            .property("weight", 10)
            .property("first_collab_year", 2020);

        assert_eq!(edge.props.len(), 2);
    }

    #[test]
    fn test_vertex_clone() {
        let vertex = GraphVertex::new(PERSON_TYPE).property("name", "Alice".to_string());

        let cloned = vertex.clone();
        assert_eq!(vertex.id, cloned.id);
        assert_eq!(vertex.t, cloned.t);
        assert_eq!(vertex.props.len(), cloned.props.len());
    }

    #[test]
    fn test_edge_clone() {
        let source_id = uuid::Uuid::new_v4();
        let target_id = uuid::Uuid::new_v4();
        let edge = GraphEdge::new(source_id, target_id, AUTHORED_TYPE).property("weight", 1);

        let cloned = edge.clone();
        assert_eq!(edge.source_id, cloned.source_id);
        assert_eq!(edge.target_id, cloned.target_id);
        assert_eq!(edge.t, cloned.t);
    }

    #[test]
    fn test_vertex_debug() {
        let vertex = GraphVertex::new(PERSON_TYPE);
        let debug_str = format!("{:?}", vertex);
        assert!(debug_str.contains("GraphVertex"));
    }

    #[test]
    fn test_edge_debug() {
        let source_id = uuid::Uuid::new_v4();
        let target_id = uuid::Uuid::new_v4();
        let edge = GraphEdge::new(source_id, target_id, AUTHORED_TYPE);
        let debug_str = format!("{:?}", edge);
        assert!(debug_str.contains("GraphEdge"));
    }

    #[test]
    fn test_type_constants() {
        assert_eq!(PERSON_TYPE, "Person");
        assert_eq!(PUBLICATION_TYPE, "Publication");
        assert_eq!(AUTHORED_TYPE, "AUTHORED");
        assert_eq!(COAUTHORED_WITH_TYPE, "COAUTHORED_WITH");
    }

    #[test]
    fn test_person_properties_structure() {
        let props = schema::person_properties();
        assert!(props.contains_key("name"));
        assert!(props.contains_key("erdos_number"));
    }

    #[test]
    fn test_publication_properties_structure() {
        let props = schema::publication_properties();
        assert!(props.contains_key("title"));
        assert!(props.contains_key("year"));
        assert!(props.contains_key("venue"));
        assert!(props.contains_key("publication_id"));
    }

    #[test]
    fn test_coauthored_properties_structure() {
        let props = schema::coauthored_with_properties();
        assert!(props.contains_key("weight"));
    }

    #[test]
    fn test_vertex_property_overwrite() {
        let vertex = GraphVertex::new(PERSON_TYPE)
            .property("name", "Original Name".to_string())
            .property("name", "New Name".to_string());

        assert_eq!(vertex.props.get("name").unwrap(), &json!("New Name"));
    }

    #[test]
    fn test_edge_property_overwrite() {
        let source_id = uuid::Uuid::new_v4();
        let target_id = uuid::Uuid::new_v4();
        let edge = GraphEdge::new(source_id, target_id, COAUTHORED_WITH_TYPE)
            .property("weight", 1)
            .property("weight", 5);

        assert_eq!(edge.props.get("weight").unwrap(), &json!(5));
    }

    #[test]
    fn test_vertex_with_various_property_types() {
        let vertex = GraphVertex::new(PERSON_TYPE)
            .property("string_prop", "test".to_string())
            .property("int_prop", 42)
            .property("bool_prop", true)
            .property("float_prop", 3.04);

        assert_eq!(vertex.props.len(), 4);
    }

    #[test]
    fn test_edge_with_various_property_types() {
        let source_id = uuid::Uuid::new_v4();
        let target_id = uuid::Uuid::new_v4();
        let edge = GraphEdge::new(source_id, target_id, COAUTHORED_WITH_TYPE)
            .property("weight", 10)
            .property("confirmed", true)
            .property("score", 0.95);

        assert_eq!(edge.props.len(), 3);
    }

    #[test]
    fn test_vertex_empty_properties() {
        let vertex = GraphVertex::new(PERSON_TYPE);
        assert_eq!(vertex.props.len(), 0);
        assert!(vertex.props.is_empty());
    }

    #[test]
    fn test_edge_empty_properties() {
        let source_id = uuid::Uuid::new_v4();
        let target_id = uuid::Uuid::new_v4();
        let edge = GraphEdge::new(source_id, target_id, AUTHORED_TYPE);
        assert_eq!(edge.props.len(), 0);
        assert!(edge.props.is_empty());
    }

    #[test]
    fn test_unique_vertex_ids() {
        let v1 = GraphVertex::new(PERSON_TYPE);
        let v2 = GraphVertex::new(PERSON_TYPE);
        assert_ne!(v1.id, v2.id);
    }

    #[test]
    fn test_vertex_with_id_preserves_id() {
        let expected_id = uuid::Uuid::new_v4();
        let vertex = GraphVertex::with_id(expected_id, PERSON_TYPE);
        assert_eq!(vertex.id, expected_id);
    }
}
