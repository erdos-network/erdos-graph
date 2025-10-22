#[cfg(test)]
mod tests {
    use crate::db::schema;

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
}