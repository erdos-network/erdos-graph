use std::collections::HashMap;

// Vertex types
pub const PERSON_TYPE: &str = "Person";
pub const PUBLICATION_TYPE: &str = "Publication";

// Edge types
pub const AUTHORED_TYPE: &str = "AUTHORED";
pub const COAUTHORED_WITH_TYPE: &str = "COAUTHORED_WITH";

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

#[allow(unused)]
pub fn publication_properties() -> HashMap<String, String> {
    let mut props = HashMap::new();
    props.insert("title".to_string(), "".to_string());
    props.insert("year".to_string(), "0".to_string());
    props.insert("venue".to_string(), "".to_string());
    props.insert("publication_id".to_string(), "".to_string());
    props
}

#[allow(unused)]
pub fn coauthored_with_properties() -> HashMap<String, String> {
    let mut props = HashMap::new();
    props.insert("weight".to_string(), "1".to_string());
    props.insert("publication_ids".to_string(), "[]".to_string());
    props
}

pub fn create_types<T>(_datastore: &mut T) -> Result<(), Box<dyn std::error::Error>> {
    Ok(())
}
