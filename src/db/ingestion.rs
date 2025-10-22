use uuid::Uuid;
use indradb::Datastore;

pub fn find_or_create_person(_datastore: &mut impl Datastore, _name: &str) -> Result<Uuid, indradb::Error> {
    // Stub: will implement real lookup/creation later
    Ok(Uuid::new_v4())
}

pub fn ingest_publication(_datastore: &mut impl Datastore, _title: &str, _authors: Vec<String>) -> Result<(), indradb::Error> {
    // TODO: Implement ingestion logic once scrapers are ready
    Ok(())
}