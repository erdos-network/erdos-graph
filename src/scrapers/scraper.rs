use crate::db::ingestion::PublicationRecord;
use crate::utilities::thread_safe_queue::QueueProducer;
use async_trait::async_trait;
use chrono::{DateTime, Utc};

#[async_trait]
pub trait Scraper: Send + Sync {
    /// Scrape publications in the given date range.
    async fn scrape_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        producer: QueueProducer<PublicationRecord>,
    ) -> Result<(), Box<dyn std::error::Error>>;
}
