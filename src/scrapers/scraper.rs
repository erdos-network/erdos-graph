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

    /// Scrape publications with a specific mode (optional, for scrapers that support multiple modes).
    /// 
    /// Default implementation calls `scrape_range` and ignores the mode parameter.
    /// Scrapers that support modes (like DBLP with "search" vs "xml") should override this.
    async fn scrape_range_with_mode(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        _mode: &str,
        producer: QueueProducer<PublicationRecord>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Default implementation ignores mode and calls regular scrape_range
        self.scrape_range(start, end, producer).await
    }
}
