use crate::db::ingestion::PublicationRecord;
use crate::logger;
use crate::scrapers::scraper::Scraper;
use crate::utilities::thread_safe_queue::{QueueConfig, QueueProducer, ThreadSafeQueue};
use async_trait::async_trait;
use chrono::{DateTime, Datelike, Utc};
use quick_xml::Reader;
use quick_xml::escape::unescape as quick_unescape;
use quick_xml::events::BytesStart;
use quick_xml::events::Event;
use reqwest::Client;
use tokio::time::{Duration, sleep};

use crate::config::ArxivSourceConfig;

/// ArXiv scraper that implements the Scraper trait.
#[derive(Clone, Debug)]
pub struct ArxivScraper {
    config: ArxivSourceConfig,
}

impl ArxivScraper {
    /// Create a new ArxivScraper with default configuration.
    pub fn new() -> Self {
        Self {
            config: ArxivSourceConfig::default(),
        }
    }

    /// Create a new ArxivScraper with custom configuration.
    pub fn with_config(config: ArxivSourceConfig) -> Self {
        Self { config }
    }
}

impl Default for ArxivScraper {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Scraper for ArxivScraper {
    async fn scrape_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        producer: QueueProducer<PublicationRecord>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        scrape_range_with_config_async(start, end, self.config.clone(), producer).await
    }
}

// Normalize text nodes: strip CDATA markers and unescape XML entities.
// Optimized to minimize allocations
fn normalize_text(txt: &str) -> String {
    let s = txt.trim();
    if s.is_empty() {
        return String::new();
    }
    if s.starts_with("<![CDATA[") && s.ends_with("]]>") {
        let inner = &s[9..s.len() - 3];
        return inner.trim().to_string();
    }
    // quick_unescape returns Cow - only allocates if unescaping is needed
    match quick_unescape(s) {
        Ok(cow) => {
            let unescaped = cow.as_ref();
            if unescaped == s {
                // No unescaping was needed, just return trimmed original
                s.to_string()
            } else {
                // Unescaping happened, trim the result
                unescaped.trim().to_string()
            }
        }
        Err(_) => s.to_string(),
    }
}

// Extract the `term` attribute (or namespaced variant) from a start element's
// attributes. Returns the value if present.
fn extract_primary_category_from_attrs(e: &BytesStart) -> Option<String> {
    for att in e.attributes().with_checks(false).flatten() {
        let key = att.key.as_ref();
        if key == b"term" || key.ends_with(b"term") {
            return Some(String::from_utf8_lossy(att.value.as_ref()).to_string());
        }
    }
    None
}

/// Find a subslice `needle` in `hay` and return the byte index if present.
///
/// This is a tiny, allocation-free byte-search helper used by the
/// streaming/sliding-window logic. It returns `None` when `needle` is empty
/// or longer than `hay` to avoid spurious matches and to mirror the previous
/// local implementation used throughout this module.
fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || hay.len() < needle.len() {
        return None;
    }
    hay.windows(needle.len()).position(|w| w == needle)
}

/// Drain and parse any complete `<entry>...</entry>` slices from `buf_acc`.
///
/// This helper repeatedly searches `buf_acc` for the next `<entry` start and
/// a matching `</entry>` end. For each complete slice found it calls
/// `parse_entry_str` and, when a `PublicationRecord` is returned, pushes it
/// into `results`. Consumed bytes (from the beginning of `buf_acc` up to the
/// end of the matched `</entry>`) are removed from `buf_acc` to keep the
/// buffer small and to mirror streaming consumption semantics.
///
/// The function is intentionally conservative: it only extracts *complete*
/// entries (both start and end found). Partial entries spanning future
/// chunks are left in `buf_acc` for later completion. This behaviour is
/// essential for correctness when streaming arbitrary chunk boundaries from
/// the network.
fn extract_complete_entries(
    buf_acc: &mut Vec<u8>,
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
    producer: &QueueProducer<PublicationRecord>,
    count: &mut usize,
) {
    loop {
        let start_opt = find_subslice(buf_acc, b"<entry");
        if start_opt.is_none() {
            // logger::debug("No <entry> start found in buffer");
            break;
        }
        let start = start_opt.unwrap();
        if let Some(rel_end) = find_subslice(&buf_acc[start..], b"</entry>") {
            let end = start + rel_end + b"</entry>".len();
            let entry_slice = &buf_acc[start..end];
            let entry_str = String::from_utf8_lossy(entry_slice);

            if let Some(rec) = parse_entry_str(&entry_str, start_date, end_date) {
                if let Err(e) = producer.submit(rec) {
                    logger::error(&format!("Failed to submit record: {}", e));
                }
                *count += 1;
            }

            buf_acc.drain(0..end);
            continue;
        } else {
            break;
        }
    }
}

// Parse a single `<entry>...</entry>` slice (slice-backed) and return a
// PublicationRecord if the entry's published date falls in [start_date, end_date).
fn parse_entry_str(
    entry_str: &str,
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
) -> Option<PublicationRecord> {
    let mut reader = Reader::from_str(entry_str);
    let mut tmp = Vec::new();
    let mut inside_entry = false;
    let mut cur_id: Option<String> = None;
    let mut cur_title: Option<String> = None;
    let mut cur_published: Option<String> = None;
    let mut cur_updated: Option<String> = None;
    let mut cur_journal_ref: Option<String> = None;
    let mut cur_primary_cat: Option<String> = None;
    let mut cur_authors: Vec<String> = Vec::new();

    // Main XML event loop to populate entry fields
    loop {
        tmp.clear();
        match reader.read_event_into(&mut tmp) {
            Ok(Event::Start(ref e)) => {
                let name = e.local_name();
                match name.as_ref() {
                    b"entry" => inside_entry = true,
                    // Capture the identifier text inside `<id>`.
                    b"id" if inside_entry => {
                        if let Ok(txt) = reader.read_text(e.name()) {
                            // Normalize CDATA and XML escapes before storing.
                            cur_id = Some(normalize_text(&txt));
                        }
                    }
                    // Capture the title for the entry.
                    b"title" if inside_entry => {
                        if let Ok(txt) = reader.read_text(e.name()) {
                            cur_title = Some(normalize_text(&txt));
                        }
                    }
                    // Capture the publication timestamp (RFC3339 expected).
                    b"published" if inside_entry => {
                        if let Ok(txt) = reader.read_text(e.name()) {
                            cur_published = Some(normalize_text(&txt));
                        }
                    }
                    // Capture the last updated timestamp (RFC3339 expected).
                    b"updated" if inside_entry => {
                        if let Ok(txt) = reader.read_text(e.name()) {
                            cur_updated = Some(normalize_text(&txt));
                        }
                    }
                    // Capture a `journal_ref` if present; this takes precedence
                    // over primary category for the `venue` field when present.
                    b"journal_ref" if inside_entry => {
                        if let Ok(txt) = reader.read_text(e.name()) {
                            cur_journal_ref = Some(normalize_text(&txt));
                        }
                    }
                    b"name" if inside_entry => {
                        if let Ok(txt) = reader.read_text(e.name()) {
                            cur_authors.push(normalize_text(&txt));
                        }
                    }
                    // Primary category element (possibly namespaced). Extract
                    // its `term` attribute and store it as a fallback venue.
                    b"arxiv:primary_category" | b"primary_category" if inside_entry => {
                        if let Some(val) = extract_primary_category_from_attrs(e) {
                            cur_primary_cat = Some(val);
                        }
                    }
                    _ => {
                        // Handle namespaced primary_category variants
                        if inside_entry
                            && name.as_ref().ends_with(b"primary_category")
                            && let Some(val) = extract_primary_category_from_attrs(e)
                        {
                            cur_primary_cat = Some(val);
                        }
                    }
                }
            }
            Ok(Event::End(ref e)) => {
                // On End events, if we've reached the end of an `<entry>`,
                // build and return a `PublicationRecord` populated from the
                // fields we've accumulated.
                if e.local_name().as_ref() == b"entry" {
                    // Use updated date for filtering (since we query by lastUpdatedDate)
                    // but fall back to published date if updated is not available
                    let date_str = cur_updated.as_ref().or(cur_published.as_ref())?;
                    let date_dt = DateTime::parse_from_rfc3339(date_str).ok()?;
                    let date_utc = date_dt.with_timezone(&Utc);

                    if date_utc < start_date || date_utc >= end_date {
                        return None;
                    }

                    // For the year field, use the published date (not updated)
                    let published_str = cur_published.as_ref()?;
                    let published_dt = DateTime::parse_from_rfc3339(published_str).ok()?;
                    let published_utc = published_dt.with_timezone(&Utc);
                    let year = published_utc.year() as u32;

                    // Fallback: if XML parser didn't find primary_category, try substring search
                    // This handles edge cases with malformed XML
                    if cur_primary_cat.is_none()
                        && let Some(idx) = entry_str.find("primary_category")
                        && let Some(term_pos) = entry_str[idx..].find("term=\"")
                    {
                        let start = idx + term_pos + "term=\"".len();
                        if let Some(endpos) = entry_str[start..].find('"') {
                            cur_primary_cat = Some(entry_str[start..start + endpos].to_string());
                        }
                    }

                    return Some(PublicationRecord {
                        id: cur_id.take().unwrap_or_default(),
                        title: cur_title.take().unwrap_or_default(),
                        authors: std::mem::take(&mut cur_authors),
                        year,
                        // Prefer journal reference; fall back to primary
                        // category when journal_ref is absent.
                        venue: cur_journal_ref.take().or(cur_primary_cat.take()),
                        source: String::from("arxiv"),
                    });
                }
            }
            Ok(Event::Eof) => break,
            Err(_) => break,
            _ => {}
        }
    }

    None
}
// Fallback: try a cheap substring parse for primary_category term if the
// XML attribute parsing didn't catch it (helps cover odd namespace/decl
// placement cases seen in tests).
// Note: this fallback runs only when the full parse above failed to
// return a record; it's a best-effort recovery.
// (Function end)

// Helper used in tests: given a sequence of byte chunks (as might be
// received from the network), extract and parse complete <entry>...</entry>
// slices and return PublicationRecords that fall inside the given date
// range. This mirrors the sliding-window logic in
// `scrape_range_with_config_async` and is intentionally `pub(crate)` so
// tests can exercise chunked inputs without needing a live HTTP server.
#[cfg(test)]
pub(crate) fn parse_entries_from_chunks(
    chunks: &[Vec<u8>],
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
) -> Vec<PublicationRecord> {
    // Local imports not needed here; parsing delegated to `parse_entry_str`

    let mut results = Vec::new();
    let mut buf_acc: Vec<u8> = Vec::new();

    let queue = ThreadSafeQueue::new(QueueConfig::default());
    let producer = queue.create_producer();

    let mut count = 0;
    for chunk in chunks {
        buf_acc.extend_from_slice(chunk);
        // Extract any complete entries from the accumulated buffer.
        extract_complete_entries(&mut buf_acc, start_date, end_date, &producer, &mut count);
    }

    // Drain queue
    drop(producer);
    while let Some(r) = queue.dequeue() {
        results.push(r);
    }

    results
}

/// Asynchronously scrape publication records from ArXiv for a given date range.
///
/// This is the high-level async entry point that uses `ArxivConfig::default()`
/// to obtain configuration (which may read environment variables). It fetches
/// pages from the ArXiv Atom API and parses `<entry>` elements in a
/// streaming, sliding-window manner so memory usage stays proportional to the
/// largest in-flight entry rather than the whole feed.
///
/// Parameters
/// - `start_date`, `end_date`: the inclusive/exclusive time window to include
///   publications (entries with `published` in [start_date, end_date) are
///   returned).
///
/// Returns
/// - `Ok(Vec<PublicationRecord>)` on success with the matched records.
/// - `Err(...)` on network, parsing, or runtime errors.
///
/// - `Err(...)` on network, parsing, or runtime errors.
///
/// Notes
/// - For tests or deterministic runs prefer `scrape_range_with_config_async`
///   and pass a custom `ArxivSourceConfig` (e.g., with `base_url` pointing at a
///   mock server).
#[coverage(off)]
pub async fn scrape_range_async(
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
    producer: QueueProducer<PublicationRecord>,
) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = ArxivSourceConfig::default();
    scrape_range_with_config_async(start_date, end_date, cfg, producer).await
}

/// Asynchronously scrape publication records using an explicit configuration.
///
/// Use this variant when you need deterministic behaviour (for example in
/// tests) or to customize scraping parameters such as `base_url`, `page_size`,
/// `channel_size`, or `delay_ms`.
///
/// Parameters
/// - `start_date`, `end_date`: include publications with `published` in
///   [start_date, end_date).
/// - `cfg`: an `ArxivSourceConfig` controlling endpoint, paging, and polite delays.
///
/// Returns
/// - `Ok(Vec<PublicationRecord>)` on success.
/// - `Err(...)` on failure (network, parsing, runtime errors).
pub async fn scrape_range_with_config_async(
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
    cfg: ArxivSourceConfig,
    producer: QueueProducer<PublicationRecord>,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new();
    let mut start: usize = 0;
    let page_size: usize = cfg.page_size;
    let _channel_size: usize = cfg.channel_size;
    let delay_ms: u64 = cfg.delay_ms;

    if start_date > end_date {
        return Ok(());
    }

    loop {
        // Format dates as YYYYMMDDHHMM
        let start_str = start_date.format("%Y%m%d%H%M").to_string();
        let end_str = end_date.format("%Y%m%d%H%M").to_string();

        let url = format!(
            "{}?search_query=lastUpdatedDate:[{}+TO+{}]&start={}&max_results={}",
            cfg.base_url, start_str, end_str, start, page_size
        );

        logger::debug(&format!("Fetching ArXiv page: {}", url));

        let mut resp = client.get(&url).send().await?;
        // When to stop (arXiv returns fewer than page_size when exhausted)
        // Remember how many results we processed in this page
        let mut page_record_count = 0;
        // Pre-allocate buffer: ~2KB per entry average, so for 5000 entries = ~10MB
        let mut buf_acc: Vec<u8> = Vec::with_capacity(10 * 1024 * 1024);

        loop {
            match resp.chunk().await? {
                Some(chunk) => buf_acc.extend_from_slice(&chunk),
                None => break,
            }

            extract_complete_entries(
                &mut buf_acc,
                start_date,
                end_date,
                &producer,
                &mut page_record_count,
            );
        }

        // Process any remaining complete entries after EOF
        extract_complete_entries(
            &mut buf_acc,
            start_date,
            end_date,
            &producer,
            &mut page_record_count,
        );

        sleep(Duration::from_millis(delay_ms)).await;

        // Close response to free resources
        drop(resp);

        // If fewer entries were added than page_size, we've reached the end
        logger::debug(&format!(
            "Parsed {} records from this page",
            page_record_count
        ));
        if page_record_count < page_size {
            break;
        }

        start += page_size;
    }

    Ok(())
}

/// Blocking wrapper around the async scraper.
///
/// This function constructs a small Tokio runtime and runs
/// `scrape_range_async` to provide a synchronous API for callers that cannot
/// be async. Prefer using the async variants where possible; this wrapper is
/// intended for compatibility and tests that require a blocking interface.
pub fn scrape_range(
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
) -> Result<Vec<PublicationRecord>, Box<dyn std::error::Error>> {
    // Create a small runtime to run the async function synchronously.
    let rt = tokio::runtime::Runtime::new()?;
    let queue = ThreadSafeQueue::new(QueueConfig::default());
    let producer = queue.create_producer();

    // We run the async function synchronously using the runtime.
    // We use an async block to move the producer into the future.
    rt.block_on(async move { scrape_range_async(start_date, end_date, producer).await })?;

    let mut results = Vec::new();

    while let Some(record) = queue.dequeue() {
        results.push(record);
    }

    Ok(results)
}

#[cfg(test)]
mod helper_tests {
    use super::*;
    use chrono::TimeZone;

    // Test: basic subslice search behavior (empty, short, and repeated cases)
    #[test]
    fn test_find_subslice_basic() {
        assert_eq!(find_subslice(b"abcdef", b"cd"), Some(2));
        assert_eq!(find_subslice(b"a", b""), None);
        assert_eq!(find_subslice(b"", b"a"), None);
        assert_eq!(find_subslice(b"aaa", b"aa"), Some(0));
    }

    // Test: parse an entry split across two chunks; ensure it's parsed once and
    // the consumed bytes are removed from the buffer.
    #[test]
    fn test_extract_complete_entries_single_and_chunked() {
        let start = Utc.with_ymd_and_hms(2025, 9, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2025, 12, 1, 0, 0, 0).unwrap();

        // Build an entry split across two chunks (simulate network chunking)
        let pre =
            b"<feed><entry><id>s1</id><published>2025-10-01T00:00:00Z</published><title>S".to_vec();
        let post = b"mall</title></entry></feed>".to_vec();

        let mut buf_acc: Vec<u8> = Vec::new();
        let mut results: Vec<PublicationRecord> = Vec::new();

        // After first chunk there is no complete entry
        buf_acc.extend_from_slice(&pre);
        let queue = ThreadSafeQueue::new(QueueConfig::default());
        let producer = queue.create_producer();
        let mut count = 0;

        extract_complete_entries(&mut buf_acc, start, end, &producer, &mut count);
        assert_eq!(count, 0, "no complete entries should be parsed yet");
        assert!(queue.dequeue().is_none());

        // After second chunk the entry should be parsed and removed from buffer
        buf_acc.extend_from_slice(&post);
        extract_complete_entries(&mut buf_acc, start, end, &producer, &mut count);
        assert_eq!(count, 1);

        let rec = queue.dequeue().unwrap();
        results.push(rec);
        let r = &results[0];
        assert_eq!(r.id, "s1");
        assert_eq!(r.title, "Small");
        // Buffer should no longer contain the consumed entry bytes (may contain trailing feed markers)
        assert!(buf_acc.len() < (pre.len() + post.len()));
    }

    // Test: when multiple <entry> elements exist in the buffer, extract them
    // all and return PublicationRecords for each.
    #[test]
    fn test_extract_complete_entries_multiple_in_buffer() {
        let start = Utc.with_ymd_and_hms(2025, 9, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2025, 12, 1, 0, 0, 0).unwrap();

        let combined = b"<feed><entry><id>a</id><published>2025-10-02T00:00:00Z</published><title>A</title></entry><entry><id>b</id><published>2025-10-03T00:00:00Z</published><title>B</title></entry></feed>".to_vec();

        let mut buf_acc = combined.clone();
        let mut results: Vec<PublicationRecord> = Vec::new();
        let queue = ThreadSafeQueue::new(QueueConfig::default());
        let producer = queue.create_producer();
        let mut count = 0;

        extract_complete_entries(&mut buf_acc, start, end, &producer, &mut count);
        assert_eq!(count, 2);

        while let Some(r) = queue.dequeue() {
            results.push(r);
        }
        let ids: Vec<_> = results.iter().map(|r| r.id.as_str()).collect();
        assert!(ids.contains(&"a"));
        assert!(ids.contains(&"b"));
    }
}
