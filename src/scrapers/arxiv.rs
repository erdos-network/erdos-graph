use crate::db::ingestion::PublicationRecord;
use crate::scrapers::scraper::Scraper;
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
    ) -> Result<Vec<PublicationRecord>, Box<dyn std::error::Error>> {
        scrape_range_with_config_async(start, end, self.config.clone()).await
    }
}

// Normalize text nodes: strip CDATA markers and unescape XML entities.
fn normalize_text(txt: &str) -> String {
    let s = txt.trim();
    if s.starts_with("<![CDATA[") && s.ends_with("]]>") {
        let inner = &s[9..s.len() - 3];
        return inner.trim().to_string();
    }
    if let Ok(cow) = quick_unescape(s) {
        return cow.into_owned().trim().to_string();
    }
    s.to_string()
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

// Returns true if the optional publication date parses and falls in
// [start_date, end_date).
fn published_in_range(
    pubdate_opt: &Option<String>,
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
) -> bool {
    pubdate_opt
        .as_ref()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| {
            let dt_utc = dt.with_timezone(&Utc);
            dt_utc >= start_date && dt_utc < end_date
        })
        .unwrap_or(false)
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
    results: &mut Vec<PublicationRecord>,
) {
    loop {
        let start_opt = find_subslice(buf_acc, b"<entry");
        if start_opt.is_none() {
            break;
        }
        let start = start_opt.unwrap();
        if let Some(rel_end) = find_subslice(&buf_acc[start..], b"</entry>") {
            let end = start + rel_end + b"</entry>".len();
            let entry_slice = &buf_acc[start..end];
            let entry_str = String::from_utf8_lossy(entry_slice);

            if let Some(rec) = parse_entry_str(&entry_str, start_date, end_date) {
                results.push(rec);
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
    let mut cur_journal_ref: Option<String> = None;
    let mut cur_primary_cat: Option<String> = None;
    let mut cur_authors: Vec<String> = Vec::new();

    // Cheap pre-scan fallback: if the entry slice contains a primary_category
    // attribute, grab its term value early so it's available when we build the
    // PublicationRecord (handles odd namespace placement in some feeds).
    if cur_primary_cat.is_none()
        && let Some(idx) = entry_str.find("primary_category")
        && let Some(term_pos) = entry_str[idx..].find("term=\"")
    {
        let start = idx + term_pos + "term=\"".len();
        if let Some(endpos) = entry_str[start..].find('"') {
            cur_primary_cat = Some(entry_str[start..start + endpos].to_string());
        }
    }

    // Main streaming XML event loop: iterate over XML events produced by
    // `quick-xml` and populate the temporary fields for the current `<entry>`.
    // We stop when we reach the end of the entry or encounter EOF/errors.
    loop {
        tmp.clear();
        match reader.read_event_into(&mut tmp) {
            Ok(Event::Start(ref e)) => {
                let name = e.local_name();
                match name.as_ref() {
                    // Start of an `<entry>` element: flip the flag so subsequent
                    // child elements are captured into the current record fields.
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
                    // Capture a `journal_ref` if present; this takes precedence
                    // over primary category for the `venue` field when present.
                    b"journal_ref" if inside_entry => {
                        if let Ok(txt) = reader.read_text(e.name()) {
                            cur_journal_ref = Some(normalize_text(&txt));
                        }
                    }
                    // Author name elements: append to the authors list.
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
                        // Handle namespaced primary_category variants like
                        // `{http://arxiv.org/schemas/atom}primary_category`. The
                        // element local-name may include a namespace, so we match
                        // on the suffix and reuse the attribute extractor.
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
                // decide whether the entry's published date falls inside the
                // requested window and, if so, build and return a
                // `PublicationRecord` populated from the fields we've
                // accumulated. If not included, we break to indicate no
                // matching record was produced from this slice.
                if e.local_name().as_ref() == b"entry" {
                    let include = published_in_range(&cur_published, start_date, end_date);
                    if include {
                        let year = cur_published
                            .as_ref()
                            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                            .map(|d| d.year() as u32)
                            .unwrap_or(0);
                        return Some(PublicationRecord {
                            id: cur_id.take().unwrap_or_default(),
                            title: cur_title.take().unwrap_or_default(),
                            authors: cur_authors.clone(),
                            year,
                            // Prefer journal reference; fall back to primary
                            // category when journal_ref is absent.
                            venue: cur_journal_ref.take().or(cur_primary_cat.take()),
                            source: String::from("arxiv"),
                        });
                    }
                    // No matching record for this entry, stop parsing this
                    // slice and return None to the caller.
                    break;
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

    for chunk in chunks {
        buf_acc.extend_from_slice(chunk);
        // Extract any complete entries from the accumulated buffer.
        extract_complete_entries(&mut buf_acc, start_date, end_date, &mut results);
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
) -> Result<Vec<PublicationRecord>, Box<dyn std::error::Error>> {
    let cfg = ArxivSourceConfig::default();
    scrape_range_with_config_async(start_date, end_date, cfg).await
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
) -> Result<Vec<PublicationRecord>, Box<dyn std::error::Error>> {
    let client = Client::new();
    let mut start: usize = 0;
    let page_size: usize = cfg.page_size;
    let _channel_size: usize = cfg.channel_size;
    let delay_ms: u64 = cfg.delay_ms;

    let mut results: Vec<PublicationRecord> = Vec::new();

    loop {
        // Format dates as YYYYMMDDHHMM
        let start_str = start_date.format("%Y%m%d%H%M").to_string();
        let end_str = end_date.format("%Y%m%d%H%M").to_string();

        let url = format!(
            "{}?search_query=lastUpdatedDate:[{}+TO+{}]&start={}&max_results={}",
            cfg.base_url, start_str, end_str, start, page_size
        );

        // Sliding-window streaming parser: accumulate chunks and extract <entry> slices
        let mut resp = client.get(&url).send().await?;
        // Remember how many results we had before this page so we can decide
        // when to stop (arXiv returns fewer than page_size when exhausted)
        let prev_results_len = results.len();
        let mut buf_acc: Vec<u8> = Vec::new();

        loop {
            match resp.chunk().await? {
                Some(chunk) => buf_acc.extend_from_slice(&chunk),
                None => break,
            }

            // Extract complete entries
            extract_complete_entries(&mut buf_acc, start_date, end_date, &mut results);
        }

        // Process any remaining complete entries after EOF
        extract_complete_entries(&mut buf_acc, start_date, end_date, &mut results);

        // Be polite to arXiv: small delay between paged requests
        sleep(Duration::from_millis(delay_ms)).await;

        // If fewer entries were added than page_size, we've reached the end
        let added = results.len() - prev_results_len;
        if added < page_size {
            break;
        }

        start += page_size;
    }

    Ok(results)
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
    rt.block_on(scrape_range_async(start_date, end_date))
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
        extract_complete_entries(&mut buf_acc, start, end, &mut results);
        assert!(
            results.is_empty(),
            "no complete entries should be parsed yet"
        );

        // After second chunk the entry should be parsed and removed from buffer
        buf_acc.extend_from_slice(&post);
        extract_complete_entries(&mut buf_acc, start, end, &mut results);
        assert_eq!(results.len(), 1);
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
        extract_complete_entries(&mut buf_acc, start, end, &mut results);
        assert_eq!(results.len(), 2);
        let ids: Vec<_> = results.iter().map(|r| r.id.as_str()).collect();
        assert!(ids.contains(&"a"));
        assert!(ids.contains(&"b"));
    }
}
