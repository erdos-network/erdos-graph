use crate::db::ingestion::PublicationRecord;
use chrono::{DateTime, Datelike, Utc};
use quick_xml::Reader;
// quick-xml unescape helper removed: we rely on read_text for decoding
use quick_xml::events::Event;
use quick_xml::escape::unescape as quick_unescape;
// streaming/channel parsing removed; we'll use sliding-window per-entry parsing
use reqwest::Client;
// streaming/channel parsing removed; keep imports minimal
use tokio::time::{Duration, sleep};

// Note: we rely on quick-xml's `read_text` to collect and decode element
// text. Avoid unescaping whole documents (that would break markup).

/// Configuration for the ArXiv scraper. Tests can construct this and pass it
/// to `scrape_range_with_config_async` to avoid relying on environment
/// variables.
#[derive(Clone, Debug)]
pub struct ArxivConfig {
    pub base_url: String,
    pub page_size: usize,
    pub channel_size: usize,
    /// Delay between pages in milliseconds
    pub delay_ms: u64,
}

// Helper used in tests: given a sequence of byte chunks (as might be
// received from the network), extract and parse complete <entry>...</entry>
// slices and return PublicationRecords that fall inside the given date
// range. This mirrors the sliding-window logic in
// `scrape_range_with_config_async` and is intentionally `pub(crate)` so
// tests can exercise chunked inputs without needing a live HTTP server.
pub(crate) fn parse_entries_from_chunks(
    chunks: &[Vec<u8>],
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
) -> Vec<PublicationRecord> {
    use quick_xml::Reader;
    use quick_xml::events::Event;

    fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
        if needle.is_empty() || hay.len() < needle.len() { return None; }
        hay.windows(needle.len()).position(|w| w == needle)
    }

    let mut results = Vec::new();
    let mut buf_acc: Vec<u8> = Vec::new();

    for chunk in chunks {
        buf_acc.extend_from_slice(chunk);

        loop {
            let start_opt = find_subslice(&buf_acc, b"<entry");
            if start_opt.is_none() { break; }
            let start = start_opt.unwrap();
            if let Some(rel_end) = find_subslice(&buf_acc[start..], b"</entry>") {
                let end = start + rel_end + b"</entry>".len();
                let entry_slice = &buf_acc[start..end];
                let entry_str = String::from_utf8_lossy(entry_slice);

                let mut reader = Reader::from_str(&entry_str);
                let mut tmp = Vec::new();
                let mut cur_id: Option<String> = None;
                let mut cur_title: Option<String> = None;
                let mut cur_published: Option<String> = None;
                let mut cur_journal_ref: Option<String> = None;
                let mut cur_primary_cat: Option<String> = None;
                let mut cur_authors: Vec<String> = Vec::new();

                loop {
                    tmp.clear();
                    match reader.read_event_into(&mut tmp) {
                        Ok(Event::Start(ref e)) => {
                            let name = e.local_name();
                            match name.as_ref() {
                                b"entry" | b"{http://www.w3.org/2005/Atom}entry" => {}
                                b"id" => if let Ok(txt) = reader.read_text(e.name()) { if let Ok(cow) = quick_unescape(&txt) { cur_id = Some(cow.into_owned().trim().to_string()) } else { cur_id = Some(txt.trim().to_string()) } },
                                b"title" => if let Ok(txt) = reader.read_text(e.name()) { if let Ok(cow) = quick_unescape(&txt) { cur_title = Some(cow.into_owned().trim().to_string()) } else { cur_title = Some(txt.trim().to_string()) } },
                                b"published" => if let Ok(txt) = reader.read_text(e.name()) { if let Ok(cow) = quick_unescape(&txt) { cur_published = Some(cow.into_owned().trim().to_string()) } else { cur_published = Some(txt.trim().to_string()) } },
                                b"journal_ref" => if let Ok(txt) = reader.read_text(e.name()) { if let Ok(cow) = quick_unescape(&txt) { cur_journal_ref = Some(cow.into_owned().trim().to_string()) } else { cur_journal_ref = Some(txt.trim().to_string()) } },
                                b"name" => if let Ok(txt) = reader.read_text(e.name()) { if let Ok(cow) = quick_unescape(&txt) { cur_authors.push(cow.into_owned().trim().to_string()) } else { cur_authors.push(txt.trim().to_string()) } },
                                b"arxiv:primary_category" | b"primary_category" => for attr in e.attributes().with_checks(false) { if let Ok(att) = attr { if att.key.as_ref() == b"term" { let val = String::from_utf8_lossy(att.value.as_ref()).to_string(); cur_primary_cat = Some(val); } } },
                                _ => {}
                            }
                        }
                        Ok(Event::End(ref e)) => {
                            if e.local_name().as_ref() == b"entry" {
                                let include = if let Some(pubdate) = &cur_published { if let Ok(dt) = DateTime::parse_from_rfc3339(pubdate) { let dt_utc = dt.with_timezone(&Utc); dt_utc >= start_date && dt_utc < end_date } else { false } } else { false };
                                if include {
                                    let year = cur_published.as_ref().and_then(|s| DateTime::parse_from_rfc3339(s).ok()).map(|d| d.year() as u32).unwrap_or(0);
                                    results.push(PublicationRecord { id: cur_id.take().unwrap_or_default(), title: cur_title.take().unwrap_or_default(), authors: cur_authors.clone(), year, venue: cur_journal_ref.take().or(cur_primary_cat.take()), source: String::from("arxiv"), });
                                }
                                break;
                            }
                        }
                        Ok(Event::Eof) => break,
                        Err(_) => break,
                        _ => {}
                    }
                }

                buf_acc.drain(0..end);
                continue;
            } else { break; }
        }
    }

    results
}
impl Default for ArxivConfig {
    fn default() -> Self {
        fn env_usize(name: &str, default: usize) -> usize {
            std::env::var(name).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
        }

        let base = std::env::var("ARXIV_API_BASE").unwrap_or_else(|_| {
            "http://export.arxiv.org/api/query".to_string()
        });
        ArxivConfig {
            base_url: base,
            page_size: env_usize("ARXIV_PAGE_SIZE", 100),
            channel_size: env_usize("ARXIV_CHANNEL_SIZE", 8),
            delay_ms: env_usize("ARXIV_DELAY_MS", 200) as u64,
        }
    }
}

/// Scrapes publication records from ArXiv for a given date range.
#[coverage(off)]
pub async fn scrape_range_async(
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
) -> Result<Vec<PublicationRecord>, Box<dyn std::error::Error>> {
    let cfg = ArxivConfig::default();
    scrape_range_with_config_async(start_date, end_date, cfg).await
}

/// Variant of the scraper that takes an explicit `ArxivConfig`. Prefer this in
/// tests so you can deterministically point the scraper at a mock server.
pub async fn scrape_range_with_config_async(
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
    cfg: ArxivConfig,
) -> Result<Vec<PublicationRecord>, Box<dyn std::error::Error>> {
    let client = Client::new();
    let mut start: usize = 0;
    let page_size: usize = cfg.page_size;
    let _channel_size: usize = cfg.channel_size;
    let delay_ms: u64 = cfg.delay_ms;

    let mut results: Vec<PublicationRecord> = Vec::new();

    loop {
        let url = format!(
            "{}?search_query=all&start={}&max_results={}",
            cfg.base_url, start, page_size
        );

        // Sliding-window streaming parser: accumulate chunks and extract <entry> slices
        let mut resp = client.get(&url).send().await?;
    // remember how many results we had before this page so we can decide
    // when to stop (arXiv returns fewer than page_size when exhausted)
    let prev_results_len = results.len();
        let mut buf_acc: Vec<u8> = Vec::new();

        fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
            if needle.is_empty() || hay.len() < needle.len() { return None; }
            hay.windows(needle.len()).position(|w| w == needle)
        }

        loop {
            match resp.chunk().await? {
                Some(chunk) => buf_acc.extend_from_slice(&chunk),
                None => break,
            }

            // extract complete entries
            loop {
                let start_opt = find_subslice(&buf_acc, b"<entry");
                if start_opt.is_none() { break; }
                let start = start_opt.unwrap();
                if let Some(rel_end) = find_subslice(&buf_acc[start..], b"</entry>") {
                    let end = start + rel_end + b"</entry>".len();
                    let entry_slice = &buf_acc[start..end];
                    let entry_str = String::from_utf8_lossy(entry_slice);

                    // parse entry slice using slice-backed reader
                    let mut reader = Reader::from_str(&entry_str);
                    let mut tmp = Vec::new();
                    let mut inside_entry = false;
                    let mut cur_id: Option<String> = None;
                    let mut cur_title: Option<String> = None;
                    let mut cur_published: Option<String> = None;
                    let mut cur_journal_ref: Option<String> = None;
                    let mut cur_primary_cat: Option<String> = None;
                    let mut cur_authors: Vec<String> = Vec::new();

                    loop {
                        tmp.clear();
                        match reader.read_event_into(&mut tmp) {
                            Ok(Event::Start(ref e)) => {
                                let name = e.local_name();
                                match name.as_ref() {
                                    b"entry" | b"{http://www.w3.org/2005/Atom}entry" => inside_entry = true,
                                    b"id" if inside_entry => if let Ok(txt) = reader.read_text(e.name()) { if let Ok(cow) = quick_unescape(&txt) { cur_id = Some(cow.into_owned().trim().to_string()) } else { cur_id = Some(txt.trim().to_string()) } },
                                    b"title" if inside_entry => if let Ok(txt) = reader.read_text(e.name()) { if let Ok(cow) = quick_unescape(&txt) { cur_title = Some(cow.into_owned().trim().to_string()) } else { cur_title = Some(txt.trim().to_string()) } },
                                    b"published" if inside_entry => if let Ok(txt) = reader.read_text(e.name()) { if let Ok(cow) = quick_unescape(&txt) { cur_published = Some(cow.into_owned().trim().to_string()) } else { cur_published = Some(txt.trim().to_string()) } },
                                    b"journal_ref" if inside_entry => if let Ok(txt) = reader.read_text(e.name()) { if let Ok(cow) = quick_unescape(&txt) { cur_journal_ref = Some(cow.into_owned().trim().to_string()) } else { cur_journal_ref = Some(txt.trim().to_string()) } },
                                    b"name" if inside_entry => if let Ok(txt) = reader.read_text(e.name()) { if let Ok(cow) = quick_unescape(&txt) { cur_authors.push(cow.into_owned().trim().to_string()) } else { cur_authors.push(txt.trim().to_string()) } },
                                    b"arxiv:primary_category" | b"primary_category" if inside_entry => for attr in e.attributes().with_checks(false) { if let Ok(att) = attr { if att.key.as_ref() == b"term" { let val = String::from_utf8_lossy(att.value.as_ref()).to_string(); cur_primary_cat = Some(val); } } },
                                    _ => {}
                                }
                            }
                            Ok(Event::End(ref e)) => {
                                if e.local_name().as_ref() == b"entry" || e.local_name().as_ref() == b"{http://www.w3.org/2005/Atom}entry" {
                                    let include = if let Some(pubdate) = &cur_published { if let Ok(dt) = DateTime::parse_from_rfc3339(pubdate) { let dt_utc = dt.with_timezone(&Utc); dt_utc >= start_date && dt_utc < end_date } else { false } } else { false };
                                    if include {
                                        let year = cur_published.as_ref().and_then(|s| DateTime::parse_from_rfc3339(s).ok()).map(|d| d.year() as u32).unwrap_or(0);
                                        results.push(PublicationRecord { id: cur_id.take().unwrap_or_default(), title: cur_title.take().unwrap_or_default(), authors: cur_authors.clone(), year, venue: cur_journal_ref.take().or(cur_primary_cat.take()), source: String::from("arxiv"), });
                                    }
                                    break;
                                }
                            }
                            Ok(Event::Eof) => break,
                            Err(_) => break,
                            _ => {}
                        }
                    }

                    // drain consumed bytes and continue extracting others
                    buf_acc.drain(0..end);
                    continue;
                } else { break; }
            }
        }

        // process any remaining complete entries after EOF
        loop {
            let start_opt = find_subslice(&buf_acc, b"<entry");
            if start_opt.is_none() { break; }
            let start = start_opt.unwrap();
            if let Some(rel_end) = find_subslice(&buf_acc[start..], b"</entry>") {
                let end = start + rel_end + b"</entry>".len();
                let entry_slice = &buf_acc[start..end];
                let entry_str = String::from_utf8_lossy(entry_slice);
                let mut reader = Reader::from_str(&entry_str);
                let mut tmp = Vec::new();
                let mut cur_id: Option<String> = None;
                let mut cur_title: Option<String> = None;
                let mut cur_published: Option<String> = None;
                let mut cur_journal_ref: Option<String> = None;
                let mut cur_primary_cat: Option<String> = None;
                let mut cur_authors: Vec<String> = Vec::new();

                loop {
                    tmp.clear();
                    match reader.read_event_into(&mut tmp) {
                        Ok(Event::Start(ref e)) => {
                            let name = e.local_name();
                            match name.as_ref() {
                                b"entry" | b"{http://www.w3.org/2005/Atom}entry" => {}
                                b"id" => if let Ok(txt) = reader.read_text(e.name()) { if let Ok(cow) = quick_unescape(&txt) { cur_id = Some(cow.into_owned().trim().to_string()) } else { cur_id = Some(txt.trim().to_string()) } },
                                b"title" => if let Ok(txt) = reader.read_text(e.name()) { if let Ok(cow) = quick_unescape(&txt) { cur_title = Some(cow.into_owned().trim().to_string()) } else { cur_title = Some(txt.trim().to_string()) } },
                                b"published" => if let Ok(txt) = reader.read_text(e.name()) { if let Ok(cow) = quick_unescape(&txt) { cur_published = Some(cow.into_owned().trim().to_string()) } else { cur_published = Some(txt.trim().to_string()) } },
                                b"journal_ref" => if let Ok(txt) = reader.read_text(e.name()) { if let Ok(cow) = quick_unescape(&txt) { cur_journal_ref = Some(cow.into_owned().trim().to_string()) } else { cur_journal_ref = Some(txt.trim().to_string()) } },
                                b"name" => if let Ok(txt) = reader.read_text(e.name()) { if let Ok(cow) = quick_unescape(&txt) { cur_authors.push(cow.into_owned().trim().to_string()) } else { cur_authors.push(txt.trim().to_string()) } },
                                b"arxiv:primary_category" | b"primary_category" => for attr in e.attributes().with_checks(false) { if let Ok(att) = attr { if att.key.as_ref() == b"term" { let val = String::from_utf8_lossy(att.value.as_ref()).to_string(); cur_primary_cat = Some(val); } } },
                                _ => {}
                            }
                        }
                        Ok(Event::End(ref e)) => { if e.local_name().as_ref() == b"entry" { let include = if let Some(pubdate) = &cur_published { if let Ok(dt) = DateTime::parse_from_rfc3339(pubdate) { let dt_utc = dt.with_timezone(&Utc); dt_utc >= start_date && dt_utc < end_date } else { false } } else { false }; if include { let year = cur_published.as_ref().and_then(|s| DateTime::parse_from_rfc3339(s).ok()).map(|d| d.year() as u32).unwrap_or(0); results.push(PublicationRecord { id: cur_id.take().unwrap_or_default(), title: cur_title.take().unwrap_or_default(), authors: cur_authors.clone(), year, venue: cur_journal_ref.take().or(cur_primary_cat.take()), source: String::from("arxiv"), }); } break } }
                        Ok(Event::Eof) => break,
                        Err(_) => break,
                        _ => {}
                    }
                }
                buf_acc.drain(0..end);
                continue;
            } else { break; }
        }

        // be polite to arXiv: small delay between paged requests
        sleep(Duration::from_millis(delay_ms)).await;

        // if fewer entries were added than page_size, we've reached the end
        let added = results.len() - prev_results_len;
        if added < page_size {
            break;
        }

        start += page_size;
    }

    Ok(results)
}

/// Synchronous wrapper for the async scraper for callers that expect a
/// blocking API. This keeps compatibility with existing synchronous code.
pub fn scrape_range(
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
) -> Result<Vec<PublicationRecord>, Box<dyn std::error::Error>> {
    // Create a small runtime to run the async function synchronously.
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(scrape_range_async(start_date, end_date))
}
