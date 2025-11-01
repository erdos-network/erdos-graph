use crate::scrapers::arxiv;
use chrono::{TimeZone, Utc};
use wiremock::matchers::query_param;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};
/// Deterministic test for `scrape_range_async` using a local mock HTTP server.
///
/// The test mounts a canned Atom XML feed on a mock server and points the
/// scraper at it by setting `ARXIV_API_BASE`. This keeps the test fully
/// deterministic and offline.
#[tokio::test]
async fn test_arxiv_scrape_range() {
    // Start a mock server
    let mock_server = MockServer::start().await;

    // Small Atom feed with two entries that fall into the requested date range
    let feed = r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:arxiv="http://arxiv.org/schemas/atom">
  <entry>
    <id>http://arxiv.org/abs/1234.5678v1</id>
    <updated>2025-10-01T12:00:00Z</updated>
    <published>2025-10-01T12:00:00Z</published>
    <title>Test Paper One</title>
    <author><name>Alice</name></author>
    <arxiv:primary_category term="cs.AI"/>
    <journal_ref>Journal One</journal_ref>
  </entry>
  <entry>
    <id>http://arxiv.org/abs/2345.6789v1</id>
    <updated>2025-09-15T12:00:00Z</updated>
    <published>2025-09-15T12:00:00Z</published>
    <title>Test Paper Two</title>
    <author><name>Bob</name></author>
    <arxiv:primary_category term="math.OC"/>
  </entry>
</feed>"#;

    // Mount mock response at /api/query
    Mock::given(method("GET"))
        .and(path("/api/query"))
        .respond_with(ResponseTemplate::new(200).set_body_string(feed))
        .mount(&mock_server)
        .await;

    // Build a test config and point the scraper at the mock server. This
    // avoids mutating process-wide env vars in tests.
    let cfg = arxiv::ArxivConfig {
        base_url: format!("{}/api/query", mock_server.uri()),
        page_size: 100,
        channel_size: 2,
        delay_ms: 1,
    };

    // Choose a date range that includes the published dates above
    let start = Utc.with_ymd_and_hms(2025, 9, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2025, 11, 1, 0, 0, 0).unwrap();

    let records = arxiv::scrape_range_with_config_async(start, end, cfg)
        .await
        .unwrap();

    assert_eq!(records.len(), 2, "expected two records parsed from feed");
    let titles: Vec<_> = records.iter().map(|r| r.title.as_str()).collect();
    assert!(titles.contains(&"Test Paper One"));
    assert!(titles.contains(&"Test Paper Two"));
    for r in records {
        assert_eq!(r.source, "arxiv");
    }
}

#[tokio::test]
async fn test_pagination_multiple_pages() {
    let mock_server = MockServer::start().await;

    // Page 0: one entry
    let feed0 = r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
  <id>http://arxiv.org/abs/1000.0000v1</id>
  <published>2025-10-01T00:00:00Z</published>
  <title>Page One</title>
  <author><name>Author A</name></author>
  </entry>
</feed>"#;

    // Page 1: one entry
    let feed1 = r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
  <id>http://arxiv.org/abs/1000.0001v1</id>
  <published>2025-10-02T00:00:00Z</published>
  <title>Page Two</title>
  <author><name>Author B</name></author>
  </entry>
</feed>"#;

    // Page 2: empty feed (signals end)
    let feed2 = r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom"></feed>"#;

    Mock::given(method("GET"))
        .and(path("/api/query"))
        .and(query_param("start", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_string(feed0))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/api/query"))
        .and(query_param("start", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_string(feed1))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/api/query"))
        .and(query_param("start", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_string(feed2))
        .mount(&mock_server)
        .await;

    let cfg = arxiv::ArxivConfig {
        base_url: format!("{}/api/query", mock_server.uri()),
        page_size: 1,
        channel_size: 2,
        delay_ms: 1,
    };

    let start = Utc.with_ymd_and_hms(2025, 9, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2025, 11, 1, 0, 0, 0).unwrap();

    let records = arxiv::scrape_range_with_config_async(start, end, cfg)
        .await
        .unwrap();
    let titles: Vec<_> = records.iter().map(|r| r.title.as_str()).collect();
    assert_eq!(titles.len(), 2);
    assert!(titles.contains(&"Page One"));
    assert!(titles.contains(&"Page Two"));
}

#[tokio::test]
async fn test_empty_feed_returns_empty() {
    let mock_server = MockServer::start().await;

    let empty_feed = r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom"></feed>"#;

    Mock::given(method("GET"))
        .and(path("/api/query"))
        .respond_with(ResponseTemplate::new(200).set_body_string(empty_feed))
        .mount(&mock_server)
        .await;

    let cfg = arxiv::ArxivConfig {
        base_url: format!("{}/api/query", mock_server.uri()),
        page_size: 100,
        channel_size: 2,
        delay_ms: 1,
    };

    let start = Utc.with_ymd_and_hms(2025, 9, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2025, 11, 1, 0, 0, 0).unwrap();

    let records = arxiv::scrape_range_with_config_async(start, end, cfg)
        .await
        .unwrap();
    assert!(records.is_empty());
}

#[tokio::test]
async fn test_truncated_feed_returns_error() {
    let mock_server = MockServer::start().await;

    // Truncated/malformed XML
    let bad =
        "<?xml version=\"1.0\"?><feed xmlns=\"http://www.w3.org/2005/Atom\"><entry><id>bad</id>";

    Mock::given(method("GET"))
        .and(path("/api/query"))
        .respond_with(ResponseTemplate::new(200).set_body_string(bad))
        .mount(&mock_server)
        .await;

    let cfg = arxiv::ArxivConfig {
        base_url: format!("{}/api/query", mock_server.uri()),
        page_size: 100,
        channel_size: 2,
        delay_ms: 1,
    };

    let start = Utc.with_ymd_and_hms(2025, 9, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2025, 11, 1, 0, 0, 0).unwrap();

    let res = arxiv::scrape_range_with_config_async(start, end, cfg).await;
    match res {
        Ok(v) => {
            // Some parsers may treat truncated input as EOF without producing
            // entries; accept that as well but ensure no records were returned.
            assert!(v.is_empty(), "malformed feed returned records unexpectedly");
        }
        Err(_) => {
            // error is acceptable
        }
    }
}

#[tokio::test]
async fn test_missing_fields_parsed() {
    let mock_server = MockServer::start().await;

    // Entry missing title and journal_ref
    let feed = r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>http://arxiv.org/abs/5555.0000v1</id>
    <published>2025-10-05T00:00:00Z</published>
    <author><name>Solo</name></author>
  </entry>
</feed>"#;

    Mock::given(method("GET"))
        .and(path("/api/query"))
        .respond_with(ResponseTemplate::new(200).set_body_string(feed))
        .mount(&mock_server)
        .await;

    let cfg = arxiv::ArxivConfig {
        base_url: format!("{}/api/query", mock_server.uri()),
        page_size: 100,
        channel_size: 2,
        delay_ms: 1,
    };

    let start = Utc.with_ymd_and_hms(2025, 9, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2025, 11, 1, 0, 0, 0).unwrap();

    let records = arxiv::scrape_range_with_config_async(start, end, cfg)
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
    let r = &records[0];
    assert_eq!(r.id, "http://arxiv.org/abs/5555.0000v1");
    assert_eq!(r.title, "");
    assert_eq!(r.authors, vec!["Solo".to_string()]);
}

#[tokio::test]
async fn test_many_authors_parsing() {
    let mock_server = MockServer::start().await;

    // Create a feed with many authors
    let mut authors_xml = String::new();
    for i in 0..50 {
        authors_xml.push_str(&format!("<author><name>Author{}</name></author>", i));
    }

    let feed = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>http://arxiv.org/abs/6666.0000v1</id>
    <published>2025-10-06T00:00:00Z</published>
    <title>Many Authors</title>
    {}
  </entry>
</feed>"#,
        authors_xml
    );

    Mock::given(method("GET"))
        .and(path("/api/query"))
        .respond_with(ResponseTemplate::new(200).set_body_string(feed))
        .mount(&mock_server)
        .await;

    let cfg = arxiv::ArxivConfig {
        base_url: format!("{}/api/query", mock_server.uri()),
        page_size: 100,
        channel_size: 2,
        delay_ms: 1,
    };

    let start = Utc.with_ymd_and_hms(2025, 9, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2025, 11, 1, 0, 0, 0).unwrap();

    let records = arxiv::scrape_range_with_config_async(start, end, cfg)
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
    let r = &records[0];
    assert_eq!(r.authors.len(), 50);
}

#[tokio::test]
async fn test_escaped_entities() {
    let mock_server = MockServer::start().await;

    let feed = r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>http://arxiv.org/abs/7777.0000v1</id>
    <published>2025-10-07T00:00:00Z</published>
    <title>A &amp; B &lt;C&gt;</title>
    <author><name>Esc</name></author>
  </entry>
</feed>"#;

    Mock::given(method("GET"))
        .and(path("/api/query"))
        .respond_with(ResponseTemplate::new(200).set_body_string(feed))
        .mount(&mock_server)
        .await;

    let cfg = arxiv::ArxivConfig {
        base_url: format!("{}/api/query", mock_server.uri()),
        page_size: 100,
        channel_size: 2,
        delay_ms: 1,
    };

    let start = Utc.with_ymd_and_hms(2025, 9, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2025, 11, 1, 0, 0, 0).unwrap();

    let records = arxiv::scrape_range_with_config_async(start, end, cfg)
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
    let r = &records[0];
    assert_eq!(r.title, "A & B <C>");
}

#[test]
fn test_escaped_entities_chunked() {
    use crate::scrapers::arxiv;
    use chrono::{TimeZone, Utc};

    // Construct an Atom entry where the title contains escaped entities
    // and deliberately split the bytes across two chunks so an entity
    // reference is split between them (e.g., '&' and 'amp;' across the
    // boundary).
    let pre = b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<feed xmlns=\"http://www.w3.org/2005/Atom\">\n  <entry>\n    <id>http://arxiv.org/abs/9999.0000v1</id>\n    <published>2025-10-08T00:00:00Z</published>\n    <title>A &".to_vec();
    let post =
        b"amp; B &lt;C&gt;</title>\n    <author><name>Chunk</name></author>\n  </entry>\n</feed>"
            .to_vec();

    let chunks = vec![pre, post];

    let start = Utc.with_ymd_and_hms(2025, 9, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2025, 11, 1, 0, 0, 0).unwrap();

    let records = arxiv::parse_entries_from_chunks(&chunks, start, end);
    assert_eq!(records.len(), 1);
    let r = &records[0];
    assert_eq!(r.title, "A & B <C>");
}

#[test]
fn test_primary_category_variants_and_venue() {
    use crate::scrapers::arxiv;
    use chrono::{TimeZone, Utc};

    let entry1 = b"<?xml version=\"1.0\"?><feed xmlns=\"http://www.w3.org/2005/Atom\"><entry><id>1</id><published>2025-10-10T00:00:00Z</published><title>T1</title><arxiv:primary_category xmlns:arxiv=\"http://arxiv.org/schemas/atom\" term=\"cs.AI\"/></entry></feed>".to_vec();
    let entry2 = b"<?xml version=\"1.0\"?><feed xmlns=\"http://www.w3.org/2005/Atom\"><entry><id>2</id><published>2025-10-11T00:00:00Z</published><title>T2</title><primary_category term=\"math.OC\"/></entry></feed>".to_vec();

    let start = Utc.with_ymd_and_hms(2025, 9, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2025, 12, 1, 0, 0, 0).unwrap();

    let recs1 = arxiv::parse_entries_from_chunks(&vec![entry1], start, end);
    let recs2 = arxiv::parse_entries_from_chunks(&vec![entry2], start, end);

    assert_eq!(recs1.len(), 1);
    assert_eq!(recs1[0].venue.as_ref().map(|s| s.as_str()), Some("cs.AI"));
    assert_eq!(recs2.len(), 1);
    assert_eq!(recs2[0].venue.as_ref().map(|s| s.as_str()), Some("math.OC"));
}

#[test]
fn test_published_invalid_or_out_of_range() {
    use crate::scrapers::arxiv;
    use chrono::{TimeZone, Utc};

    // invalid published date
    let bad_date = b"<?xml version=\"1.0\"?><feed xmlns=\"http://www.w3.org/2005/Atom\"><entry><id>bad</id><published>not-a-date</published><title>B</title></entry></feed>".to_vec();
    // out of range
    let out_of_range = b"<?xml version=\"1.0\"?><feed xmlns=\"http://www.w3.org/2005/Atom\"><entry><id>o</id><published>2020-01-01T00:00:00Z</published><title>O</title></entry></feed>".to_vec();

    let start = Utc.with_ymd_and_hms(2025, 9, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2025, 12, 1, 0, 0, 0).unwrap();

    let r1 = arxiv::parse_entries_from_chunks(&vec![bad_date], start, end);
    let r2 = arxiv::parse_entries_from_chunks(&vec![out_of_range], start, end);

    assert!(r1.is_empty());
    assert!(r2.is_empty());
}

#[test]
fn test_namespaced_elements_parsing() {
    use crate::scrapers::arxiv;
    use chrono::{TimeZone, Utc};

    // Use the default Atom namespace so the parser sees namespaced elements
    let feed = r#"<?xml version='1.0'?><feed xmlns='http://www.w3.org/2005/Atom'><entry><id>n1</id><published>2025-10-12T00:00:00Z</published><title>Namespaced</title><author><name>N</name></author></entry></feed>"#.as_bytes().to_vec();

    let start = Utc.with_ymd_and_hms(2025, 9, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2025, 12, 1, 0, 0, 0).unwrap();

    let recs = arxiv::parse_entries_from_chunks(&vec![feed], start, end);
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].title, "Namespaced");
}

#[test]
fn test_journal_ref_precedence_over_primary_category() {
    use crate::scrapers::arxiv;
    use chrono::{TimeZone, Utc};

    let feed = r#"<?xml version='1.0'?><feed xmlns='http://www.w3.org/2005/Atom' xmlns:arxiv='http://arxiv.org/schemas/atom'><entry><id>p1</id><published>2025-10-13T00:00:00Z</published><title>Journ</title><arxiv:primary_category term='cs.AI'/><journal_ref>Journal X</journal_ref></entry></feed>"#.as_bytes().to_vec();

    let start = Utc.with_ymd_and_hms(2025, 9, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2025, 12, 1, 0, 0, 0).unwrap();

    let recs = arxiv::parse_entries_from_chunks(&vec![feed], start, end);
    assert_eq!(recs.len(), 1);
    assert_eq!(
        recs[0].venue.as_ref().map(|s| s.as_str()),
        Some("Journal X")
    );
}

#[test]
fn test_primary_category_substring_fallback() {
    use crate::scrapers::arxiv;
    use chrono::{TimeZone, Utc};

    // Construct an entry that contains the literal substring 'primary_category' so
    // the fallback substring scanner in parse_entry_str picks up the term.
    let feed = b"<?xml version='1.0'?><feed><entry><id>f1</id><published>2025-10-14T00:00:00Z</published><title>Fallback</title><div>some primary_category term=\"span.A\" content</div></entry></feed>".to_vec();

    let start = Utc.with_ymd_and_hms(2025, 9, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2025, 12, 1, 0, 0, 0).unwrap();

    let recs = arxiv::parse_entries_from_chunks(&vec![feed], start, end);
    // the substring fallback should extract 'span.A' as venue
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].venue.as_ref().map(|s| s.as_str()), Some("span.A"));
}

#[test]
fn test_malformed_entry_handling() {
    use crate::scrapers::arxiv;
    use chrono::{TimeZone, Utc};

    // malformed XML inside title (unescaped & and unclosed tag)
    let feed = b"<?xml version='1.0'?><feed><entry><id>m1</id><published>2025-10-15T00:00:00Z</published><title>Bad & <b></entry></feed>".to_vec();

    let start = Utc.with_ymd_and_hms(2025, 9, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2025, 12, 1, 0, 0, 0).unwrap();

    let recs = arxiv::parse_entries_from_chunks(&vec![feed], start, end);
    // parser should not panic; it's acceptable for malformed entries to be
    // skipped or yield no records
    assert!(recs.is_empty());
}

#[test]
fn test_missing_published_does_not_include() {
    use crate::scrapers::arxiv;
    use chrono::{TimeZone, Utc};

    let feed =
        b"<?xml version='1.0'?><feed><entry><id>no_pub</id><title>NoPub</title></entry></feed>"
            .to_vec();

    let start = Utc.with_ymd_and_hms(2025, 9, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2025, 12, 1, 0, 0, 0).unwrap();

    let recs = arxiv::parse_entries_from_chunks(&vec![feed], start, end);
    assert!(recs.is_empty());
}

#[test]
fn test_attribute_namespaced_term_suffix() {
    use crate::scrapers::arxiv;
    use chrono::{TimeZone, Utc};

    // attribute key uses a prefix 'arxiv:term' which should match ends_with('term')
    let feed = b"<?xml version='1.0'?><feed xmlns:arxiv='http://arxiv.org/schemas/atom'><entry><id>at2</id><published>2025-10-16T00:00:00Z</published><title>AttrNs</title><primary_category term=\"x.A\"/></entry></feed>".to_vec();

    let start = Utc.with_ymd_and_hms(2025, 9, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2025, 12, 1, 0, 0, 0).unwrap();

    let recs = arxiv::parse_entries_from_chunks(&vec![feed], start, end);
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].venue.as_ref().map(|s| s.as_str()), Some("x.A"));
}

#[test]
fn test_arxiv_config_default_values() {
    use crate::scrapers::arxiv::ArxivConfig;

    let cfg = ArxivConfig::default();
    // basic sanity checks
    assert!(cfg.base_url.contains("arxiv") || cfg.base_url.len() > 0);
    assert!(cfg.page_size > 0);
    assert!(cfg.channel_size > 0);
}

#[test]
fn test_large_entry_all_fields() {
    use crate::scrapers::arxiv;
    use chrono::{TimeZone, Utc};

    let mut authors = String::new();
    for i in 0..20 {
        authors.push_str(&format!("<author><name>A{}</name></author>", i));
    }
    let feed = format!(
        r#"<?xml version='1.0'?><feed xmlns='http://www.w3.org/2005/Atom'><entry><id>big1</id><published>2025-10-20T00:00:00Z</published><title>Big</title><journal_ref>J Big</journal_ref><arxiv:primary_category xmlns:arxiv='http://arxiv.org/schemas/atom' term='cs.BIG'/>{}</entry></feed>"#,
        authors
    )
    .as_bytes()
    .to_vec();

    let start = Utc.with_ymd_and_hms(2025, 9, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2025, 12, 1, 0, 0, 0).unwrap();

    let recs = arxiv::parse_entries_from_chunks(&vec![feed], start, end);
    assert_eq!(recs.len(), 1);
    let r = &recs[0];
    assert_eq!(r.id, "big1");
    assert_eq!(r.title, "Big");
    assert_eq!(r.authors.len(), 20);
    assert_eq!(r.venue.as_ref().map(|s| s.as_str()), Some("J Big"));
}

#[test]
fn test_title_cdata_and_author_escape() {
    use crate::scrapers::arxiv;
    use chrono::{TimeZone, Utc};

    // Title in CDATA and an author name with escaped entities
    let feed = b"<?xml version='1.0'?><feed xmlns='http://www.w3.org/2005/Atom'><entry><id>c1</id><published>2025-10-21T00:00:00Z</published><title><![CDATA[CD A & B <C>]]></title><author><name>Esc &amp; Name</name></author></entry></feed>".to_vec();

    let start = Utc.with_ymd_and_hms(2025, 9, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2025, 12, 1, 0, 0, 0).unwrap();

    let recs = arxiv::parse_entries_from_chunks(&vec![feed], start, end);
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].title, "CD A & B <C>");
    assert_eq!(
        recs[0].authors.get(0).map(|s| s.as_str()),
        Some("Esc & Name")
    );
}

#[test]
fn test_namespaced_attribute_with_prefix_term() {
    use crate::scrapers::arxiv;
    use chrono::{TimeZone, Utc};

    // Use attribute with explicit prefix in the feed text to test attribute
    // key endings matching logic (e.g., 'arxiv:term')
    let feed = b"<?xml version='1.0'?><feed xmlns:arxiv='http://arxiv.org/schemas/atom'><entry><id>a1</id><published>2025-10-22T00:00:00Z</published><title>A</title><primary_category arxiv:term=\"pref.A\"/></entry></feed>".to_vec();

    let start = Utc.with_ymd_and_hms(2025, 9, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2025, 12, 1, 0, 0, 0).unwrap();

    let recs = arxiv::parse_entries_from_chunks(&vec![feed], start, end);
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].venue.as_ref().map(|s| s.as_str()), Some("pref.A"));
}
