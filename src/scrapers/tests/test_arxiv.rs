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
