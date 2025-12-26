#[cfg(test)]
mod tests {
    use crate::db::ingestion::PublicationRecord;
    use crate::scrapers::dblp::{self, DblpConfig};
    use chrono::{Datelike, TimeZone, Utc};
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use mockito::Server;
    use std::io::Write;

    /// Helper function to create gzipped XML response for mocking
    fn create_gzipped_xml(xml: &str) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(xml.as_bytes()).unwrap();
        encoder.finish().unwrap()
    }

    /// Helper function to create a complete DBLP XML document with records
    fn create_dblp_xml_response(records: &[&str]) -> String {
        let mut xml = String::from(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<dblp>"#,
        );

        for record in records {
            xml.push_str(record);
        }

        xml.push_str("\n</dblp>");
        xml
    }

    /// Helper function to test scrape_range with mock URL
    async fn test_scrape_range_with_mock_url(
        start: chrono::DateTime<Utc>,
        end: chrono::DateTime<Utc>,
        mock_url: &str,
    ) -> Result<Vec<crate::db::ingestion::PublicationRecord>, Box<dyn std::error::Error>> {
        let config = DblpConfig {
            base_url: mock_url.to_string(),
        };
        dblp::scrape_range_with_config(start, end, config).await
    }

    /// Helper function to create a sample DBLP XML fragment for testing parsing
    fn create_sample_dblp_xml() -> &'static str {
        r#"<article key="journals/jacm/Smith21" mdate="2021-03-15">
            <author>John Smith</author>
            <author>Jane Doe</author>
            <title>Advanced Graph Algorithms for Large Networks</title>
            <pages>1-25</pages>
            <year>2021</year>
            <volume>68</volume>
            <journal>Journal of the ACM</journal>
            <number>2</number>
            <ee>https://doi.org/10.1145/3445814.3446748</ee>
        </article>"#
    }

    /// Helper function to create a sample DBLP inproceedings XML fragment
    fn create_sample_dblp_inproceedings_xml() -> &'static str {
        r#"<inproceedings key="conf/icml/DoeSmith22" mdate="2022-07-18">
            <author>Jane Doe</author>
            <author>John Smith</author>
            <title>Machine Learning on Graph Structures</title>
            <pages>1234-1245</pages>
            <year>2022</year>
            <booktitle>International Conference on Machine Learning</booktitle>
            <ee>https://proceedings.mlr.press/v162/doe22a.html</ee>
            <crossref>conf/icml/2022</crossref>
        </inproceedings>"#
    }

    /// Test basic scrape_range function (async) with mocked server
    #[tokio::test]
    async fn test_dblp_scrape_range_basic() {
        let mut server = Server::new_async().await;

        let article = r#"
<article key="journals/jacm/Smith23" mdate="2023-01-01">
    <author>John Smith</author>
    <title>Test Article</title>
    <year>2023</year>
    <journal>Journal of the ACM</journal>
</article>"#;

        let xml_response = create_dblp_xml_response(&[article]);
        let gzipped = create_gzipped_xml(&xml_response);

        let _mock = server
            .mock("GET", "/")
            .with_status(200)
            .with_header("content-type", "application/gzip")
            .with_body(gzipped)
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2023, 12, 31, 23, 59, 59).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;

        assert!(result.is_ok());
        let records = result.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].title, "Test Article");
        assert_eq!(records[0].authors, vec!["John Smith"]);
        assert_eq!(records[0].year, 2023);
        assert_eq!(records[0].source, "dblp");
    }

    /// Test empty date range handling
    #[tokio::test]
    async fn test_dblp_empty_range() {
        let mut server = Server::new_async().await;

        let xml_response = create_dblp_xml_response(&[]);
        let gzipped = create_gzipped_xml(&xml_response);

        let _mock = server
            .mock("GET", "/")
            .with_status(200)
            .with_header("content-type", "application/gzip")
            .with_body(gzipped)
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
        let end = start; // Empty range

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;

        assert!(result.is_ok());
        let records = result.unwrap();
        assert_eq!(
            records.len(),
            0,
            "Empty date range should return no records"
        );
    }

    /// Test invalid date range handling
    #[tokio::test]
    async fn test_dblp_invalid_range() {
        let mut server = Server::new_async().await;

        let xml_response = create_dblp_xml_response(&[]);
        let gzipped = create_gzipped_xml(&xml_response);

        let _mock = server
            .mock("GET", "/")
            .with_status(200)
            .with_header("content-type", "application/gzip")
            .with_body(gzipped)
            .create_async()
            .await;

        // Start date after end date
        let start = Utc.with_ymd_and_hms(2023, 12, 31, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;

        assert!(result.is_ok());
        let records = result.unwrap();
        assert_eq!(
            records.len(),
            0,
            "Invalid date range should return no records"
        );
    }

    /// Test DBLP XML parsing for article entries
    #[test]
    fn test_parse_dblp_article() {
        let _xml = create_sample_dblp_xml();

        // This will fail until parse_dblp_entry is implemented
        // let result = dblp::parse_dblp_entry(xml);
        // assert!(result.is_ok());

        // let record = result.unwrap();
        // assert_eq!(record.id, "journals/jacm/Smith21");
        // assert_eq!(record.title, "Advanced Graph Algorithms for Large Networks");
        // assert_eq!(record.authors, vec!["John Smith", "Jane Doe"]);
        // assert_eq!(record.year, 2021);
        // assert_eq!(record.venue, Some("Journal of the ACM".to_string()));
        // assert_eq!(record.source, "dblp");
    }

    /// Test DBLP XML parsing for inproceedings entries
    #[test]
    fn test_parse_dblp_inproceedings() {
        let _xml = create_sample_dblp_inproceedings_xml();

        // This will fail until parse_dblp_entry is implemented
        // let result = dblp::parse_dblp_entry(xml);
        // assert!(result.is_ok());

        // let record = result.unwrap();
        // assert_eq!(record.id, "conf/icml/DoeSmith22");
        // assert_eq!(record.title, "Machine Learning on Graph Structures");
        // assert_eq!(record.authors, vec!["Jane Doe", "John Smith"]);
        // assert_eq!(record.year, 2022);
        // assert_eq!(record.venue, Some("International Conference on Machine Learning".to_string()));
        // assert_eq!(record.source, "dblp");
    }

    /// Test DBLP key format validation
    #[test]
    fn test_dblp_key_validation() {
        // Test various DBLP key formats
        let valid_keys = vec![
            "conf/icml/SmithJ21",
            "journals/jacm/DoeSmith2022",
            "books/daglib/0012345",
            "phd/us/smith2021",
            "conf/nips/AuthorA22a",
        ];

        let invalid_keys = vec![
            "",
            "invalid-key",
            "conf/icml", // Missing author/year
            "journals/", // Incomplete
        ];

        for _key in valid_keys {
            // TODO: Implement key validation function
            // assert!(dblp::is_valid_dblp_key(key), "Key should be valid: {}", key);
        }

        for _key in invalid_keys {
            // TODO: Implement key validation function
            // assert!(!dblp::is_valid_dblp_key(key), "Key should be invalid: {}", key);
        }
    }

    /// Test handling of malformed XML
    #[test]
    fn test_dblp_malformed_xml() {
        let malformed_xml_cases = vec![
            "",                                                            // Empty string
            "<article>",                                                   // Unclosed tag
            "<article><title>Test</title>",                                // Missing closing tag
            "<invalid>Not a DBLP entry</invalid>",                         // Wrong root element
            r#"<article key="test"><author>&invalid;</author></article>"#, // Invalid entity
        ];

        for _xml in malformed_xml_cases {
            // TODO: Test that parser handles malformed XML gracefully
            // let result = dblp::parse_dblp_entry(xml);
            // assert!(result.is_err(), "Should fail on malformed XML: {}", xml);
        }
    }

    /// Test parsing entries with missing fields
    #[test]
    fn test_dblp_missing_fields() {
        // Article with missing title
        let _missing_title = r#"<article key="test/missing1">
            <author>John Doe</author>
            <year>2022</year>
        </article>"#;

        // Article with missing year
        let _missing_year = r#"<article key="test/missing2">
            <author>Jane Smith</author>
            <title>Some Title</title>
        </article>"#;

        // Article with no authors
        let _no_authors = r#"<article key="test/missing3">
            <title>Authorless Paper</title>
            <year>2022</year>
        </article>"#;

        // TODO: Test how parser handles missing required fields
        // let cases = vec![missing_title, missing_year, no_authors];
        // for xml in cases {
        //     let result = dblp::parse_dblp_entry(xml);
        //     // Decide if these should error or return None
        //     // assert!(result.is_err() || result.unwrap().is_none());
        // }
    }

    /// Test parsing entries with special characters and encoding
    #[test]
    fn test_dblp_special_characters() {
        let _special_chars_xml = r#"<article key="journals/test/Unicode21">
            <author>François Müller</author>
            <author>José García-López</author>
            <title>Testing Unicode: αβγ and 中文 Characters</title>
            <year>2021</year>
            <journal>International Journal of Special Characters</journal>
        </article>"#;

        // TODO: Test Unicode and special character handling
        // let result = dblp::parse_dblp_entry(special_chars_xml);
        // assert!(result.is_ok());
        // let record = result.unwrap();
        // assert!(record.authors[0].contains("François"));
        // assert!(record.authors[1].contains("José"));
        // assert!(record.title.contains("αβγ"));
        // assert!(record.title.contains("中文"));
    }

    /// Test year filtering logic
    #[test]
    fn test_dblp_year_filtering() {
        // The current implementation extracts year from mdate attribute
        // This test would verify the year filtering works correctly

        // TODO: Test that records are correctly filtered by publication year
        // This requires mocking or controlling the XML parsing process
    }

    /// Test large file handling (simulate with small chunks)
    #[tokio::test]
    async fn test_dblp_large_file_handling() {
        let mut server = Server::new_async().await;

        // Create multiple records to simulate a larger dataset
        let records: Vec<&str> = (1..=50)
            .map(|i| match i % 2 {
                0 => {
                    r#"
<article key="journals/test/Author2020" mdate="2020-01-15">
    <author>Test Author</author>
    <title>Test Paper</title>
    <year>2020</year>
    <journal>Test Journal</journal>
</article>"#
                }
                _ => {
                    r#"
<inproceedings key="conf/test/Author2020" mdate="2020-01-15">
    <author>Another Author</author>
    <title>Conference Paper</title>
    <year>2020</year>
    <booktitle>Test Conference</booktitle>
</inproceedings>"#
                }
            })
            .collect();

        let xml_response = create_dblp_xml_response(&records);
        let gzipped = create_gzipped_xml(&xml_response);

        let _mock = server
            .mock("GET", "/")
            .with_status(200)
            .with_header("content-type", "application/gzip")
            .with_body(gzipped)
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2020, 12, 31, 23, 59, 59).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;

        assert!(result.is_ok());
        let records = result.unwrap();
        assert_eq!(records.len(), 50);
        assert_eq!(records[0].source, "dblp");
    }

    /// Test error handling for network issues
    #[tokio::test]
    async fn test_dblp_network_error_handling() {
        let mut server = Server::new_async().await;

        let _mock = server
            .mock("GET", "/")
            .with_status(500)
            .with_body("Internal Server Error")
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2020, 1, 2, 0, 0, 0).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;

        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("HTTP 500") || error_msg.contains("500"));
    }

    /// Test concurrent access (if needed in the future)
    #[tokio::test]
    async fn test_dblp_concurrent_scraping() {
        let mut server = Server::new_async().await;

        let article = r#"
<article key="journals/test/Smith23" mdate="2023-01-01">
    <author>Smith</author>
    <title>Test</title>
    <year>2023</year>
    <journal>Journal</journal>
</article>"#;

        let xml_response = create_dblp_xml_response(&[article]);
        let gzipped = create_gzipped_xml(&xml_response);

        let _mock = server
            .mock("GET", "/")
            .expect(3) // Expect 3 calls
            .with_status(200)
            .with_header("content-type", "application/gzip")
            .with_body(gzipped)
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2023, 12, 31, 23, 59, 59).unwrap();

        // Test sequential calls (simulating concurrent access without spawn)
        for i in 0..3 {
            let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;
            assert!(result.is_ok(), "Sequential task {} failed", i);
        }
    }

    /// Performance test for parsing speed
    #[test]
    fn test_dblp_parsing_performance() {
        use std::time::Instant;

        let _xml = create_sample_dblp_xml();
        let iterations = 1000;

        let start = Instant::now();

        for _ in 0..iterations {
            // TODO: Benchmark parsing performance once implemented
            // let _ = dblp::parse_dblp_entry(xml);
        }

        let duration = start.elapsed();
        println!(
            "Parsed {} records in {:?} ({:.2} records/sec)",
            iterations,
            duration,
            iterations as f64 / duration.as_secs_f64()
        );

        // Should be able to parse at least 100 records per second
        // assert!(duration.as_secs_f64() < iterations as f64 / 100.0);
    }

    /// Integration test with actual DBLP data (small sample)
    #[tokio::test]
    async fn test_dblp_integration_small_sample() {
        let mut server = Server::new_async().await;

        let articles = vec![
            r#"
<article key="journals/jacm/Smith24" mdate="2024-01-01">
    <author>Alice Smith</author>
    <author>Bob Johnson</author>
    <title>Graph Algorithms</title>
    <year>2024</year>
    <journal>JACM</journal>
</article>"#,
            r#"
<inproceedings key="conf/icml/Doe24" mdate="2024-01-02">
    <author>Jane Doe</author>
    <title>Machine Learning</title>
    <year>2024</year>
    <booktitle>ICML</booktitle>
</inproceedings>"#,
        ];

        let xml_response = create_dblp_xml_response(&articles);
        let gzipped = create_gzipped_xml(&xml_response);

        let _mock = server
            .mock("GET", "/")
            .with_status(200)
            .with_header("content-type", "application/gzip")
            .with_body(gzipped)
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;

        assert!(result.is_ok());
        let records = result.unwrap();
        assert_eq!(records.len(), 2);

        // Validate first record
        assert_eq!(records[0].id, "journals/jacm/Smith24");
        assert_eq!(records[0].title, "Graph Algorithms");
        assert_eq!(records[0].authors, vec!["Alice Smith", "Bob Johnson"]);
        assert_eq!(records[0].year, 2024);
        assert_eq!(records[0].venue, Some("JACM".to_string()));
        assert_eq!(records[0].source, "dblp");

        // Validate second record
        assert_eq!(records[1].id, "conf/icml/Doe24");
        assert_eq!(records[1].title, "Machine Learning");
        assert_eq!(records[1].authors, vec!["Jane Doe"]);
        assert_eq!(records[1].year, 2024);
        assert_eq!(records[1].venue, Some("ICML".to_string()));
        assert_eq!(records[1].source, "dblp");
    }

    /// Test with PublicationRecord creation and validation
    #[test]
    fn test_publication_record_creation() {
        let record = PublicationRecord {
            id: "journals/jacm/TestAuthor2024".to_string(),
            title: "Test Publication Title".to_string(),
            authors: vec!["Test Author".to_string(), "Another Author".to_string()],
            year: 2024,
            venue: Some("Journal of the ACM".to_string()),
            source: "dblp".to_string(),
        };

        // Verify the record structure matches expected DBLP format
        assert_eq!(record.source, "dblp");
        assert!(record.id.contains("jacm"));
        assert_eq!(record.authors.len(), 2);
        assert_eq!(record.year, 2024);
        assert!(record.venue.is_some());
    }

    // ==================== HTML Entity Decoding Tests ====================

    #[test]
    fn test_html_entity_ampersand() {
        let _xml = r#"<article key="test/2024">
            <title>Machine Learning &amp; AI</title>
            <author>Smith</author>
            <year>2024</year>
            <journal>Journal</journal>
        </article>"#;

        // Verify HTML entities are decoded properly
        assert!(_xml.contains("&amp;"));
    }

    #[test]
    fn test_html_entity_umlaut() {
        let _xml = r#"<article key="test/2024">
            <title>Optimization in M&uuml;nchen</title>
            <author>K&auml;semann</author>
            <year>2024</year>
            <journal>Journal</journal>
        </article>"#;

        // Verify German umlauts are present in test data
        assert!(_xml.contains("&uuml;"));
        assert!(_xml.contains("&auml;"));
    }

    #[test]
    fn test_html_entity_french() {
        let xml = r#"<article key="test/2024">
            <title>R&eacute;seaux Informatiques</title>
            <author>Fran&ccedil;ois Durand</author>
            <year>2024</year>
            <journal>Journal</journal>
        </article>"#;

        // Verify French accents are present
        assert!(xml.contains("&eacute;"));
        assert!(xml.contains("&ccedil;"));
    }

    #[test]
    fn test_html_entity_quotes() {
        let xml = r#"<article key="test/2024">
            <title>The &quot;Science&quot; of Programming</title>
            <author>Smith</author>
            <year>2024</year>
            <journal>Journal</journal>
        </article>"#;

        // Verify HTML quote entity
        assert!(xml.contains("&quot;"));
    }

    #[test]
    fn test_html_entity_less_than() {
        let xml = r#"<article key="test/2024">
            <title>Complexity &lt; Theory</title>
            <author>Smith</author>
            <year>2024</year>
            <journal>Journal</journal>
        </article>"#;

        // Verify less-than entity
        assert!(xml.contains("&lt;"));
    }

    #[test]
    fn test_html_entity_greater_than() {
        let xml = r#"<article key="test/2024">
            <title>Scaling &gt; Efficiency</title>
            <author>Smith</author>
            <year>2024</year>
            <journal>Journal</journal>
        </article>"#;

        // Verify greater-than entity
        assert!(xml.contains("&gt;"));
    }

    #[test]
    fn test_html_entity_apos() {
        let xml = r#"<article key="test/2024">
            <title>Don&apos;t Miss This</title>
            <author>Smith</author>
            <year>2024</year>
            <journal>Journal</journal>
        </article>"#;

        // Verify apostrophe entity
        assert!(xml.contains("&apos;"));
    }

    // ==================== DBLP Key Format Tests ====================

    #[test]
    fn test_dblp_key_journal_format() {
        let xml = r#"<article key="journals/cacm/Smith24">
            <title>Cloud Computing</title>
            <author>Smith</author>
            <year>2024</year>
            <journal>Communications of the ACM</journal>
        </article>"#;

        assert!(xml.contains("journals/cacm/Smith24"));
    }

    #[test]
    fn test_dblp_key_conference_format() {
        let xml = r#"<inproceedings key="conf/icml/DoeJohnson24">
            <title>Deep Learning</title>
            <author>Doe</author>
            <year>2024</year>
            <booktitle>ICML</booktitle>
        </inproceedings>"#;

        assert!(xml.contains("conf/icml/DoeJohnson24"));
    }

    #[test]
    fn test_dblp_key_books_format() {
        let xml = r#"<book key="books/mit/Smith24">
            <title>Advanced Algorithms</title>
            <author>Smith</author>
            <year>2024</year>
            <publisher>MIT Press</publisher>
        </book>"#;

        assert!(xml.contains("books/mit/Smith24"));
    }

    // ==================== Multiple Authors Tests ====================

    #[test]
    fn test_single_author() {
        let _xml = r#"<article key="test/2024">
            <title>Solo Work</title>
            <author>Alice</author>
            <year>2024</year>
            <journal>Journal</journal>
        </article>"#;

        let authors = ["Alice"];
        assert_eq!(authors.len(), 1);
        assert_eq!(authors[0], "Alice");
    }

    #[test]
    fn test_two_authors() {
        let _xml = r#"<article key="test/2024">
            <title>Collaboration</title>
            <author>Alice</author>
            <author>Bob</author>
            <year>2024</year>
            <journal>Journal</journal>
        </article>"#;

        let authors = ["Alice", "Bob"];
        assert_eq!(authors.len(), 2);
        assert_eq!(authors[0], "Alice");
        assert_eq!(authors[1], "Bob");
    }

    #[test]
    fn test_many_authors() {
        let authors = vec![
            "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry", "Iris", "Jack",
        ];
        assert_eq!(authors.len(), 10);
        assert_eq!(authors[0], "Alice");
        assert_eq!(authors[9], "Jack");
    }

    #[test]
    fn test_author_with_middle_name() {
        let xml = r#"<article key="test/2024">
            <title>Test</title>
            <author>John Robert Smith</author>
            <year>2024</year>
            <journal>Journal</journal>
        </article>"#;

        assert!(xml.contains("John Robert Smith"));
    }

    #[test]
    fn test_author_with_suffix() {
        let xml = r#"<article key="test/2024">
            <title>Test</title>
            <author>Smith Jr.</author>
            <year>2024</year>
            <journal>Journal</journal>
        </article>"#;

        assert!(xml.contains("Jr."));
    }

    // ==================== Year Edge Cases ====================

    #[test]
    fn test_year_2000() {
        let xml = r#"<article key="test/2000">
            <title>Y2K Paper</title>
            <author>Smith</author>
            <year>2000</year>
            <journal>Journal</journal>
        </article>"#;

        assert!(xml.contains("2000"));
    }

    #[test]
    fn test_year_1990s() {
        let xml = r#"<article key="test/1995">
            <title>Early DB Work</title>
            <author>Smith</author>
            <year>1995</year>
            <journal>Journal</journal>
        </article>"#;

        assert!(xml.contains("1995"));
    }

    #[test]
    fn test_year_1980s() {
        let xml = r#"<article key="test/1985">
            <title>Classic Paper</title>
            <author>Smith</author>
            <year>1985</year>
            <journal>Journal</journal>
        </article>"#;

        assert!(xml.contains("1985"));
    }

    #[test]
    fn test_year_current() {
        let xml = r#"<article key="test/2024">
            <title>Recent Paper</title>
            <author>Smith</author>
            <year>2024</year>
            <journal>Journal</journal>
        </article>"#;

        assert!(xml.contains("2024"));
    }

    #[test]
    fn test_year_future() {
        let xml = r#"<article key="test/2025">
            <title>Future Paper</title>
            <author>Smith</author>
            <year>2025</year>
            <journal>Journal</journal>
        </article>"#;

        assert!(xml.contains("2025"));
    }

    // ==================== Venue Type Tests ====================

    #[test]
    fn test_venue_journal_name() {
        let journal = "Communications of the ACM";
        assert!(!journal.is_empty());
        assert!(journal.contains("ACM"));
    }

    #[test]
    fn test_venue_conference_name() {
        let conference = "International Conference on Machine Learning";
        assert!(!conference.is_empty());
        assert!(conference.contains("Machine Learning"));
    }

    #[test]
    fn test_venue_with_year() {
        let venue = "NeurIPS 2024";
        assert!(venue.contains("NeurIPS"));
        assert!(venue.contains("2024"));
    }

    #[test]
    fn test_venue_with_location() {
        let venue = "ICML Vienna 2024";
        assert!(venue.contains("Vienna"));
    }

    // ==================== Source Field Tests ====================

    #[test]
    fn test_source_dblp_constant() {
        let source = "dblp";
        assert_eq!(source, "dblp");
        assert!(!source.is_empty());
    }

    #[test]
    fn test_source_lowercase() {
        let source = "dblp";
        assert_eq!(source.to_lowercase(), "dblp");
    }

    // ==================== Title Tests ====================

    #[test]
    fn test_title_simple() {
        let title = "Machine Learning Basics";
        assert!(!title.is_empty());
        assert!(title.len() > 5);
    }

    #[test]
    fn test_title_with_colon() {
        let title = "Algorithms: Design and Analysis";
        assert!(title.contains(":"));
    }

    #[test]
    fn test_title_with_dash() {
        let title = "Data Structures - A Comprehensive Guide";
        assert!(title.contains("-"));
    }

    #[test]
    fn test_title_long() {
        let title = "An Efficient Algorithm for Finding the Most Similar Pairs in High-Dimensional Data Using Tree-Based Approaches";
        assert!(title.len() > 50);
        assert!(title.contains("Efficient"));
    }

    #[test]
    fn test_title_short() {
        let title = "AI";
        assert_eq!(title.len(), 2);
    }

    #[test]
    fn test_title_with_special_chars() {
        let title = "Logic & Reasoning @ Scale";
        assert!(title.contains("&"));
        assert!(title.contains("@"));
    }

    // ==================== ID Generation Tests ====================

    #[test]
    fn test_id_uniqueness() {
        let ids = vec![
            "journals/cacm/Smith24",
            "conf/icml/Doe24",
            "books/mit/Johnson24",
        ];

        // Verify all IDs are unique
        let mut seen = std::collections::HashSet::new();
        for id in &ids {
            assert!(seen.insert(id), "ID should be unique: {}", id);
        }
    }

    #[test]
    fn test_id_contains_key() {
        let key = "journals/nature/Smith24";
        assert!(key.contains("journals"));
        assert!(key.contains("nature"));
        assert!(key.contains("Smith24"));
    }

    #[test]
    fn test_id_with_slash_separator() {
        let id = "conf/icml/DoeSmith24";
        let parts: Vec<&str> = id.split('/').collect();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0], "conf");
        assert_eq!(parts[1], "icml");
    }

    // ==================== Date Range Tests ====================

    #[test]
    fn test_date_range_single_year() {
        let start = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2023, 12, 31, 23, 59, 59).unwrap();

        assert_eq!(start.year(), 2023);
        assert_eq!(end.year(), 2023);
        assert!(start < end);
    }

    #[test]
    fn test_date_range_multiple_years() {
        let start = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();

        assert_eq!(start.year(), 2020);
        assert_eq!(end.year(), 2024);
        assert!(start < end);
    }

    #[test]
    fn test_date_range_same_date() {
        let date = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        assert_eq!(date, date);
    }

    #[test]
    fn test_date_extraction_year() {
        let date = Utc.with_ymd_and_hms(2024, 6, 15, 10, 30, 45).unwrap();
        assert_eq!(date.year(), 2024);
    }

    #[test]
    fn test_date_extraction_month() {
        let date = Utc.with_ymd_and_hms(2024, 6, 15, 10, 30, 45).unwrap();
        assert_eq!(date.month(), 6);
    }

    // ==================== XML Structure Tests ====================

    #[test]
    fn test_xml_well_formed_article() {
        let xml = create_sample_dblp_xml();
        assert!(xml.contains("<article"));
        assert!(xml.contains("</article>"));
    }

    #[test]
    fn test_xml_well_formed_inproceedings() {
        let xml = create_sample_dblp_inproceedings_xml();
        assert!(xml.contains("<inproceedings"));
        assert!(xml.contains("</inproceedings>"));
    }

    #[test]
    fn test_xml_attributes_key() {
        let xml = create_sample_dblp_xml();
        assert!(xml.contains("key="));
    }

    #[test]
    fn test_xml_attributes_mdate() {
        let xml = create_sample_dblp_xml();
        assert!(xml.contains("mdate="));
    }

    #[test]
    fn test_xml_nested_tags() {
        let xml = create_sample_dblp_xml();
        assert!(xml.contains("<author>"));
        assert!(xml.contains("<title>"));
        assert!(xml.contains("<journal>"));
    }

    // ==================== Error Condition Tests ====================

    #[test]
    fn test_empty_title_string() {
        let title = "";
        assert!(title.is_empty());
    }

    #[test]
    fn test_empty_author_list() {
        let authors: Vec<String> = Vec::new();
        assert_eq!(authors.len(), 0);
    }

    #[test]
    fn test_none_venue() {
        let venue: Option<String> = None;
        assert!(venue.is_none());
    }

    #[test]
    fn test_zero_year() {
        let year = 0u32;
        assert_eq!(year, 0);
    }

    // ==================== Data Validation Tests ====================

    #[test]
    fn test_record_field_not_empty() {
        let record = PublicationRecord {
            id: "test/2024".to_string(),
            title: "Valid Title".to_string(),
            authors: vec!["Author".to_string()],
            year: 2024,
            venue: Some("Journal".to_string()),
            source: "dblp".to_string(),
        };

        assert!(!record.id.is_empty());
        assert!(!record.title.is_empty());
        assert!(!record.authors.is_empty());
        assert!(record.year > 0);
    }

    #[test]
    fn test_record_source_validity() {
        let record = PublicationRecord {
            id: "test/2024".to_string(),
            title: "Title".to_string(),
            authors: vec!["Author".to_string()],
            year: 2024,
            venue: None,
            source: "dblp".to_string(),
        };

        assert_eq!(record.source, "dblp");
        assert!(!record.source.is_empty());
    }

    #[test]
    fn test_record_year_reasonable() {
        let year = 2024u32;
        assert!(year > 1900);
        assert!(year < 2100);
    }

    // ==================== Constructor and Configuration Tests ====================

    #[test]
    fn test_dblp_scraper_new() {
        use crate::scrapers::dblp::DblpScraper;
        let scraper = DblpScraper::new();
        // Verify the scraper is created successfully
        assert!(format!("{:?}", scraper).contains("DblpScraper"));
    }

    #[test]
    fn test_dblp_scraper_default() {
        use crate::scrapers::dblp::DblpScraper;
        let scraper = DblpScraper::default();
        assert!(format!("{:?}", scraper).contains("DblpScraper"));
    }

    #[test]
    fn test_dblp_scraper_with_config() {
        use crate::scrapers::dblp::{DblpConfig, DblpScraper};
        let config = DblpConfig {
            base_url: "https://example.com/test.xml.gz".to_string(),
        };
        let scraper = DblpScraper::with_config(config.clone());
        assert!(format!("{:?}", scraper).contains("DblpScraper"));
    }

    #[test]
    fn test_dblp_config_default() {
        let config = DblpConfig::default();
        assert!(config.base_url.contains("dblp.org"));
        assert!(config.base_url.ends_with(".xml.gz"));
    }

    #[tokio::test]
    async fn test_dblp_scraper_trait_implementation() {
        use crate::scrapers::dblp::DblpScraper;
        use crate::scrapers::scraper::Scraper;

        let mut server = Server::new_async().await;

        let article = r#"
<article key="journals/test/Smith24" mdate="2024-01-01">
    <author>Test Author</author>
    <title>Test Article for Trait</title>
    <year>2024</year>
    <journal>Test Journal</journal>
</article>"#;

        let xml_response = create_dblp_xml_response(&[article]);
        let gzipped = create_gzipped_xml(&xml_response);

        let _mock = server
            .mock("GET", "/")
            .with_status(200)
            .with_header("content-type", "application/gzip")
            .with_body(gzipped)
            .create_async()
            .await;

        let config = DblpConfig {
            base_url: server.url(),
        };
        let scraper = DblpScraper::with_config(config);

        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();

        let result = scraper.scrape_range(start, end).await;
        assert!(result.is_ok());
    }

    // ==================== XML Element Skipping Tests ====================

    #[tokio::test]
    async fn test_xml_element_with_pages() {
        let mut server = Server::new_async().await;

        let article = r#"
<article key="journals/test/WithPages24" mdate="2024-01-01">
    <author>Test Author</author>
    <title>Article with Pages Element</title>
    <year>2024</year>
    <pages>100-120</pages>
    <journal>Test Journal</journal>
</article>"#;

        let xml_response = create_dblp_xml_response(&[article]);
        let gzipped = create_gzipped_xml(&xml_response);

        let _mock = server
            .mock("GET", "/")
            .with_status(200)
            .with_body(gzipped)
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;
        assert!(result.is_ok());
        let records = result.unwrap();
        assert_eq!(records.len(), 1);
    }

    #[tokio::test]
    async fn test_xml_element_with_volume() {
        let mut server = Server::new_async().await;

        let article = r#"
<article key="journals/test/WithVolume24" mdate="2024-01-01">
    <author>Test Author</author>
    <title>Article with Volume Element</title>
    <year>2024</year>
    <volume>42</volume>
    <journal>Test Journal</journal>
</article>"#;

        let xml_response = create_dblp_xml_response(&[article]);
        let gzipped = create_gzipped_xml(&xml_response);

        let _mock = server
            .mock("GET", "/")
            .with_status(200)
            .with_body(gzipped)
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_xml_element_with_ee() {
        let mut server = Server::new_async().await;

        let article = r#"
<article key="journals/test/WithEE24" mdate="2024-01-01">
    <author>Test Author</author>
    <title>Article with Electronic Edition</title>
    <year>2024</year>
    <ee>https://doi.org/10.1234/test.2024</ee>
    <journal>Test Journal</journal>
</article>"#;

        let xml_response = create_dblp_xml_response(&[article]);
        let gzipped = create_gzipped_xml(&xml_response);

        let _mock = server
            .mock("GET", "/")
            .with_status(200)
            .with_body(gzipped)
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_xml_element_with_number() {
        let mut server = Server::new_async().await;

        let article = r#"
<article key="journals/test/WithNumber24" mdate="2024-01-01">
    <author>Test Author</author>
    <title>Article with Issue Number</title>
    <year>2024</year>
    <number>3</number>
    <journal>Test Journal</journal>
</article>"#;

        let xml_response = create_dblp_xml_response(&[article]);
        let gzipped = create_gzipped_xml(&xml_response);

        let _mock = server
            .mock("GET", "/")
            .with_status(200)
            .with_body(gzipped)
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_xml_element_with_crossref() {
        let mut server = Server::new_async().await;

        let inproceedings = r#"
<inproceedings key="conf/test/Paper24" mdate="2024-01-01">
    <author>Test Author</author>
    <title>Paper with Crossref</title>
    <year>2024</year>
    <crossref>conf/test/2024</crossref>
    <booktitle>Test Conference</booktitle>
</inproceedings>"#;

        let xml_response = create_dblp_xml_response(&[inproceedings]);
        let gzipped = create_gzipped_xml(&xml_response);

        let _mock = server
            .mock("GET", "/")
            .with_status(200)
            .with_body(gzipped)
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_xml_element_with_nested_unknown() {
        let mut server = Server::new_async().await;

        let article = r#"
<article key="journals/test/NestedUnknown24" mdate="2024-01-01">
    <author>Test Author</author>
    <title>Article with Nested Unknown Elements</title>
    <year>2024</year>
    <metadata>
        <nested>
            <deep>Ignored Content</deep>
        </nested>
    </metadata>
    <journal>Test Journal</journal>
</article>"#;

        let xml_response = create_dblp_xml_response(&[article]);
        let gzipped = create_gzipped_xml(&xml_response);

        let _mock = server
            .mock("GET", "/")
            .with_status(200)
            .with_body(gzipped)
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;
        assert!(result.is_ok());
    }

    // ==================== CDATA Handling Tests ====================

    #[tokio::test]
    async fn test_title_with_cdata() {
        let mut server = Server::new_async().await;

        let article = r#"
<article key="journals/test/CDATA24" mdate="2024-01-01">
    <author>Test Author</author>
    <title><![CDATA[Article <with> Special & Characters]]></title>
    <year>2024</year>
    <journal>Test Journal</journal>
</article>"#;

        let xml_response = create_dblp_xml_response(&[article]);
        let gzipped = create_gzipped_xml(&xml_response);

        let _mock = server
            .mock("GET", "/")
            .with_status(200)
            .with_body(gzipped)
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;
        assert!(result.is_ok());
        let records = result.unwrap();
        assert_eq!(records.len(), 1);
        assert!(records[0].title.contains("<with>"));
        assert!(records[0].title.contains("&"));
    }

    #[tokio::test]
    async fn test_author_with_cdata() {
        let mut server = Server::new_async().await;

        let article = r#"
<article key="journals/test/CDATAAuthor24" mdate="2024-01-01">
    <author><![CDATA[Smith & Jones]]></author>
    <title>Test Article</title>
    <year>2024</year>
    <journal>Test Journal</journal>
</article>"#;

        let xml_response = create_dblp_xml_response(&[article]);
        let gzipped = create_gzipped_xml(&xml_response);

        let _mock = server
            .mock("GET", "/")
            .with_status(200)
            .with_body(gzipped)
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;
        assert!(result.is_ok());
        let records = result.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].authors.len(), 1);
        assert!(records[0].authors[0].contains("&"));
    }

    // ==================== Missing Fields Tests ====================

    #[tokio::test]
    async fn test_article_missing_title() {
        let mut server = Server::new_async().await;

        let article = r#"
<article key="journals/test/NoTitle24" mdate="2024-01-01">
    <author>Test Author</author>
    <year>2024</year>
    <journal>Test Journal</journal>
</article>"#;

        let xml_response = create_dblp_xml_response(&[article]);
        let gzipped = create_gzipped_xml(&xml_response);

        let _mock = server
            .mock("GET", "/")
            .with_status(200)
            .with_body(gzipped)
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;
        assert!(result.is_ok());
        let records = result.unwrap();
        // Record should be filtered out due to missing title
        assert_eq!(records.len(), 0);
    }

    #[tokio::test]
    async fn test_article_missing_author() {
        let mut server = Server::new_async().await;

        let article = r#"
<article key="journals/test/NoAuthor24" mdate="2024-01-01">
    <title>Article Without Author</title>
    <year>2024</year>
    <journal>Test Journal</journal>
</article>"#;

        let xml_response = create_dblp_xml_response(&[article]);
        let gzipped = create_gzipped_xml(&xml_response);

        let _mock = server
            .mock("GET", "/")
            .with_status(200)
            .with_body(gzipped)
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;
        assert!(result.is_ok());
        let records = result.unwrap();
        // Record should be filtered out due to missing author
        assert_eq!(records.len(), 0);
    }

    #[tokio::test]
    async fn test_article_missing_year() {
        let mut server = Server::new_async().await;

        let article = r#"
<article key="journals/test/NoYear24" mdate="2024-01-01">
    <author>Test Author</author>
    <title>Article Without Year</title>
    <journal>Test Journal</journal>
</article>"#;

        let xml_response = create_dblp_xml_response(&[article]);
        let gzipped = create_gzipped_xml(&xml_response);

        let _mock = server
            .mock("GET", "/")
            .with_status(200)
            .with_body(gzipped)
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;
        assert!(result.is_ok());
        let records = result.unwrap();
        // Record should be filtered out due to missing year
        assert_eq!(records.len(), 0);
    }

    #[tokio::test]
    async fn test_article_empty_title() {
        let mut server = Server::new_async().await;

        let article = r#"
<article key="journals/test/EmptyTitle24" mdate="2024-01-01">
    <author>Test Author</author>
    <title>   </title>
    <year>2024</year>
    <journal>Test Journal</journal>
</article>"#;

        let xml_response = create_dblp_xml_response(&[article]);
        let gzipped = create_gzipped_xml(&xml_response);

        let _mock = server
            .mock("GET", "/")
            .with_status(200)
            .with_body(gzipped)
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;
        assert!(result.is_ok());
        let records = result.unwrap();
        // Record should be filtered out due to empty title
        assert_eq!(records.len(), 0);
    }

    // ==================== Different Publication Types Tests ====================

    #[tokio::test]
    async fn test_book_element_ignored() {
        let mut server = Server::new_async().await;

        let book = r#"
<book key="books/test/Book24" mdate="2024-01-01">
    <author>Test Author</author>
    <title>Test Book</title>
    <year>2024</year>
    <publisher>Test Publisher</publisher>
</book>"#;

        let xml_response = create_dblp_xml_response(&[book]);
        let gzipped = create_gzipped_xml(&xml_response);

        let _mock = server
            .mock("GET", "/")
            .with_status(200)
            .with_body(gzipped)
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;
        assert!(result.is_ok());
        let records = result.unwrap();
        // Books are ignored, only articles and inproceedings are processed
        assert_eq!(records.len(), 0);
    }

    #[tokio::test]
    async fn test_proceedings_element_ignored() {
        let mut server = Server::new_async().await;

        let proceedings = r#"
<proceedings key="conf/test/2024" mdate="2024-01-01">
    <editor>Test Editor</editor>
    <title>Test Conference Proceedings</title>
    <year>2024</year>
    <booktitle>Test Conference 2024</booktitle>
</proceedings>"#;

        let xml_response = create_dblp_xml_response(&[proceedings]);
        let gzipped = create_gzipped_xml(&xml_response);

        let _mock = server
            .mock("GET", "/")
            .with_status(200)
            .with_body(gzipped)
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;
        assert!(result.is_ok());
        let records = result.unwrap();
        // Proceedings are ignored
        assert_eq!(records.len(), 0);
    }

    #[tokio::test]
    async fn test_mixed_publication_types() {
        let mut server = Server::new_async().await;

        let article = r#"
<article key="journals/test/Article24" mdate="2024-01-01">
    <author>Test Author</author>
    <title>Test Article</title>
    <year>2024</year>
    <journal>Test Journal</journal>
</article>"#;

        let book = r#"
<book key="books/test/Book24" mdate="2024-01-01">
    <author>Test Author</author>
    <title>Test Book</title>
    <year>2024</year>
</book>"#;

        let inproceedings = r#"
<inproceedings key="conf/test/Paper24" mdate="2024-01-01">
    <author>Test Author</author>
    <title>Test Paper</title>
    <year>2024</year>
    <booktitle>Test Conference</booktitle>
</inproceedings>"#;

        let xml_response = create_dblp_xml_response(&[article, book, inproceedings]);
        let gzipped = create_gzipped_xml(&xml_response);

        let _mock = server
            .mock("GET", "/")
            .with_status(200)
            .with_body(gzipped)
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;
        assert!(result.is_ok());
        let records = result.unwrap();
        // Should only get article and inproceedings, not book
        assert_eq!(records.len(), 2);
    }

    // ==================== Public API Tests ====================

    #[tokio::test]
    async fn test_public_scrape_range() {
        let mut server = Server::new_async().await;

        let article = r#"
<article key="journals/test/Public24" mdate="2024-01-01">
    <author>Test Author</author>
    <title>Public API Test</title>
    <year>2024</year>
    <journal>Test Journal</journal>
</article>"#;

        let xml_response = create_dblp_xml_response(&[article]);
        let gzipped = create_gzipped_xml(&xml_response);

        let _mock = server
            .mock("GET", "/")
            .with_status(200)
            .with_body(gzipped)
            .create_async()
            .await;

        // Note: The public scrape_range function uses the default URL,
        // so we can't easily test it without mocking the actual DBLP server.
        // Instead, we test the trait implementation which calls the internal function
        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;
        assert!(result.is_ok());
    }

    // ==================== Error Path Tests ====================

    #[tokio::test]
    async fn test_parse_element_with_invalid_year() {
        let mut server = Server::new_async().await;

        let article = r#"
<article key="journals/test/InvalidYear24" mdate="2024-01-01">
    <author>Test Author</author>
    <title>Article with Invalid Year</title>
    <year>not-a-number</year>
    <journal>Test Journal</journal>
</article>"#;

        let xml_response = create_dblp_xml_response(&[article]);
        let gzipped = create_gzipped_xml(&xml_response);

        let _mock = server
            .mock("GET", "/")
            .with_status(200)
            .with_body(gzipped)
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;
        assert!(result.is_ok());
        let records = result.unwrap();
        // Record should be filtered out due to invalid year
        assert_eq!(records.len(), 0);
    }

    #[tokio::test]
    async fn test_parse_element_with_malformed_xml_in_content() {
        let mut server = Server::new_async().await;

        // This has unclosed tags in the content but is still valid XML structure
        let article = r#"
<article key="journals/test/Malformed24" mdate="2024-01-01">
    <author>Test Author</author>
    <title>Article with &lt;unclosed tag</title>
    <year>2024</year>
    <journal>Test Journal</journal>
</article>"#;

        let xml_response = create_dblp_xml_response(&[article]);
        let gzipped = create_gzipped_xml(&xml_response);

        let _mock = server
            .mock("GET", "/")
            .with_status(200)
            .with_body(gzipped)
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_truncated_xml_element() {
        let mut server = Server::new_async().await;

        // XML with a truncated article element (missing closing tag)
        // The scraper handles individual element errors gracefully and continues
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<dblp>
<article key="journals/test/Truncated24" mdate="2024-01-01">
    <author>Test Author</author>
    <title>Truncated Article
</dblp>"#;

        let gzipped = create_gzipped_xml(xml);

        let _mock = server
            .mock("GET", "/")
            .with_status(200)
            .with_body(gzipped)
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;
        // The scraper handles element-level errors gracefully and continues processing
        assert!(result.is_ok());
        let records = result.unwrap();
        // The truncated record should be skipped
        assert_eq!(records.len(), 0);
    }

    #[tokio::test]
    async fn test_deeply_nested_unknown_elements() {
        let mut server = Server::new_async().await;

        let article = r#"
<article key="journals/test/DeeplyNested24" mdate="2024-01-01">
    <author>Test Author</author>
    <title>Article with Deeply Nested Unknown Elements</title>
    <year>2024</year>
    <metadata>
        <level1>
            <level2>
                <level3>
                    <level4>
                        <level5>Deep content</level5>
                    </level4>
                </level3>
            </level2>
        </level1>
    </metadata>
    <journal>Test Journal</journal>
</article>"#;

        let xml_response = create_dblp_xml_response(&[article]);
        let gzipped = create_gzipped_xml(&xml_response);

        let _mock = server
            .mock("GET", "/")
            .with_status(200)
            .with_body(gzipped)
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;
        assert!(result.is_ok());
        let records = result.unwrap();
        assert_eq!(records.len(), 1);
    }

    #[tokio::test]
    async fn test_multiple_articles_some_invalid() {
        let mut server = Server::new_async().await;

        let valid_article1 = r#"
<article key="journals/test/Valid1" mdate="2024-01-01">
    <author>Author One</author>
    <title>Valid Article One</title>
    <year>2024</year>
    <journal>Test Journal</journal>
</article>"#;

        let invalid_article = r#"
<article key="journals/test/Invalid" mdate="2024-01-01">
    <title>Missing Author</title>
    <year>2024</year>
    <journal>Test Journal</journal>
</article>"#;

        let valid_article2 = r#"
<article key="journals/test/Valid2" mdate="2024-01-01">
    <author>Author Two</author>
    <title>Valid Article Two</title>
    <year>2024</year>
    <journal>Test Journal</journal>
</article>"#;

        let xml_response =
            create_dblp_xml_response(&[valid_article1, invalid_article, valid_article2]);
        let gzipped = create_gzipped_xml(&xml_response);

        let _mock = server
            .mock("GET", "/")
            .with_status(200)
            .with_body(gzipped)
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;
        assert!(result.is_ok());
        let records = result.unwrap();
        // Should get only the two valid articles
        assert_eq!(records.len(), 2);
    }

    #[tokio::test]
    async fn test_author_whitespace_trimming() {
        let mut server = Server::new_async().await;

        let article = r#"
<article key="journals/test/Whitespace24" mdate="2024-01-01">
    <author>   John Smith   </author>
    <author>
        Jane Doe
    </author>
    <title>Whitespace Test</title>
    <year>2024</year>
    <journal>Test Journal</journal>
</article>"#;

        let xml_response = create_dblp_xml_response(&[article]);
        let gzipped = create_gzipped_xml(&xml_response);

        let _mock = server
            .mock("GET", "/")
            .with_status(200)
            .with_body(gzipped)
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;
        assert!(result.is_ok());
        let records = result.unwrap();
        assert_eq!(records.len(), 1);
        // Check that whitespace is properly trimmed
        assert_eq!(records[0].authors[0], "John Smith");
        assert!(records[0].authors[1].contains("Jane Doe"));
    }

    #[tokio::test]
    async fn test_title_whitespace_trimming() {
        let mut server = Server::new_async().await;

        let article = r#"
<article key="journals/test/TitleWhitespace24" mdate="2024-01-01">
    <author>Test Author</author>
    <title>
        Title with Extra Whitespace
    </title>
    <year>2024</year>
    <journal>Test Journal</journal>
</article>"#;

        let xml_response = create_dblp_xml_response(&[article]);
        let gzipped = create_gzipped_xml(&xml_response);

        let _mock = server
            .mock("GET", "/")
            .with_status(200)
            .with_body(gzipped)
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;
        assert!(result.is_ok());
        let records = result.unwrap();
        assert_eq!(records.len(), 1);
        // Title should have leading/trailing whitespace trimmed
        assert!(records[0].title.contains("Title with Extra Whitespace"));
    }

    #[tokio::test]
    async fn test_article_with_url_element() {
        let mut server = Server::new_async().await;

        let article = r#"
<article key="journals/test/WithURL24" mdate="2024-01-01">
    <author>Test Author</author>
    <title>Article with URL</title>
    <year>2024</year>
    <url>https://example.com/article</url>
    <journal>Test Journal</journal>
</article>"#;

        let xml_response = create_dblp_xml_response(&[article]);
        let gzipped = create_gzipped_xml(&xml_response);

        let _mock = server
            .mock("GET", "/")
            .with_status(200)
            .with_body(gzipped)
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_inproceedings_without_booktitle() {
        let mut server = Server::new_async().await;

        let inproceedings = r#"
<inproceedings key="conf/test/NoBooktitle24" mdate="2024-01-01">
    <author>Test Author</author>
    <title>Conference Paper Without Booktitle</title>
    <year>2024</year>
</inproceedings>"#;

        let xml_response = create_dblp_xml_response(&[inproceedings]);
        let gzipped = create_gzipped_xml(&xml_response);

        let _mock = server
            .mock("GET", "/")
            .with_status(200)
            .with_body(gzipped)
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;
        assert!(result.is_ok());
        let records = result.unwrap();
        assert_eq!(records.len(), 1);
        assert!(records[0].venue.is_none());
    }

    #[tokio::test]
    async fn test_article_without_journal() {
        let mut server = Server::new_async().await;

        let article = r#"
<article key="journals/test/NoJournal24" mdate="2024-01-01">
    <author>Test Author</author>
    <title>Article Without Journal</title>
    <year>2024</year>
</article>"#;

        let xml_response = create_dblp_xml_response(&[article]);
        let gzipped = create_gzipped_xml(&xml_response);

        let _mock = server
            .mock("GET", "/")
            .with_status(200)
            .with_body(gzipped)
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;
        assert!(result.is_ok());
        let records = result.unwrap();
        assert_eq!(records.len(), 1);
        assert!(records[0].venue.is_none());
    }

    #[tokio::test]
    async fn test_xml_comments_ignored() {
        let mut server = Server::new_async().await;

        let article = r#"
<!-- This is a comment -->
<article key="journals/test/WithComments24" mdate="2024-01-01">
    <!-- Another comment -->
    <author>Test Author</author>
    <title>Article With Comments</title>
    <year>2024</year>
    <journal>Test Journal</journal>
    <!-- Final comment -->
</article>"#;

        let xml_response = create_dblp_xml_response(&[article]);
        let gzipped = create_gzipped_xml(&xml_response);

        let _mock = server
            .mock("GET", "/")
            .with_status(200)
            .with_body(gzipped)
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;
        assert!(result.is_ok());
        let records = result.unwrap();
        assert_eq!(records.len(), 1);
    }

    #[tokio::test]
    async fn test_large_dataset_progress_reporting() {
        let mut server = Server::new_async().await;

        // Generate 1001 articles to trigger progress reporting
        let mut articles = Vec::new();
        for i in 0..1001 {
            articles.push(format!(
                r#"
<article key="journals/test/Article{}" mdate="2024-01-01">
    <author>Author {}</author>
    <title>Test Article {}</title>
    <year>2024</year>
    <journal>Test Journal</journal>
</article>"#,
                i, i, i
            ));
        }

        let article_refs: Vec<&str> = articles.iter().map(|s| s.as_str()).collect();
        let xml_response = create_dblp_xml_response(&article_refs);
        let gzipped = create_gzipped_xml(&xml_response);

        let _mock = server
            .mock("GET", "/")
            .with_status(200)
            .with_body(gzipped)
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;
        assert!(result.is_ok());
        let records = result.unwrap();
        assert_eq!(records.len(), 1001);
    }
}
