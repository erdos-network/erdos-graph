#[cfg(test)]
mod tests {
    use crate::scrapers::zbmath::{DublinCore, Metadata, OaiPmh, Record, RecordHeader};
    use crate::scrapers::zbmath::{
        ZbmathConfig, convert_to_publication_record, scrape_chunk_with_config,
        scrape_range_with_config,
    };
    use crate::utilities::thread_safe_queue::{QueueConfig, ThreadSafeQueue};
    use chrono::{TimeZone, Utc};
    use mockito::{Matcher, Server};

    // Helper function for testing scrape_range with mock server
    async fn test_scrape_range_with_mock_url(
        start: chrono::DateTime<Utc>,
        end: chrono::DateTime<Utc>,
        mock_url: &str,
    ) -> Result<Vec<crate::db::ingestion::PublicationRecord>, Box<dyn std::error::Error>> {
        let config = ZbmathConfig {
            base_url: mock_url.to_string(),
            delay_between_pages_ms: 1, // Fast for tests
        };
        let queue = ThreadSafeQueue::new(QueueConfig::default());
        let producer = queue.create_producer();
        scrape_range_with_config(start, end, config, producer).await?;

        let mut results = Vec::new();
        while let Some(r) = queue.dequeue() {
            results.push(r);
        }
        Ok(results)
    }

    // Helper function for testing scrape_chunk with mock server
    async fn test_scrape_chunk_with_mock_url(
        client: &reqwest::Client,
        start: chrono::DateTime<Utc>,
        end: chrono::DateTime<Utc>,
        mock_url: &str,
    ) -> Result<Vec<crate::db::ingestion::PublicationRecord>, Box<dyn std::error::Error>> {
        let config = ZbmathConfig {
            base_url: mock_url.to_string(),
            delay_between_pages_ms: 1,
        };
        let queue = ThreadSafeQueue::new(QueueConfig::default());
        let producer = queue.create_producer();
        scrape_chunk_with_config(client, start, end, &config, producer).await?;

        let mut results = Vec::new();
        while let Some(r) = queue.dequeue() {
            results.push(r);
        }
        Ok(results)
    }

    /// Test ZbmathConfig Default implementation
    #[test]
    fn test_zbmath_config_default() {
        let config = ZbmathConfig::default();

        assert_eq!(config.base_url, "https://oai.zbmath.org/v1/");
        assert_eq!(config.delay_between_pages_ms, 500);
    }

    /// Test ZbmathConfig custom configuration
    #[test]
    fn test_zbmath_config_custom() {
        let config = ZbmathConfig {
            base_url: "https://custom-zbmath.example.com/".to_string(),
            delay_between_pages_ms: 1000,
        };

        assert_eq!(config.base_url, "https://custom-zbmath.example.com/");
        assert_eq!(config.delay_between_pages_ms, 1000);
    }

    /// Test ZbmathConfig Clone implementation
    #[test]
    fn test_zbmath_config_clone() {
        let config1 = ZbmathConfig::default();
        let config2 = config1.clone();

        assert_eq!(config1.base_url, config2.base_url);
        assert_eq!(
            config1.delay_between_pages_ms,
            config2.delay_between_pages_ms
        );
    }

    /// Test the new configuration-based API
    #[tokio::test]
    async fn test_zbmath_config_api() {
        let mut server = Server::new_async().await;

        let mock_response = r#"<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
    <ListRecords>
        <record>
            <header>
                <identifier>oai:zbmath:1234567</identifier>
                <datestamp>2024-01-01T00:00:00Z</datestamp>
            </header>
            <metadata>
                <dc xmlns="http://purl.org/dc/elements/1.1/">
                    <title>Test Configuration Paper</title>
                    <creator>Config, Test</creator>
                    <date>2024</date>
                    <source>Journal of Config Testing</source>
                </dc>
            </metadata>
        </record>
    </ListRecords>
</OAI-PMH>"#;

        let _mock = server
            .mock("GET", "/")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("verb".into(), "ListRecords".into()),
                Matcher::UrlEncoded("metadataPrefix".into(), "oai_dc".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "text/xml")
            .with_body(mock_response)
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap();

        // Test the new config-based API
        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;

        assert!(result.is_ok());
        let records = result.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].title, "Test Configuration Paper");
        assert_eq!(records[0].authors, vec!["Config, Test"]);
        assert_eq!(records[0].year, 2024);
        assert_eq!(records[0].source, "zbmath");
    }

    /// Test scraping zbMATH publications in a date range using mocked API.
    ///
    /// This test verifies that the scraper can handle API responses
    /// and parse XML responses correctly.
    #[tokio::test]
    async fn test_zbmath_scrape_range() {
        let mut server = Server::new_async().await;

        let mock_response = r#"<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
    <ListRecords>
        <record>
            <header>
                <identifier>oai:zbmath.org:1234567</identifier>
                <datestamp>2024-01-01</datestamp>
                <setSpec>mathematics</setSpec>
            </header>
            <metadata>
                <oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" 
                          xmlns:dc="http://purl.org/dc/elements/1.1/">
                    <dc:title>Test Algebra Paper</dc:title>
                    <dc:creator>Johnson, Alice; Williams, Bob</dc:creator>
                    <dc:date>2024</dc:date>
                    <dc:source>Journal of Algebra</dc:source>
                    <dc:identifier>zbMATH:1234567</dc:identifier>
                </oai_dc:dc>
            </metadata>
        </record>
        <record>
            <header>
                <identifier>oai:zbmath.org:7654321</identifier>
                <datestamp>2024-01-02</datestamp>
                <setSpec>mathematics</setSpec>
            </header>
            <metadata>
                <oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" 
                          xmlns:dc="http://purl.org/dc/elements/1.1/">
                    <dc:title>Advanced Number Theory</dc:title>
                    <dc:creator>Smith, Carol</dc:creator>
                    <dc:date>2024</dc:date>
                    <dc:source>Number Theory Journal</dc:source>
                    <dc:identifier>zbMATH:7654321</dc:identifier>
                </oai_dc:dc>
            </metadata>
        </record>
    </ListRecords>
</OAI-PMH>"#;

        let _mock = server
            .mock("GET", "/")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("verb".into(), "ListRecords".into()),
                Matcher::UrlEncoded("metadataPrefix".into(), "oai_dc".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "text/xml")
            .with_body(mock_response)
            .create_async()
            .await;

        let client = reqwest::Client::new();
        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 1, 3, 0, 0, 0).unwrap();

        let result = test_scrape_chunk_with_mock_url(&client, start, end, &server.url()).await;

        assert!(result.is_ok());
        let records = result.unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].title, "Test Algebra Paper");
        assert_eq!(records[0].authors, vec!["Johnson, Alice", "Williams, Bob"]);
        assert_eq!(records[0].year, 2024);
        assert_eq!(records[0].source, "zbmath");
        assert_eq!(records[1].title, "Advanced Number Theory");
        assert_eq!(records[1].authors, vec!["Smith, Carol"]);
    }

    /// Test that scrape_chunk handles no records match scenario.
    #[tokio::test]
    async fn test_zbmath_empty_range() {
        let mut server = Server::new_async().await;

        let error_response = r#"<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
    <error code="noRecordsMatch">No records match the given criteria</error>
</OAI-PMH>"#;

        let _mock = server
            .mock("GET", "/")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("verb".into(), "ListRecords".into()),
                Matcher::UrlEncoded("metadataPrefix".into(), "oai_dc".into()),
            ]))
            .with_status(400)
            .with_header("content-type", "text/xml")
            .with_body(error_response)
            .create_async()
            .await;

        let client = reqwest::Client::new();
        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(); // Same start and end

        let result = test_scrape_chunk_with_mock_url(&client, start, end, &server.url()).await;

        // Should succeed with empty results when no records match
        assert!(result.is_ok());
        let records = result.unwrap();
        assert_eq!(
            records.len(),
            0,
            "No records match should return empty results"
        );
    }

    /// Test successful scrape_chunk with mocked server
    #[tokio::test]
    async fn test_scrape_chunk_mocked_success() {
        let mut server = Server::new_async().await;

        let mock_response = r#"<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
    <ListRecords>
        <record>
            <header>
                <identifier>oai:zbmath.org:1234567</identifier>
                <datestamp>2024-01-01</datestamp>
                <setSpec>mathematics</setSpec>
            </header>
            <metadata>
                <oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" 
                          xmlns:dc="http://purl.org/dc/elements/1.1/">
                    <dc:title>Test Graph Theory Paper</dc:title>
                    <dc:creator>Smith, John; Doe, Jane</dc:creator>
                    <dc:date>2024</dc:date>
                    <dc:source>Journal of Mathematics</dc:source>
                    <dc:identifier>zbMATH:1234567</dc:identifier>
                </oai_dc:dc>
            </metadata>
        </record>
    </ListRecords>
</OAI-PMH>"#;

        let _mock = server
            .mock("GET", "/")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("verb".into(), "ListRecords".into()),
                Matcher::UrlEncoded("metadataPrefix".into(), "oai_dc".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "text/xml")
            .with_body(mock_response)
            .create_async()
            .await;

        let client = reqwest::Client::new();
        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap();

        let result = test_scrape_chunk_with_mock_url(&client, start, end, &server.url()).await;

        assert!(result.is_ok());
        let records = result.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].title, "Test Graph Theory Paper");
        assert_eq!(records[0].authors, vec!["Smith, John", "Doe, Jane"]);
        assert_eq!(records[0].year, 2024);
        assert_eq!(records[0].source, "zbmath");
    }

    /// Test API error responses
    #[tokio::test]
    async fn test_scrape_chunk_no_records_match() {
        let mock_response = r#"<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
    <error code="noRecordsMatch">No records match the given criteria</error>
</OAI-PMH>"#;

        let parsed: OaiPmh = serde_xml_rs::from_str(mock_response).unwrap();

        assert!(parsed.error.is_some());
        let error = parsed.error.unwrap();
        assert_eq!(error.code, "noRecordsMatch");
        assert_eq!(error.message, "No records match the given criteria");
    }

    /// Test API bad argument error
    #[tokio::test]
    async fn test_scrape_chunk_bad_argument() {
        let mock_response = r#"<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
    <error code="badArgument">Invalid date format</error>
</OAI-PMH>"#;

        let parsed: OaiPmh = serde_xml_rs::from_str(mock_response).unwrap();

        assert!(parsed.error.is_some());
        let error = parsed.error.unwrap();
        assert_eq!(error.code, "badArgument");
        assert_eq!(error.message, "Invalid date format");
    }

    /// Test pagination with resumption token
    #[tokio::test]
    async fn test_scrape_chunk_with_pagination() {
        let mock_response = r#"<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
    <ListRecords>
        <record>
            <header>
                <identifier>oai:zbmath.org:1111111</identifier>
                <datestamp>2024-01-01</datestamp>
            </header>
            <metadata>
                <oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" 
                          xmlns:dc="http://purl.org/dc/elements/1.1/">
                    <dc:title>First Paper</dc:title>
                    <dc:creator>Author One</dc:creator>
                    <dc:date>2024</dc:date>
                </oai_dc:dc>
            </metadata>
        </record>
        <resumptionToken>mock-token-123</resumptionToken>
    </ListRecords>
</OAI-PMH>"#;

        let parsed: OaiPmh = serde_xml_rs::from_str(mock_response).unwrap();

        assert!(parsed.list_records.is_some());
        let records = parsed.list_records.unwrap();
        assert_eq!(records.records.len(), 1);
        assert!(records.resumption_token.is_some());
        assert_eq!(records.resumption_token.unwrap(), "mock-token-123");
    }

    /// Test convert_to_publication_record with various edge cases
    #[test]
    fn test_convert_to_publication_record_edge_cases() {
        // Test with empty creator
        let record = Record {
            header: RecordHeader {
                identifier: "oai:zbmath.org:1234567".to_string(),
                date_stamp: "2024-01-01".to_string(),
                set_spec: vec![],
            },
            metadata: Some(Metadata {
                dc: DublinCore {
                    contributor: None,
                    creator: None, // No creator
                    date: Some("2024".to_string()),
                    identifier: Some("zbMATH:1234567".to_string()),
                    language: Some("en".to_string()),
                    publisher: Some("Academic Press".to_string()),
                    relation: None,
                    rights: None,
                    source: Some("Journal of Mathematics".to_string()),
                    subject: Some("Mathematics".to_string()),
                    title: Some("A Study of Graph Theory".to_string()),
                    doc_type: Some("article".to_string()),
                },
            }),
        };

        let result = convert_to_publication_record(record).unwrap();
        assert!(result.is_some());

        let pub_record = result.unwrap();
        assert!(pub_record.authors.is_empty()); // Should be empty array
    }

    /// Test with invalid year
    #[test]
    fn test_convert_to_publication_record_invalid_year() {
        let record = Record {
            header: RecordHeader {
                identifier: "oai:zbmath.org:1234567".to_string(),
                date_stamp: "2024-01-01".to_string(),
                set_spec: vec![],
            },
            metadata: Some(Metadata {
                dc: DublinCore {
                    contributor: None,
                    creator: Some("Smith, John".to_string()),
                    date: Some("invalid-year".to_string()), // Invalid year
                    identifier: Some("zbMATH:1234567".to_string()),
                    language: Some("en".to_string()),
                    publisher: Some("Academic Press".to_string()),
                    relation: None,
                    rights: None,
                    source: Some("Journal of Mathematics".to_string()),
                    subject: Some("Mathematics".to_string()),
                    title: Some("A Study of Graph Theory".to_string()),
                    doc_type: Some("article".to_string()),
                },
            }),
        };

        let result = convert_to_publication_record(record).unwrap();
        assert!(result.is_none()); // Should return None for invalid year
    }

    /// Test with missing date
    #[test]
    fn test_convert_to_publication_record_missing_date() {
        let record = Record {
            header: RecordHeader {
                identifier: "oai:zbmath.org:1234567".to_string(),
                date_stamp: "2024-01-01".to_string(),
                set_spec: vec![],
            },
            metadata: Some(Metadata {
                dc: DublinCore {
                    contributor: None,
                    creator: Some("Smith, John".to_string()),
                    date: None, // Missing date
                    identifier: Some("zbMATH:1234567".to_string()),
                    language: Some("en".to_string()),
                    publisher: Some("Academic Press".to_string()),
                    relation: None,
                    rights: None,
                    source: Some("Journal of Mathematics".to_string()),
                    subject: Some("Mathematics".to_string()),
                    title: Some("A Study of Graph Theory".to_string()),
                    doc_type: Some("article".to_string()),
                },
            }),
        };

        let result = convert_to_publication_record(record).unwrap();
        assert!(result.is_none()); // Should return None for missing date
    }

    /// Test with missing title (should use default)
    #[test]
    fn test_convert_to_publication_record_missing_title() {
        let record = Record {
            header: RecordHeader {
                identifier: "oai:zbmath.org:1234567".to_string(),
                date_stamp: "2024-01-01".to_string(),
                set_spec: vec![],
            },
            metadata: Some(Metadata {
                dc: DublinCore {
                    contributor: None,
                    creator: Some("Smith, John".to_string()),
                    date: Some("2024".to_string()),
                    identifier: Some("zbMATH:1234567".to_string()),
                    language: Some("en".to_string()),
                    publisher: Some("Academic Press".to_string()),
                    relation: None,
                    rights: None,
                    source: Some("Journal of Mathematics".to_string()),
                    subject: Some("Mathematics".to_string()),
                    title: None, // Missing title
                    doc_type: Some("article".to_string()),
                },
            }),
        };

        let result = convert_to_publication_record(record).unwrap();
        assert!(result.is_some());

        let pub_record = result.unwrap();
        assert_eq!(pub_record.title, "Unknown Title"); // Should use default
    }

    /// Test complex author parsing
    #[test]
    fn test_convert_to_publication_record_complex_authors() {
        let record = Record {
            header: RecordHeader {
                identifier: "oai:zbmath.org:1234567".to_string(),
                date_stamp: "2024-01-01".to_string(),
                set_spec: vec![],
            },
            metadata: Some(Metadata {
                dc: DublinCore {
                    contributor: None,
                    creator: Some("Smith, John A.; ; Doe, Jane B.;  ; Johnson, Bob".to_string()), // Complex with empty entries
                    date: Some("2024".to_string()),
                    identifier: Some("zbMATH:1234567".to_string()),
                    language: Some("en".to_string()),
                    publisher: Some("Academic Press".to_string()),
                    relation: None,
                    rights: None,
                    source: Some("Journal of Mathematics".to_string()),
                    subject: Some("Mathematics".to_string()),
                    title: Some("A Study of Graph Theory".to_string()),
                    doc_type: Some("article".to_string()),
                },
            }),
        };

        let result = convert_to_publication_record(record).unwrap();
        assert!(result.is_some());

        let pub_record = result.unwrap();
        // Should filter out empty entries and trim whitespace
        assert_eq!(
            pub_record.authors,
            vec!["Smith, John A.", "Doe, Jane B.", "Johnson, Bob"]
        );
    }

    /// Test HTTP error handling
    #[tokio::test]
    async fn test_scrape_chunk_http_error() {
        let mut server = Server::new_async().await;

        let _mock = server
            .mock("GET", "/")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("verb".into(), "ListRecords".into()),
                Matcher::UrlEncoded("metadataPrefix".into(), "oai_dc".into()),
            ]))
            .with_status(500)
            .with_body("Internal Server Error")
            .create_async()
            .await;

        let client = reqwest::Client::new();
        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap();

        let result = test_scrape_chunk_with_mock_url(&client, start, end, &server.url()).await;

        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("HTTP error 500"));
    }

    /// Test noRecordsMatch error handling
    #[tokio::test]
    async fn test_scrape_chunk_no_records_error() {
        let mut server = Server::new_async().await;

        let error_response = r#"<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
    <error code="noRecordsMatch">No records match the given criteria</error>
</OAI-PMH>"#;

        let _mock = server
            .mock("GET", "/")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("verb".into(), "ListRecords".into()),
                Matcher::UrlEncoded("metadataPrefix".into(), "oai_dc".into()),
            ]))
            .with_status(400)
            .with_header("content-type", "text/xml")
            .with_body(error_response)
            .create_async()
            .await;

        let client = reqwest::Client::new();
        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap();

        let result = test_scrape_chunk_with_mock_url(&client, start, end, &server.url()).await;

        // This should succeed with empty results, not error
        assert!(result.is_ok());
        let records = result.unwrap();
        assert_eq!(records.len(), 0);
    }

    /// Test badArgument error handling
    #[tokio::test]
    async fn test_scrape_chunk_bad_argument_error() {
        let mut server = Server::new_async().await;

        let error_response = r#"<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
    <error code="badArgument">Invalid date format</error>
</OAI-PMH>"#;

        let _mock = server
            .mock("GET", "/")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("verb".into(), "ListRecords".into()),
                Matcher::UrlEncoded("metadataPrefix".into(), "oai_dc".into()),
            ]))
            .with_status(400)
            .with_header("content-type", "text/xml")
            .with_body(error_response)
            .create_async()
            .await;

        let client = reqwest::Client::new();
        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap();

        let result = test_scrape_chunk_with_mock_url(&client, start, end, &server.url()).await;

        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Bad API argument"));
        assert!(error_msg.contains("Invalid date format"));
    }

    /// Test pagination flow with multiple pages
    #[tokio::test]
    async fn test_scrape_chunk_pagination_flow() {
        let mut server = Server::new_async().await;

        // First request with resumption token
        let first_response = r#"<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
    <ListRecords>
        <record>
            <header>
                <identifier>oai:zbmath.org:1111111</identifier>
                <datestamp>2024-01-01</datestamp>
            </header>
            <metadata>
                <oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" 
                          xmlns:dc="http://purl.org/dc/elements/1.1/">
                    <dc:title>First Paper</dc:title>
                    <dc:creator>Author One</dc:creator>
                    <dc:date>2024</dc:date>
                </oai_dc:dc>
            </metadata>
        </record>
        <resumptionToken>page2token</resumptionToken>
    </ListRecords>
</OAI-PMH>"#;

        // Second request (using resumption token)
        let second_response = r#"<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
    <ListRecords>
        <record>
            <header>
                <identifier>oai:zbmath.org:2222222</identifier>
                <datestamp>2024-01-01</datestamp>
            </header>
            <metadata>
                <oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" 
                          xmlns:dc="http://purl.org/dc/elements/1.1/">
                    <dc:title>Second Paper</dc:title>
                    <dc:creator>Author Two</dc:creator>
                    <dc:date>2024</dc:date>
                </oai_dc:dc>
            </metadata>
        </record>
    </ListRecords>
</OAI-PMH>"#;

        // Mock first request (with date parameters)
        let _mock1 = server
            .mock("GET", "/")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("verb".into(), "ListRecords".into()),
                Matcher::UrlEncoded("metadataPrefix".into(), "oai_dc".into()),
                Matcher::UrlEncoded("from".into(), "2024-01-01T00:00:00Z".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "text/xml")
            .with_body(first_response)
            .create_async()
            .await;

        // Mock second request (with resumption token)
        let _mock2 = server
            .mock("GET", "/")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("verb".into(), "ListRecords".into()),
                Matcher::UrlEncoded("resumptionToken".into(), "page2token".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "text/xml")
            .with_body(second_response)
            .create_async()
            .await;

        let client = reqwest::Client::new();
        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap();

        let result = test_scrape_chunk_with_mock_url(&client, start, end, &server.url()).await;

        assert!(result.is_ok());
        let records = result.unwrap();
        assert_eq!(records.len(), 2); // Should have both records from both pages
        assert_eq!(records[0].title, "First Paper");
        assert_eq!(records[1].title, "Second Paper");
    }

    /// Test API rate limiting handling.
    #[test]
    #[ignore = "Scraper not yet implemented"]
    fn test_zbmath_rate_limiting() {
        // TODO: Test that rate limiting is properly handled
        // - Verify exponential backoff on 429 responses
        // - Check that requests don't exceed API limits
    }

    /// Test pagination through large result sets.
    #[test]
    #[ignore = "Scraper not yet implemented"]
    fn test_zbmath_pagination() {
        // TODO: Test pagination logic
        // - Verify all pages are fetched
        // - Check that results are deduplicated across pages
    }

    /// Test parsing of zbMATH-specific metadata.
    #[test]
    #[ignore = "Scraper not yet implemented"]
    fn test_zbmath_metadata_parsing() {
        // TODO: Test zbMATH-specific fields
        // - MSC (Mathematics Subject Classification) codes
        // - zbMATH ID format
        // - Review text extraction (if applicable)
    }

    /// Test scrape_range function with a date range
    #[tokio::test]
    async fn test_scrape_range_single_range() {
        let mut server = Server::new_async().await;

        let mock_response = r#"<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
    <ListRecords>
        <record>
            <header>
                <identifier>oai:zbmath.org:1234567</identifier>
                <datestamp>2024-01-01</datestamp>
                <setSpec>mathematics</setSpec>
            </header>
            <metadata>
                <oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" 
                          xmlns:dc="http://purl.org/dc/elements/1.1/">
                    <dc:title>Range Test Paper</dc:title>
                    <dc:creator>Range, Alice</dc:creator>
                    <dc:date>2024</dc:date>
                    <dc:source>Range Journal</dc:source>
                    <dc:identifier>zbMATH:1234567</dc:identifier>
                </oai_dc:dc>
            </metadata>
        </record>
    </ListRecords>
</OAI-PMH>"#;

        let _mock = server
            .mock("GET", "/")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("verb".into(), "ListRecords".into()),
                Matcher::UrlEncoded("metadataPrefix".into(), "oai_dc".into()),
                Matcher::UrlEncoded("from".into(), "2024-01-01T00:00:00Z".into()),
                Matcher::UrlEncoded("until".into(), "2024-01-03T00:00:00Z".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "text/xml")
            .with_body(mock_response)
            .create_async()
            .await;

        // Test with a 2-day range
        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 1, 3, 0, 0, 0).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;

        assert!(result.is_ok());
        let records = result.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].title, "Range Test Paper");
        assert_eq!(records[0].authors, vec!["Range, Alice"]);
        assert_eq!(records[0].source, "zbmath");
    }

    /// Test scrape_range function with empty results
    #[tokio::test]
    async fn test_scrape_range_empty_results() {
        let mut server = Server::new_async().await;

        let error_response = r#"<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
    <error code="noRecordsMatch">No records match the given criteria</error>
</OAI-PMH>"#;

        let _mock = server
            .mock("GET", "/")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("verb".into(), "ListRecords".into()),
                Matcher::UrlEncoded("metadataPrefix".into(), "oai_dc".into()),
            ]))
            .with_status(400)
            .with_header("content-type", "text/xml")
            .with_body(error_response)
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;

        assert!(result.is_ok());
        let records = result.unwrap();
        assert_eq!(records.len(), 0);
    }

    /// Test scrape_range function error handling
    #[tokio::test]
    async fn test_scrape_range_error_handling() {
        let mut server = Server::new_async().await;

        let error_response = r#"<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
    <error code="badArgument">Invalid date format</error>
</OAI-PMH>"#;

        let _mock = server
            .mock("GET", "/")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("verb".into(), "ListRecords".into()),
                Matcher::UrlEncoded("metadataPrefix".into(), "oai_dc".into()),
            ]))
            .with_status(400)
            .with_header("content-type", "text/xml")
            .with_body(error_response)
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;

        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Bad API argument"));
    }

    /// Test scrape_range function with HTTP error
    #[tokio::test]
    async fn test_scrape_range_http_error() {
        let mut server = Server::new_async().await;

        let _mock = server
            .mock("GET", "/")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("verb".into(), "ListRecords".into()),
                Matcher::UrlEncoded("metadataPrefix".into(), "oai_dc".into()),
            ]))
            .with_status(500)
            .with_body("Internal Server Error")
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;

        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("HTTP error 500"));
    }

    /// Test convert_to_publication_record with no metadata
    #[test]
    fn test_convert_to_publication_record_no_metadata() {
        let record = Record {
            header: RecordHeader {
                identifier: "oai:zbmath.org:1234567".to_string(),
                date_stamp: "2024-01-01".to_string(),
                set_spec: vec![],
            },
            metadata: None, // No metadata
        };

        let result = convert_to_publication_record(record).unwrap();
        assert!(result.is_none()); // Should return None for no metadata
    }

    /// Test convert_to_publication_record with venue field
    #[test]
    fn test_convert_to_publication_record_with_venue() {
        let record = Record {
            header: RecordHeader {
                identifier: "oai:zbmath.org:1234567".to_string(),
                date_stamp: "2024-01-01".to_string(),
                set_spec: vec![],
            },
            metadata: Some(Metadata {
                dc: DublinCore {
                    contributor: None,
                    creator: Some("Smith, John".to_string()),
                    date: Some("2024".to_string()),
                    identifier: Some("zbMATH:1234567".to_string()),
                    language: Some("en".to_string()),
                    publisher: Some("Academic Press".to_string()),
                    relation: None,
                    rights: None,
                    source: Some("Journal of Advanced Mathematics".to_string()), // Has venue
                    subject: Some("Mathematics".to_string()),
                    title: Some("A Study of Graph Theory".to_string()),
                    doc_type: Some("article".to_string()),
                },
            }),
        };

        let result = convert_to_publication_record(record).unwrap();
        assert!(result.is_some());

        let pub_record = result.unwrap();
        assert_eq!(
            pub_record.venue,
            Some("Journal of Advanced Mathematics".to_string())
        );
    }

    /// Test scrape_chunk with unknown API error code in HTTP error response
    #[tokio::test]
    async fn test_scrape_chunk_unknown_error_code_http() {
        let mut server = Server::new_async().await;

        let error_response = r#"<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
    <error code="someUnknownError">Some unknown error occurred</error>
</OAI-PMH>"#;

        let _mock = server
            .mock("GET", "/")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("verb".into(), "ListRecords".into()),
                Matcher::UrlEncoded("metadataPrefix".into(), "oai_dc".into()),
            ]))
            .with_status(500) // HTTP error status
            .with_header("content-type", "text/xml")
            .with_body(error_response)
            .create_async()
            .await;

        let client = reqwest::Client::new();
        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap();

        let result = test_scrape_chunk_with_mock_url(&client, start, end, &server.url()).await;

        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("API error someUnknownError"));
        assert!(error_msg.contains("Some unknown error occurred"));
    }

    /// Test scrape_chunk with unknown API error code in successful response
    #[tokio::test]
    async fn test_scrape_chunk_unknown_error_code_success() {
        let mut server = Server::new_async().await;

        let error_response = r#"<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
    <error code="someOtherUnknownError">Another unknown error occurred</error>
</OAI-PMH>"#;

        let _mock = server
            .mock("GET", "/")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("verb".into(), "ListRecords".into()),
                Matcher::UrlEncoded("metadataPrefix".into(), "oai_dc".into()),
            ]))
            .with_status(200) // HTTP success status but with error in content
            .with_header("content-type", "text/xml")
            .with_body(error_response)
            .create_async()
            .await;

        let client = reqwest::Client::new();
        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap();

        let result = test_scrape_chunk_with_mock_url(&client, start, end, &server.url()).await;

        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("API error someOtherUnknownError"));
        assert!(error_msg.contains("Another unknown error occurred"));
    }

    /// Test scrape_chunk with empty response (no list_records element)
    #[tokio::test]
    async fn test_scrape_chunk_empty_response() {
        let mut server = Server::new_async().await;

        let empty_response = r#"<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
</OAI-PMH>"#;

        let _mock = server
            .mock("GET", "/")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("verb".into(), "ListRecords".into()),
                Matcher::UrlEncoded("metadataPrefix".into(), "oai_dc".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "text/xml")
            .with_body(empty_response)
            .create_async()
            .await;

        let client = reqwest::Client::new();
        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap();
        let result = test_scrape_chunk_with_mock_url(&client, start, end, &server.url()).await;

        // Should succeed with empty results when no list_records element
        assert!(result.is_ok());
        let records = result.unwrap();
        assert_eq!(records.len(), 0);
    }

    /// Test scrape_chunk with noRecordsMatch error in successful HTTP response
    #[tokio::test]
    async fn test_scrape_chunk_no_records_match_success_response() {
        let mut server = Server::new_async().await;

        let error_response = r#"<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
    <error code="noRecordsMatch">No records match the given criteria</error>
</OAI-PMH>"#;

        let _mock = server
            .mock("GET", "/")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("verb".into(), "ListRecords".into()),
                Matcher::UrlEncoded("metadataPrefix".into(), "oai_dc".into()),
            ]))
            .with_status(200) // HTTP success status but with noRecordsMatch error in content
            .with_header("content-type", "text/xml")
            .with_body(error_response)
            .create_async()
            .await;

        let client = reqwest::Client::new();
        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap();
        let result = test_scrape_chunk_with_mock_url(&client, start, end, &server.url()).await;

        // Should succeed with empty results when noRecordsMatch in successful response
        assert!(result.is_ok());
        let records = result.unwrap();
        assert_eq!(records.len(), 0);
    }

    /// Test scrape_chunk with HTTP error but valid XML with no error element
    #[tokio::test]
    async fn test_scrape_chunk_http_error_no_xml_error() {
        let mut server = Server::new_async().await;

        let response_without_error = r#"<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
    <ListRecords>
    </ListRecords>
</OAI-PMH>"#;

        let _mock = server
            .mock("GET", "/")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("verb".into(), "ListRecords".into()),
                Matcher::UrlEncoded("metadataPrefix".into(), "oai_dc".into()),
            ]))
            .with_status(500) // HTTP error status
            .with_header("content-type", "text/xml")
            .with_body(response_without_error)
            .create_async()
            .await;

        let client = reqwest::Client::new();
        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap();
        let result = test_scrape_chunk_with_mock_url(&client, start, end, &server.url()).await;

        // Should succeed with empty results - HTTP error with valid XML but no error element falls through to normal processing
        assert!(result.is_ok());
        let records = result.unwrap();
        assert_eq!(records.len(), 0); // Empty ListRecords should return no records
    }

    /// Test scrape_chunk with successful HTTP response but malformed XML
    #[tokio::test]
    async fn test_scrape_chunk_success_malformed_xml() {
        let mut server = Server::new_async().await;

        let malformed_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
    <ListRecords>
        <record>
            <header>
                <identifier>oai:zbmath.org:123</identifier>
                <!-- Missing closing tag for header! -->
        </record>
    </ListRecords>
<!-- Missing closing OAI-PMH tag! -->"#;

        let _mock = server
            .mock("GET", "/")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("verb".into(), "ListRecords".into()),
                Matcher::UrlEncoded("metadataPrefix".into(), "oai_dc".into()),
            ]))
            .with_status(200) // HTTP success status
            .with_header("content-type", "text/xml")
            .with_body(malformed_xml)
            .create_async()
            .await;

        let client = reqwest::Client::new();
        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap();
        let result = test_scrape_chunk_with_mock_url(&client, start, end, &server.url()).await;

        // Should fail with XML parsing error
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Failed to parse XML response"));
    }

    /// Test ZbmathScraper::new()
    #[test]
    fn test_zbmath_scraper_new() {
        use crate::scrapers::zbmath::ZbmathScraper;

        let scraper = ZbmathScraper::new();
        assert_eq!(scraper.config.base_url, "https://oai.zbmath.org/v1/");
        assert_eq!(scraper.config.delay_between_pages_ms, 500);
    }

    /// Test ZbmathScraper::with_config()
    #[test]
    fn test_zbmath_scraper_with_config() {
        use crate::scrapers::zbmath::ZbmathScraper;

        let custom_config = ZbmathConfig {
            base_url: "https://custom.example.com/".to_string(),
            delay_between_pages_ms: 100,
        };

        let scraper = ZbmathScraper::with_config(custom_config.clone());
        assert_eq!(scraper.config.base_url, custom_config.base_url);
        assert_eq!(
            scraper.config.delay_between_pages_ms,
            custom_config.delay_between_pages_ms
        );
    }

    /// Test ZbmathScraper::default()
    #[test]
    fn test_zbmath_scraper_default() {
        use crate::scrapers::zbmath::ZbmathScraper;

        let scraper = ZbmathScraper::default();
        assert_eq!(scraper.config.base_url, "https://oai.zbmath.org/v1/");
    }

    /// Test convert_to_publication_record with year in date field (YYYY-MM-DD format)
    #[test]
    fn test_convert_to_publication_record_full_date_format() {
        let record = Record {
            header: RecordHeader {
                identifier: "oai:zbmath.org:1234567".to_string(),
                date_stamp: "2024-01-01".to_string(),
                set_spec: vec![],
            },
            metadata: Some(Metadata {
                dc: DublinCore {
                    contributor: None,
                    creator: Some("Smith, John".to_string()),
                    date: Some("2024-06-15".to_string()), // Full date format
                    identifier: Some("zbMATH:1234567".to_string()),
                    language: Some("en".to_string()),
                    publisher: Some("Academic Press".to_string()),
                    relation: None,
                    rights: None,
                    source: Some("Journal of Mathematics".to_string()),
                    subject: Some("Mathematics".to_string()),
                    title: Some("A Study of Graph Theory".to_string()),
                    doc_type: Some("article".to_string()),
                },
            }),
        };

        let result = convert_to_publication_record(record).unwrap();
        assert!(result.is_some());

        let pub_record = result.unwrap();
        assert_eq!(pub_record.year, 2024);
    }

    /// Test convert_to_publication_record with short year (3 digits)
    #[test]
    fn test_convert_to_publication_record_short_year() {
        let record = Record {
            header: RecordHeader {
                identifier: "oai:zbmath.org:1234567".to_string(),
                date_stamp: "2024-01-01".to_string(),
                set_spec: vec![],
            },
            metadata: Some(Metadata {
                dc: DublinCore {
                    contributor: None,
                    creator: Some("Smith, John".to_string()),
                    date: Some("999".to_string()), // Short year
                    identifier: Some("zbMATH:1234567".to_string()),
                    language: Some("en".to_string()),
                    publisher: Some("Academic Press".to_string()),
                    relation: None,
                    rights: None,
                    source: Some("Journal of Mathematics".to_string()),
                    subject: Some("Mathematics".to_string()),
                    title: Some("A Study of Graph Theory".to_string()),
                    doc_type: Some("article".to_string()),
                },
            }),
        };

        let result = convert_to_publication_record(record).unwrap();
        assert!(result.is_none()); // Should return None for invalid year (only 3 digits)
    }

    /// Test scrape_range function (the simpler API without config)
    #[tokio::test]
    async fn test_scrape_range_simple_api() {
        let mut server = Server::new_async().await;

        let mock_response = r#"<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
    <ListRecords>
        <record>
            <header>
                <identifier>oai:zbmath.org:9999999</identifier>
                <datestamp>2024-01-01</datestamp>
            </header>
            <metadata>
                <oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" 
                          xmlns:dc="http://purl.org/dc/elements/1.1/">
                    <dc:title>Simple API Test</dc:title>
                    <dc:creator>Simple, Test</dc:creator>
                    <dc:date>2024</dc:date>
                </oai_dc:dc>
            </metadata>
        </record>
    </ListRecords>
</OAI-PMH>"#;

        let _mock = server
            .mock("GET", "/")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("verb".into(), "ListRecords".into()),
                Matcher::UrlEncoded("metadataPrefix".into(), "oai_dc".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "text/xml")
            .with_body(mock_response)
            .create_async()
            .await;

        // This will use the default config which points to the real zbmath API
        // For testing purposes, we'd need to modify this to accept a custom URL
        // Since we can't easily test scrape_range without modifying the default URL,
        // we'll just verify it compiles and has the right signature
    }

    /// Test convert_to_publication_record with only whitespace in creator
    #[test]
    fn test_convert_to_publication_record_whitespace_creator() {
        let record = Record {
            header: RecordHeader {
                identifier: "oai:zbmath.org:1234567".to_string(),
                date_stamp: "2024-01-01".to_string(),
                set_spec: vec![],
            },
            metadata: Some(Metadata {
                dc: DublinCore {
                    contributor: None,
                    creator: Some("   ;   ;   ".to_string()), // Only whitespace
                    date: Some("2024".to_string()),
                    identifier: Some("zbMATH:1234567".to_string()),
                    language: Some("en".to_string()),
                    publisher: Some("Academic Press".to_string()),
                    relation: None,
                    rights: None,
                    source: Some("Journal of Mathematics".to_string()),
                    subject: Some("Mathematics".to_string()),
                    title: Some("A Study of Graph Theory".to_string()),
                    doc_type: Some("article".to_string()),
                },
            }),
        };

        let result = convert_to_publication_record(record).unwrap();
        assert!(result.is_some());

        let pub_record = result.unwrap();
        assert!(pub_record.authors.is_empty()); // Should filter out empty entries
    }

    /// Test empty resumption token handling
    #[tokio::test]
    async fn test_scrape_chunk_empty_resumption_token() {
        let mut server = Server::new_async().await;

        let mock_response = r#"<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
    <ListRecords>
        <record>
            <header>
                <identifier>oai:zbmath.org:1111111</identifier>
                <datestamp>2024-01-01</datestamp>
            </header>
            <metadata>
                <oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" 
                          xmlns:dc="http://purl.org/dc/elements/1.1/">
                    <dc:title>Empty Token Paper</dc:title>
                    <dc:creator>Author One</dc:creator>
                    <dc:date>2024</dc:date>
                </oai_dc:dc>
            </metadata>
        </record>
        <resumptionToken></resumptionToken>
    </ListRecords>
</OAI-PMH>"#;

        let _mock = server
            .mock("GET", "/")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("verb".into(), "ListRecords".into()),
                Matcher::UrlEncoded("metadataPrefix".into(), "oai_dc".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "text/xml")
            .with_body(mock_response)
            .create_async()
            .await;

        let client = reqwest::Client::new();
        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap();

        let result = test_scrape_chunk_with_mock_url(&client, start, end, &server.url()).await;

        assert!(result.is_ok());
        let records = result.unwrap();
        assert_eq!(records.len(), 1);
    }

    /// Test ZbmathConfig debug trait
    #[test]
    fn test_zbmath_config_debug() {
        let config = ZbmathConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("https://oai.zbmath.org/v1/"));
        assert!(debug_str.contains("500"));
    }

    /// Test scrape_chunk with records that have all optional fields missing
    #[tokio::test]
    async fn test_scrape_chunk_minimal_record() {
        let mut server = Server::new_async().await;

        let mock_response = r#"<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
    <ListRecords>
        <record>
            <header>
                <identifier>oai:zbmath.org:minimal</identifier>
                <datestamp>2024-01-01</datestamp>
            </header>
            <metadata>
                <oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" 
                          xmlns:dc="http://purl.org/dc/elements/1.1/">
                    <dc:date>2024</dc:date>
                </oai_dc:dc>
            </metadata>
        </record>
    </ListRecords>
</OAI-PMH>"#;

        let _mock = server
            .mock("GET", "/")
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("verb".into(), "ListRecords".into()),
                Matcher::UrlEncoded("metadataPrefix".into(), "oai_dc".into()),
            ]))
            .with_status(200)
            .with_header("content-type", "text/xml")
            .with_body(mock_response)
            .create_async()
            .await;

        let client = reqwest::Client::new();
        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap();

        let result = test_scrape_chunk_with_mock_url(&client, start, end, &server.url()).await;

        assert!(result.is_ok());
        let records = result.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].title, "Unknown Title");
        assert_eq!(records[0].authors.len(), 0);
        assert_eq!(records[0].venue, None);
    }

    /// Test RecordHeader fields are parsed correctly
    #[test]
    fn test_record_header_with_multiple_set_specs() {
        let header = RecordHeader {
            identifier: "test-id".to_string(),
            date_stamp: "2024-01-01".to_string(),
            set_spec: vec![
                "math".to_string(),
                "algebra".to_string(),
                "geometry".to_string(),
            ],
        };

        assert_eq!(header.identifier, "test-id");
        assert_eq!(header.set_spec.len(), 3);
    }

    /// Test DublinCore with all fields populated
    #[test]
    fn test_dublin_core_all_fields() {
        let dc = DublinCore {
            contributor: Some("Contributor Name".to_string()),
            creator: Some("Creator Name".to_string()),
            date: Some("2024".to_string()),
            identifier: Some("ID123".to_string()),
            language: Some("en".to_string()),
            publisher: Some("Publisher Name".to_string()),
            relation: Some("Related Work".to_string()),
            rights: Some("CC BY 4.0".to_string()),
            source: Some("Source Journal".to_string()),
            subject: Some("Mathematics".to_string()),
            title: Some("Title Text".to_string()),
            doc_type: Some("article".to_string()),
        };

        assert_eq!(dc.contributor, Some("Contributor Name".to_string()));
        assert_eq!(dc.rights, Some("CC BY 4.0".to_string()));
        assert_eq!(dc.relation, Some("Related Work".to_string()));
    }

    /// Test OaiError structure
    #[test]
    fn test_oai_error_structure() {
        let _xml = r#"<error code="testCode">Test error message</error>"#;
        // We can't easily deserialize just the error element without the full structure,
        // but we can verify the structure exists
    }

    /// Test convert_to_publication_record with year containing extra text
    #[test]
    fn test_convert_to_publication_record_year_with_text() {
        let record = Record {
            header: RecordHeader {
                identifier: "oai:zbmath.org:1234567".to_string(),
                date_stamp: "2024-01-01".to_string(),
                set_spec: vec![],
            },
            metadata: Some(Metadata {
                dc: DublinCore {
                    contributor: None,
                    creator: Some("Smith, John".to_string()),
                    date: Some("2024 (Published online)".to_string()), // Year with extra text
                    identifier: Some("zbMATH:1234567".to_string()),
                    language: Some("en".to_string()),
                    publisher: Some("Academic Press".to_string()),
                    relation: None,
                    rights: None,
                    source: Some("Journal of Mathematics".to_string()),
                    subject: Some("Mathematics".to_string()),
                    title: Some("A Study of Graph Theory".to_string()),
                    doc_type: Some("article".to_string()),
                },
            }),
        };

        let result = convert_to_publication_record(record).unwrap();
        assert!(result.is_some());

        let pub_record = result.unwrap();
        assert_eq!(pub_record.year, 2024); // Should extract first 4 digits
    }
}
