#[cfg(test)]
mod tests {
    use crate::config::DblpSourceConfig;
    use crate::db::ingestion::PublicationRecord;
    use crate::scrapers::dblp;
    use chrono::{TimeZone, Utc};
    use mockito::Server;
    use serde_json::json;

    /// Helper function to create DBLP JSON response
    fn create_dblp_json_response(records: &[serde_json::Value]) -> String {
        let response = json!({
            "result": {
                "hits": {
                    "hit": records,
                    "sent": records.len(),
                    "total": records.len()
                }
            }
        });
        response.to_string()
    }

    /// Helper function to test scrape_range with mock URL
    async fn test_scrape_range_with_mock_url(
        start: chrono::DateTime<Utc>,
        end: chrono::DateTime<Utc>,
        mock_url: &str,
    ) -> Result<Vec<PublicationRecord>, Box<dyn std::error::Error>> {
        let config = DblpSourceConfig {
            base_url: mock_url.to_string(),
            page_size: 100,
            delay_ms: 0, // No delay for tests
            enable_cache: false,
        };
        dblp::scrape_range_with_config(start, end, config).await
    }

    #[tokio::test]
    async fn test_dblp_scrape_range_basic() {
        let mut server = Server::new_async().await;

        let record = json!({
            "info": {
                "title": "Test Article",
                "authors": {
                    "author": ["John Smith"]
                },
                "year": "2023",
                "venue": "Journal of the ACM",
                "key": "journals/jacm/Smith23"
            }
        });

        let response_body = create_dblp_json_response(&[record]);

        // Match query containing year:2023
        let _mock = server
            .mock("GET", "/")
            .match_query(mockito::Matcher::Regex("q=year(:|%3A)2023.*".into()))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(response_body)
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

    #[tokio::test]
    async fn test_dblp_empty_range() {
        let server = Server::new_async().await;
        // Should produce no requests if range is empty/invalid
        let start = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
        let end = start; // Empty range

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;

        assert!(result.is_ok());
        let records = result.unwrap();
        assert_eq!(records.len(), 0);
    }

    #[tokio::test]
    async fn test_dblp_missing_fields() {
        let mut server = Server::new_async().await;

        let record_missing_title = json!({
            "info": {
                "authors": {"author": ["A"]},
                "year": "2023",
                "key": "k1"
            }
        });

        let record_missing_authors = json!({
            "info": {
                "title": "T",
                "year": "2023",
                "key": "k2"
            }
        });

        let response_body =
            create_dblp_json_response(&[record_missing_title, record_missing_authors]);

        // Match query containing year:2023
        let _mock = server
            .mock("GET", "/")
            .match_query(mockito::Matcher::Regex("q=year(:|%3A)2023.*".into()))
            .with_status(200)
            .with_body(response_body)
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2023, 12, 31, 23, 59, 59).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;
        assert!(result.is_ok());
        let records = result.unwrap();
        // Both should be filtered out
        assert_eq!(records.len(), 0);
    }

    #[tokio::test]
    async fn test_dblp_pagination() {
        let mut server = Server::new_async().await;

        let record1 = json!({
            "info": {
                "title": "P1",
                "authors": {"author": ["A1"]},
                "year": "2023",
                "key": "k1"
            }
        });

        let record2 = json!({
            "info": {
                "title": "P2",
                "authors": {"author": ["A2"]},
                "year": "2023",
                "key": "k2"
            }
        });

        // Page 1: total 2, sent 1, hit P1
        let page1 = json!({
            "result": {
                "hits": {
                    "hit": [record1],
                    "sent": 1,
                    "total": 2
                }
            }
        })
        .to_string();

        // Page 2: total 2, sent 1, hit P2
        let page2 = json!({
            "result": {
                "hits": {
                    "hit": [record2],
                    "sent": 1,
                    "total": 2
                }
            }
        })
        .to_string();

        // Using f=0 for first page
        let _m1 = server
            .mock("GET", "/")
            .match_query(mockito::Matcher::Regex("f=0".into()))
            .with_status(200)
            .with_body(page1)
            .create_async()
            .await;

        // Using f=1 for second page (since page_size is likely > 1, but logic uses hit len)
        // Wait, logic: first += hits_len.
        // If hits_len is 1, next req has f=1.
        let _m2 = server
            .mock("GET", "/")
            .match_query(mockito::Matcher::Regex("f=1".into()))
            .with_status(200)
            .with_body(page2)
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2023, 12, 31, 23, 59, 59).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;
        assert!(result.is_ok());
        let records = result.unwrap();
        assert_eq!(records.len(), 2);
    }

    #[tokio::test]
    async fn test_author_structure_variations() {
        let mut server = Server::new_async().await;

        // Author as string
        let rec1 = json!({
            "info": {
                "title": "T1",
                "authors": {"author": ["Name1"]},
                "year": "2023",
                "key": "k1"
            }
        });

        // Author as struct
        let rec2 = json!({
            "info": {
                "title": "T2",
                "authors": {"author": [{"text": "Name2", "pid": "p2"}]},
                "year": "2023",
                "key": "k2"
            }
        });

        let response = create_dblp_json_response(&[rec1, rec2]);

        // Match query containing year:2023
        let _mock = server
            .mock("GET", "/")
            .match_query(mockito::Matcher::Regex("q=year(:|%3A)2023.*".into()))
            .with_status(200)
            .with_body(response)
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2023, 12, 31, 23, 59, 59).unwrap();

        let records = test_scrape_range_with_mock_url(start, end, &server.url())
            .await
            .unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].authors[0], "Name1");
        assert_eq!(records[1].authors[0], "Name2");
    }

    #[tokio::test]
    async fn test_dblp_optimization_skips_historic_year_without_jan1() {
        let server = Server::new_async().await;

        // Config: use mock server
        let config = DblpSourceConfig {
            base_url: server.url(),
            page_size: 100,
            delay_ms: 0,
            enable_cache: false,
        };

        // Date setup
        // Current date in env is Jan 2026.
        // Historic year: 2020.
        // Range: Feb 1 2020 to Mar 1 2020. Does NOT include Jan 1.
        let start = Utc.with_ymd_and_hms(2020, 2, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2020, 3, 1, 0, 0, 0).unwrap();

        // If the optimization works, NO request should be made to the server.
        let result = dblp::scrape_range_with_config(start, end, config).await;

        assert!(result.is_ok());
        let records = result.unwrap();
        assert_eq!(records.len(), 0);
    }

    #[tokio::test]
    async fn test_dblp_optimization_includes_historic_year_with_jan1() {
        let mut server = Server::new_async().await;

        let config = DblpSourceConfig {
            base_url: server.url(),
            page_size: 100,
            delay_ms: 0,
            enable_cache: false,
        };

        // Historic year: 2020.
        // Range: Dec 31 2019 to Jan 2 2020. Includes Jan 1 2020.
        let start = Utc.with_ymd_and_hms(2019, 12, 31, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2020, 1, 2, 0, 0, 0).unwrap();

        // Expect request for 2020
        let _m2020 = server
            .mock("GET", "/")
            .match_query(mockito::Matcher::Regex("q=year(:|%3A)2020.*".into()))
            .with_status(200)
            .with_body(r#"{"result":{"hits":{"hit":[],"sent":0,"total":0}}}"#)
            .create_async()
            .await;

        let result = dblp::scrape_range_with_config(start, end, config).await;

        assert!(result.is_ok());
        _m2020.assert();
    }
}
