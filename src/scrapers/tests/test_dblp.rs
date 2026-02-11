#[cfg(test)]
mod tests {
    use crate::config::DblpSourceConfig;
    use crate::db::ingestion::PublicationRecord;
    use crate::scrapers::dblp;
    use crate::utilities::thread_safe_queue::{QueueConfig, ThreadSafeQueue}; // Added imports
    use chrono::{Datelike, TimeZone, Utc};
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
            delay_ms: 0,
            enable_cache: false,
            cache_dir: ".dblp_cache".to_string(),
            ..Default::default()
        };
        let queue = ThreadSafeQueue::new(QueueConfig::default());
        let producer = queue.create_producer();
        dblp::scrape_range_with_config(start, end, config, producer).await?;

        let mut results = Vec::new();
        while let Some(r) = queue.dequeue() {
            results.push(r);
        }
        Ok(results)
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
        // With multi-query strategy, each year makes 85 queries (26 conf + 26 journal + 26 book + 7 other)
        // Each query returns the same mock record, so we get 85 duplicates
        assert_eq!(records.len(), 85);
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
        // With multi-query strategy: 85 queries × 2 records per query = 170 records
        assert_eq!(records.len(), 170);
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
        // Multi-query strategy: 85 queries × 2 records = 170 total
        assert_eq!(records.len(), 170);
        assert_eq!(records[0].authors[0], "Name1");
        assert_eq!(records[85].authors[0], "Name2");
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
            cache_dir: ".dblp_cache".to_string(),
            ..Default::default()
        };

        // Date setup
        // Current date in env is Jan 2026.
        // Historic year: 2020.
        // Range: Feb 1 2020 to Mar 1 2020. Does NOT include Jan 1.
        let start = Utc.with_ymd_and_hms(2020, 2, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2020, 3, 1, 0, 0, 0).unwrap();

        // If the optimization works, NO request should be made to the server.
        let queue = ThreadSafeQueue::new(QueueConfig::default());
        let producer = queue.create_producer();
        let result = dblp::scrape_range_with_config(start, end, config, producer).await;
        // Drain queue just in case (though expect empty)
        let mut records = Vec::new();
        while let Some(r) = queue.dequeue() {
            records.push(r);
        }
        assert!(result.is_ok());
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
            cache_dir: ".dblp_cache".to_string(),
            ..Default::default()
        };

        // Historic year: 2020.
        // Range: Dec 31 2019 to Jan 2 2020. Includes Jan 1 2020.
        let start = Utc.with_ymd_and_hms(2019, 12, 31, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2020, 1, 2, 0, 0, 0).unwrap();

        // Expect request for 2020 (multi-query strategy: 85 queries per year)
        let _m2020 = server
            .mock("GET", "/")
            .match_query(mockito::Matcher::Regex("q=year(:|%3A)2020.*".into()))
            .with_status(200)
            .with_body(r#"{"result":{"hits":{"hit":[],"sent":0,"total":0}}}"#)
            .expect(85)
            .create_async()
            .await;

        let queue = ThreadSafeQueue::new(QueueConfig::default());
        let producer = queue.create_producer();
        let result = dblp::scrape_range_with_config(start, end, config, producer).await;

        assert!(result.is_ok());
        _m2020.assert();
    }

    #[tokio::test]
    async fn test_dblp_scraper_new() {
        use crate::scrapers::dblp::DblpScraper;
        let scraper = DblpScraper::new();
        // Verify it was created successfully
        assert!(
            format!("{:?}", scraper).contains("DblpScraper"),
            "Should create a DblpScraper"
        );
    }

    #[tokio::test]
    async fn test_dblp_scraper_default() {
        use crate::scrapers::dblp::DblpScraper;
        let scraper = DblpScraper::default();
        assert!(
            format!("{:?}", scraper).contains("DblpScraper"),
            "Should create a DblpScraper via default"
        );
    }

    #[tokio::test]
    async fn test_dblp_scraper_with_config() {
        use crate::scrapers::dblp::DblpScraper;
        let config = DblpSourceConfig {
            base_url: "https://test.example.com".to_string(),
            page_size: 50,
            delay_ms: 10,
            enable_cache: true,
            cache_dir: ".dblp_cache".to_string(),
            ..Default::default()
        };
        let scraper = DblpScraper::with_config(config);
        assert!(
            format!("{:?}", scraper).contains("DblpScraper"),
            "Should create a DblpScraper with custom config"
        );
    }

    #[tokio::test]
    async fn test_dblp_scraper_trait_scrape_range() {
        use crate::scrapers::dblp::DblpScraper;
        use crate::scrapers::scraper::Scraper;

        let mut server = Server::new_async().await;

        let record = json!({
            "info": {
                "title": "Trait Test",
                "authors": {"author": ["Test Author"]},
                "year": "2023",
                "key": "test/key"
            }
        });

        let response_body = create_dblp_json_response(&[record]);

        let _mock = server
            .mock("GET", "/")
            .match_query(mockito::Matcher::Regex("q=year(:|%3A)2023.*".into()))
            .with_status(200)
            .with_body(response_body)
            .create_async()
            .await;

        let config = DblpSourceConfig {
            base_url: server.url(),
            page_size: 100,
            delay_ms: 0,
            enable_cache: false,
            cache_dir: ".dblp_cache".to_string(),
            ..Default::default()
        };

        let scraper = DblpScraper::with_config(config);
        let start = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2023, 12, 31, 23, 59, 59).unwrap();

        let queue = ThreadSafeQueue::new(QueueConfig::default());
        let producer = queue.create_producer();
        let result = scraper.scrape_range(start, end, producer).await;

        let mut records = Vec::new();
        while let Some(r) = queue.dequeue() {
            records.push(r);
        }
        assert!(result.is_ok());
        // let records = result.unwrap(); // Removed shadowing
        // Multi-query strategy: 85 queries per year
        assert_eq!(records.len(), 85);
    }

    #[tokio::test]
    async fn test_dblp_scrape_range_function() {
        let mut server = Server::new_async().await;

        let record = json!({
            "info": {
                "title": "Function Test",
                "authors": {"author": ["Func Author"]},
                "year": "2022",
                "key": "func/key"
            }
        });

        let response_body = create_dblp_json_response(&[record]);

        let _mock = server
            .mock("GET", "/")
            .match_query(mockito::Matcher::Regex("q=year(:|%3A)2022.*".into()))
            .with_status(200)
            .with_body(response_body)
            .create_async()
            .await;

        // Override default config to use mock server
        let config = DblpSourceConfig {
            base_url: server.url(),
            page_size: 100,
            delay_ms: 0,
            enable_cache: false,
            cache_dir: ".dblp_cache".to_string(),
            ..Default::default()
        };

        let start = Utc.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2022, 12, 31, 23, 59, 59).unwrap();

        let queue = ThreadSafeQueue::new(QueueConfig::default());
        let producer = queue.create_producer();
        let result = dblp::scrape_range_with_config(start, end, config, producer).await;

        let mut records = Vec::new();
        while let Some(r) = queue.dequeue() {
            records.push(r);
        }
        assert!(result.is_ok());
        // let records = result.unwrap(); // Removed shadowing
        // Multi-query strategy: 85 queries per year
        assert_eq!(records.len(), 85);
    }

    #[tokio::test]
    async fn test_dblp_http_error_handling() {
        let mut server = Server::new_async().await;

        // Return HTTP 500 error
        let _mock = server
            .mock("GET", "/")
            .match_query(mockito::Matcher::Regex("q=year(:|%3A)2023.*".into()))
            .with_status(500)
            .with_body("Internal Server Error")
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2023, 12, 31, 23, 59, 59).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;

        // Should return Ok but with empty results due to error handling
        assert!(result.is_ok());
        let records = result.unwrap();
        assert_eq!(records.len(), 0);
    }

    #[tokio::test]
    async fn test_dblp_malformed_json_handling() {
        let mut server = Server::new_async().await;

        // Return malformed JSON
        let _mock = server
            .mock("GET", "/")
            .match_query(mockito::Matcher::Regex("q=year(:|%3A)2023.*".into()))
            .with_status(200)
            .with_body("{this is not valid json")
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2023, 12, 31, 23, 59, 59).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;

        // Should return Ok but with empty results due to error handling
        assert!(result.is_ok());
        let records = result.unwrap();
        assert_eq!(records.len(), 0);
    }

    #[tokio::test]
    async fn test_dblp_total_as_string() {
        let mut server = Server::new_async().await;

        let record = json!({
            "info": {
                "title": "String Total Test",
                "authors": {"author": ["Author"]},
                "year": "2023",
                "key": "test/key"
            }
        });

        // total as string instead of number
        let response = json!({
            "result": {
                "hits": {
                    "hit": [record],
                    "sent": "1",
                    "total": "1"
                }
            }
        });

        let _mock = server
            .mock("GET", "/")
            .match_query(mockito::Matcher::Regex("q=year(:|%3A)2023.*".into()))
            .with_status(200)
            .with_body(response.to_string())
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2023, 12, 31, 23, 59, 59).unwrap();

        let result = test_scrape_range_with_mock_url(start, end, &server.url()).await;
        assert!(result.is_ok());
        let records = result.unwrap();
        // Multi-query strategy: 85 queries per year
        assert_eq!(records.len(), 85);
    }

    #[tokio::test]
    async fn test_dblp_with_caching_enabled() {
        use tempfile::TempDir;

        let mut server = Server::new_async().await;
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().join(".dblp_cache");

        let record = json!({
            "info": {
                "title": "Cache Test",
                "authors": {"author": ["Cache Author"]},
                "year": "2023",
                "key": "cache/key"
            }
        });

        let response_body = create_dblp_json_response(&[record]);

        let _mock = server
            .mock("GET", "/")
            .match_query(mockito::Matcher::Regex("q=year(:|%3A)2023.*".into()))
            .with_status(200)
            .with_body(response_body.clone())
            .expect(85) // Multi-query strategy: 85 queries per year
            .create_async()
            .await;

        let config = DblpSourceConfig {
            base_url: server.url(),
            page_size: 100,
            delay_ms: 0,
            enable_cache: true,
            cache_dir: cache_dir.to_string_lossy().to_string(),
            ..Default::default()
        };

        let start = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2023, 12, 31, 23, 59, 59).unwrap();

        // First call - should hit server
        let queue1 = ThreadSafeQueue::new(QueueConfig::default());
        let producer1 = queue1.create_producer();
        let result1 = dblp::scrape_range_with_config(start, end, config.clone(), producer1).await;
        assert!(result1.is_ok());

        let mut recs1 = Vec::new();
        while let Some(r) = queue1.dequeue() {
            recs1.push(r);
        }
        // Multi-query strategy: 85 queries per year
        assert_eq!(recs1.len(), 85);

        // Second call - should use cache
        let queue2 = ThreadSafeQueue::new(QueueConfig::default());
        let producer2 = queue2.create_producer();
        let result2 = dblp::scrape_range_with_config(start, end, config, producer2).await;
        assert!(result2.is_ok());

        let mut recs2 = Vec::new();
        while let Some(r) = queue2.dequeue() {
            recs2.push(r);
        }
        // Multi-query strategy: 85 queries per year
        assert_eq!(recs2.len(), 85);

        // Verify cache directory was created
        assert!(cache_dir.exists());
    }

    #[tokio::test]
    async fn test_dblp_single_author_as_object() {
        let mut server = Server::new_async().await;

        // Single author as object (not array)
        let record = json!({
            "info": {
                "title": "Single Author Test",
                "authors": {"author": {"text": "Solo Author", "pid": "123"}},
                "year": "2023",
                "key": "single/key"
            }
        });

        let response_body = create_dblp_json_response(&[record]);

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
        // Multi-query strategy: 85 queries per year
        assert_eq!(records.len(), 85);
        assert_eq!(records[0].authors, vec!["Solo Author"]);
    }

    #[tokio::test]
    async fn test_dblp_title_as_array() {
        let mut server = Server::new_async().await;

        // Title as array
        let record = json!({
            "info": {
                "title": ["Part 1", "Part 2"],
                "authors": {"author": ["Author"]},
                "year": "2023",
                "key": "array/key"
            }
        });

        let response_body = create_dblp_json_response(&[record]);

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
        // Multi-query strategy: 85 queries per year
        assert_eq!(records.len(), 85);
        assert_eq!(records[0].title, "Part 1 Part 2");
    }

    #[tokio::test]
    async fn test_dblp_venue_as_array() {
        let mut server = Server::new_async().await;

        // Venue as array
        let record = json!({
            "info": {
                "title": "Venue Array Test",
                "authors": {"author": ["Author"]},
                "year": "2023",
                "venue": ["Conference", "Workshop"],
                "key": "venue/key"
            }
        });

        let response_body = create_dblp_json_response(&[record]);

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
        // Multi-query strategy: 85 queries per year
        assert_eq!(records.len(), 85);
        assert_eq!(records[0].venue, Some("Conference, Workshop".to_string()));
    }

    #[tokio::test]
    async fn test_dblp_empty_title_filtered() {
        let mut server = Server::new_async().await;

        // Empty or whitespace title
        let record = json!({
            "info": {
                "title": "   ",
                "authors": {"author": ["Author"]},
                "year": "2023",
                "key": "empty/key"
            }
        });

        let response_body = create_dblp_json_response(&[record]);

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
        // Should be filtered out
        assert_eq!(records.len(), 0);
    }

    #[tokio::test]
    async fn test_dblp_missing_key_uses_default() {
        let mut server = Server::new_async().await;

        // No key field
        let record = json!({
            "info": {
                "title": "No Key Test",
                "authors": {"author": ["Author"]},
                "year": "2023"
            }
        });

        let response_body = create_dblp_json_response(&[record]);

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
        // Multi-query strategy: 85 queries per year
        assert_eq!(records.len(), 85);
        assert_eq!(records[0].id, "unknown");
    }

    #[tokio::test]
    async fn test_dblp_cache_disabled_for_active_years() {
        use tempfile::TempDir;

        let mut server = Server::new_async().await;
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().join(".dblp_cache");

        // Current year (2026 based on system date in context)
        let current_year = Utc::now().year();

        let record_v1 = json!({
            "info": {
                "title": "Paper V1",
                "authors": {"author": ["Author One"]},
                "year": current_year.to_string(),
                "key": "test/key1"
            }
        });

        let record_v2 = json!({
            "info": {
                "title": "Paper V2",
                "authors": {"author": ["Author Two"]},
                "year": current_year.to_string(),
                "key": "test/key2"
            }
        });

        let response_v1 = create_dblp_json_response(&[record_v1]);
        let response_v2 = create_dblp_json_response(&[record_v2]);

        // First request returns V1
        let _mock1 = server
            .mock("GET", "/")
            .match_query(mockito::Matcher::Regex(format!(
                "q=year(:|%3A){}.*",
                current_year
            )))
            .with_status(200)
            .with_body(response_v1)
            .expect(85) // Multi-query strategy: 85 queries per year
            .create_async()
            .await;

        let start = Utc.with_ymd_and_hms(current_year, 1, 1, 0, 0, 0).unwrap();
        let end = Utc
            .with_ymd_and_hms(current_year, 12, 31, 23, 59, 59)
            .unwrap();

        let config = DblpSourceConfig {
            base_url: server.url(),
            page_size: 100,
            delay_ms: 0,
            enable_cache: true, // Cache is enabled
            cache_dir: cache_dir.to_string_lossy().to_string(),
            ..Default::default()
        };

        // First scrape
        let queue1 = ThreadSafeQueue::new(QueueConfig::default());
        let producer1 = queue1.create_producer();
        let result1 = dblp::scrape_range_with_config(start, end, config.clone(), producer1).await;
        assert!(result1.is_ok());

        let mut recs1 = Vec::new();
        while let Some(r) = queue1.dequeue() {
            recs1.push(r);
        }
        // Multi-query strategy: 85 queries per year
        assert_eq!(recs1.len(), 85);
        assert_eq!(recs1[0].title, "Paper V1");

        // Mock now returns V2 (simulating new papers added)
        let _mock2 = server
            .mock("GET", "/")
            .match_query(mockito::Matcher::Regex(format!(
                "q=year(:|%3A){}.*",
                current_year
            )))
            .with_status(200)
            .with_body(response_v2)
            .expect(85) // Multi-query strategy: 85 queries per year // Should hit server again despite cache being enabled
            .create_async()
            .await;

        // Second scrape - should NOT use cache for active year
        let queue2 = ThreadSafeQueue::new(QueueConfig::default());
        let producer2 = queue2.create_producer();
        let result2 = dblp::scrape_range_with_config(start, end, config, producer2).await;
        assert!(result2.is_ok());

        let mut recs2 = Vec::new();
        while let Some(r) = queue2.dequeue() {
            recs2.push(r);
        }
        // Multi-query strategy: 85 queries per year
        assert_eq!(recs2.len(), 85);
        assert_eq!(recs2[0].title, "Paper V2"); // Should get fresh data, not cached V1
    }

    // --- XML Mode Tests ---

    #[test]
    fn test_dblp_mode_from_str() {
        use crate::scrapers::dblp::DblpMode;

        assert_eq!(DblpMode::from_str("search").unwrap(), DblpMode::Search);
        assert_eq!(DblpMode::from_str("Search").unwrap(), DblpMode::Search);
        assert_eq!(DblpMode::from_str("SEARCH").unwrap(), DblpMode::Search);
        
        assert_eq!(DblpMode::from_str("xml").unwrap(), DblpMode::Xml);
        assert_eq!(DblpMode::from_str("Xml").unwrap(), DblpMode::Xml);
        assert_eq!(DblpMode::from_str("XML").unwrap(), DblpMode::Xml);
        
        assert!(DblpMode::from_str("invalid").is_err());
        assert!(DblpMode::from_str("").is_err());
    }

    #[tokio::test]
    async fn test_dblp_xml_mode_basic() {
        use crate::scrapers::dblp::{DblpMode, scrape_range_with_mode};
        use tempfile::TempDir;
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write;

        let temp_dir = TempDir::new().unwrap();
        let xml_download_dir = temp_dir.path().join("xml_downloads");
        let mut server = Server::new_async().await;

        // Create a minimal DBLP XML dump
        let xml_content = r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE dblp SYSTEM "dblp.dtd">
<dblp>
<article key="journals/cacm/Doe2023">
<author>John Doe</author>
<title>Test Article Title</title>
<year>2023</year>
<journal>Communications of the ACM</journal>
</article>
<inproceedings key="conf/icse/Smith2023">
<author>Jane Smith</author>
<author>Bob Johnson</author>
<title>Another Test Paper</title>
<year>2023</year>
<booktitle>ICSE</booktitle>
</inproceedings>
</dblp>"#;

        // Compress the XML content
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(xml_content.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();

        // Mock the XML download endpoint (now using main dump instead of monthly snapshots)
        let _mock = server
            .mock("GET", "/dblp.xml.gz")
            .with_status(200)
            .with_body(compressed)
            .create_async()
            .await;

        let config = DblpSourceConfig {
            xml_base_url: server.url(),
            xml_download_dir: xml_download_dir.to_string_lossy().to_string(),
            ..Default::default()
        };

        let start = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2023, 1, 31, 23, 59, 59).unwrap();

        let queue = ThreadSafeQueue::new(QueueConfig::default());
        let producer = queue.create_producer();
        
        let result = scrape_range_with_mode(start, end, DblpMode::Xml, config, producer).await;
        
        assert!(result.is_ok());
        
        let mut records = Vec::new();
        while let Some(r) = queue.dequeue() {
            records.push(r);
        }
        
        assert_eq!(records.len(), 2);
        
        // Check first record
        assert_eq!(records[0].title, "Test Article Title");
        assert_eq!(records[0].authors, vec!["John Doe"]);
        assert_eq!(records[0].year, 2023);
        assert_eq!(records[0].venue, Some("Communications of the ACM".to_string()));
        assert_eq!(records[0].source, "dblp");
        
        // Check second record
        assert_eq!(records[1].title, "Another Test Paper");
        assert_eq!(records[1].authors, vec!["Jane Smith", "Bob Johnson"]);
        assert_eq!(records[1].year, 2023);
        assert_eq!(records[1].venue, Some("ICSE".to_string()));
    }

    #[tokio::test]
    async fn test_dblp_xml_mode_filters_by_year() {
        use crate::scrapers::dblp::{DblpMode, scrape_range_with_mode};
        use tempfile::TempDir;
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write;

        let temp_dir = TempDir::new().unwrap();
        let xml_download_dir = temp_dir.path().join("xml_downloads");
        let mut server = Server::new_async().await;

        // XML with publications from different years
        let xml_content = r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE dblp SYSTEM "dblp.dtd">
<dblp>
<article key="journals/test/2022">
<author>Author A</author>
<title>Paper from 2022</title>
<year>2022</year>
<journal>Test Journal</journal>
</article>
<article key="journals/test/2023">
<author>Author B</author>
<title>Paper from 2023</title>
<year>2023</year>
<journal>Test Journal</journal>
</article>
<article key="journals/test/2024">
<author>Author C</author>
<title>Paper from 2024</title>
<year>2024</year>
<journal>Test Journal</journal>
</article>
</dblp>"#;

        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(xml_content.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();

        let _mock = server
            .mock("GET", "/dblp.xml.gz")
            .with_status(200)
            .with_body(compressed)
            .create_async()
            .await;

        let config = DblpSourceConfig {
            xml_base_url: server.url(),
            xml_download_dir: xml_download_dir.to_string_lossy().to_string(),
            ..Default::default()
        };

        // Only request 2023
        let start = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2023, 12, 31, 23, 59, 59).unwrap();

        let queue = ThreadSafeQueue::new(QueueConfig::default());
        let producer = queue.create_producer();
        
        let result = scrape_range_with_mode(start, end, DblpMode::Xml, config, producer).await;
        
        assert!(result.is_ok());
        
        let mut records = Vec::new();
        while let Some(r) = queue.dequeue() {
            records.push(r);
        }
        
        // Should only get the 2023 paper
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].title, "Paper from 2023");
        assert_eq!(records[0].year, 2023);
    }

    #[tokio::test]
    async fn test_dblp_xml_mode_missing_fields() {
        use crate::scrapers::dblp::{DblpMode, scrape_range_with_mode};
        use tempfile::TempDir;
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write;

        let temp_dir = TempDir::new().unwrap();
        let xml_download_dir = temp_dir.path().join("xml_downloads");
        let mut server = Server::new_async().await;

        // XML with invalid/incomplete records
        let xml_content = r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE dblp SYSTEM "dblp.dtd">
<dblp>
<article key="journals/test/1">
<title>No Author</title>
<year>2023</year>
<journal>Test Journal</journal>
</article>
<article key="journals/test/2">
<author>Has Author</author>
<year>2023</year>
<journal>Test Journal</journal>
</article>
<article key="journals/test/3">
<author>Valid Record</author>
<title>Valid Title</title>
<year>2023</year>
<journal>Test Journal</journal>
</article>
<article key="journals/test/4">
<author>Empty Title</author>
<title>   </title>
<year>2023</year>
<journal>Test Journal</journal>
</article>
</dblp>"#;

        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(xml_content.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();

        let _mock = server
            .mock("GET", "/dblp.xml.gz")
            .with_status(200)
            .with_body(compressed)
            .create_async()
            .await;

        let config = DblpSourceConfig {
            xml_base_url: server.url(),
            xml_download_dir: xml_download_dir.to_string_lossy().to_string(),
            ..Default::default()
        };

        let start = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2023, 12, 31, 23, 59, 59).unwrap();

        let queue = ThreadSafeQueue::new(QueueConfig::default());
        let producer = queue.create_producer();
        
        let result = scrape_range_with_mode(start, end, DblpMode::Xml, config, producer).await;
        
        assert!(result.is_ok());
        
        let mut records = Vec::new();
        while let Some(r) = queue.dequeue() {
            records.push(r);
        }
        
        // Should only get the valid record
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].title, "Valid Title");
        assert_eq!(records[0].authors, vec!["Valid Record"]);
    }

    #[tokio::test]
    async fn test_dblp_xml_mode_multiple_months() {
        use crate::scrapers::dblp::{DblpMode, scrape_range_with_mode};
        use tempfile::TempDir;
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write;

        let temp_dir = TempDir::new().unwrap();
        let xml_download_dir = temp_dir.path().join("xml_downloads");
        let mut server = Server::new_async().await;

        // Create XML with records from both January and February
        // (Now we download the full dump once and filter by date)
        let xml_content = r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE dblp SYSTEM "dblp.dtd">
<dblp>
<article key="journals/test/jan" mdate="2023-01-15">
<author>January Author</author>
<title>January Paper</title>
<year>2023</year>
<journal>Test Journal</journal>
</article>
<article key="journals/test/feb" mdate="2023-02-15">
<author>February Author</author>
<title>February Paper</title>
<year>2023</year>
<journal>Test Journal</journal>
</article>
</dblp>"#;

        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(xml_content.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();

        let _mock = server
            .mock("GET", "/dblp.xml.gz")
            .with_status(200)
            .with_body(compressed)
            .create_async()
            .await;

        let config = DblpSourceConfig {
            xml_base_url: server.url(),
            xml_download_dir: xml_download_dir.to_string_lossy().to_string(),
            ..Default::default()
        };

        // Request Jan-Feb 2023
        let start = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2023, 2, 28, 23, 59, 59).unwrap();

        let queue = ThreadSafeQueue::new(QueueConfig::default());
        let producer = queue.create_producer();
        
        let result = scrape_range_with_mode(start, end, DblpMode::Xml, config, producer).await;
        
        assert!(result.is_ok());
        
        let mut records = Vec::new();
        while let Some(r) = queue.dequeue() {
            records.push(r);
        }
        
        // Should get both papers
        assert_eq!(records.len(), 2);
        
        let titles: Vec<&str> = records.iter().map(|r| r.title.as_str()).collect();
        assert!(titles.contains(&"January Paper"));
        assert!(titles.contains(&"February Paper"));
    }

    #[tokio::test]
    async fn test_dblp_xml_mode_download_failure() {
        use crate::scrapers::dblp::{DblpMode, scrape_range_with_mode};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let xml_download_dir = temp_dir.path().join("xml_downloads");
        let mut server = Server::new_async().await;

        // Mock returns 404
        let _mock = server
            .mock("GET", "/dblp.xml.gz")
            .with_status(404)
            .create_async()
            .await;

        let config = DblpSourceConfig {
            xml_base_url: server.url(),
            xml_download_dir: xml_download_dir.to_string_lossy().to_string(),
            ..Default::default()
        };

        let start = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2023, 1, 31, 23, 59, 59).unwrap();

        let queue = ThreadSafeQueue::new(QueueConfig::default());
        let producer = queue.create_producer();
        
        // Should not fail, just log error and continue
        let result = scrape_range_with_mode(start, end, DblpMode::Xml, config, producer).await;
        
        assert!(result.is_ok());
        
        let mut records = Vec::new();
        while let Some(r) = queue.dequeue() {
            records.push(r);
        }
        
        assert_eq!(records.len(), 0);
    }

    #[tokio::test]
    async fn test_dblp_xml_mode_cleans_up_files() {
        use crate::scrapers::dblp::{DblpMode, scrape_range_with_mode};
        use tempfile::TempDir;
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write;

        let temp_dir = TempDir::new().unwrap();
        let xml_download_dir = temp_dir.path().join("xml_downloads");
        let mut server = Server::new_async().await;

        let xml_content = r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE dblp SYSTEM "dblp.dtd">
<dblp>
<article key="journals/test/1">
<author>Test Author</author>
<title>Test Title</title>
<year>2023</year>
<journal>Test Journal</journal>
</article>
</dblp>"#;

        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(xml_content.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();

        let _mock = server
            .mock("GET", "/dblp.xml.gz")
            .with_status(200)
            .with_body(compressed)
            .create_async()
            .await;

        let config = DblpSourceConfig {
            xml_base_url: server.url(),
            xml_download_dir: xml_download_dir.to_string_lossy().to_string(),
            ..Default::default()
        };

        let start = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2023, 1, 31, 23, 59, 59).unwrap();

        let queue = ThreadSafeQueue::new(QueueConfig::default());
        let producer = queue.create_producer();
        
        let result = scrape_range_with_mode(start, end, DblpMode::Xml, config, producer).await;
        
        assert!(result.is_ok());
        
        // Check that the downloaded file was deleted
        let expected_file = xml_download_dir.join("dblp-2023-01-01.xml.gz");
        assert!(!expected_file.exists(), "Downloaded file should be deleted after processing");
    }

    #[tokio::test]
    async fn test_dblp_xml_mode_handles_special_characters() {
        use crate::scrapers::dblp::{DblpMode, scrape_range_with_mode};
        use tempfile::TempDir;
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write;

        let temp_dir = TempDir::new().unwrap();
        let xml_download_dir = temp_dir.path().join("xml_downloads");
        let mut server = Server::new_async().await;

        // XML with special characters and HTML entities
        let xml_content = r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE dblp SYSTEM "dblp.dtd">
<dblp>
<article key="journals/test/1">
<author>François Müller</author>
<author>José García</author>
<title>A Survey on Machine Learning &amp; AI</title>
<year>2023</year>
<journal>Test Journal</journal>
</article>
</dblp>"#;

        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(xml_content.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();

        let _mock = server
            .mock("GET", "/dblp.xml.gz")
            .with_status(200)
            .with_body(compressed)
            .create_async()
            .await;

        let config = DblpSourceConfig {
            xml_base_url: server.url(),
            xml_download_dir: xml_download_dir.to_string_lossy().to_string(),
            ..Default::default()
        };

        let start = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2023, 12, 31, 23, 59, 59).unwrap();

        let queue = ThreadSafeQueue::new(QueueConfig::default());
        let producer = queue.create_producer();
        
        let result = scrape_range_with_mode(start, end, DblpMode::Xml, config, producer).await;
        
        assert!(result.is_ok());
        
        let mut records = Vec::new();
        while let Some(r) = queue.dequeue() {
            records.push(r);
        }
        
        assert_eq!(records.len(), 1);
        // Note: XML entities are decoded, but the title might have spacing issues
        // depending on how the XML parser handles entity boundaries
        assert!(records[0].title.contains("Machine Learning"));
        assert!(records[0].title.contains("AI"));
        assert_eq!(records[0].authors, vec!["François Müller", "José García"]);
    }

    #[tokio::test]
    async fn test_dblp_xml_mode_different_publication_types() {
        use crate::scrapers::dblp::{DblpMode, scrape_range_with_mode};
        use tempfile::TempDir;
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write;

        let temp_dir = TempDir::new().unwrap();
        let xml_download_dir = temp_dir.path().join("xml_downloads");
        let mut server = Server::new_async().await;

        // XML with different publication types
        let xml_content = r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE dblp SYSTEM "dblp.dtd">
<dblp>
<article key="journals/test/1">
<author>Author A</author>
<title>Journal Article</title>
<year>2023</year>
<journal>Test Journal</journal>
</article>
<inproceedings key="conf/test/1">
<author>Author B</author>
<title>Conference Paper</title>
<year>2023</year>
<booktitle>Test Conference</booktitle>
</inproceedings>
<book key="books/test/1">
<author>Author C</author>
<title>Test Book</title>
<year>2023</year>
</book>
<phdthesis key="phd/test/1">
<author>Author D</author>
<title>PhD Thesis</title>
<year>2023</year>
</phdthesis>
</dblp>"#;

        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(xml_content.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();

        let _mock = server
            .mock("GET", "/dblp.xml.gz")
            .with_status(200)
            .with_body(compressed)
            .create_async()
            .await;

        let config = DblpSourceConfig {
            xml_base_url: server.url(),
            xml_download_dir: xml_download_dir.to_string_lossy().to_string(),
            ..Default::default()
        };

        let start = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2023, 12, 31, 23, 59, 59).unwrap();

        let queue = ThreadSafeQueue::new(QueueConfig::default());
        let producer = queue.create_producer();
        
        let result = scrape_range_with_mode(start, end, DblpMode::Xml, config, producer).await;
        
        assert!(result.is_ok());
        
        let mut records = Vec::new();
        while let Some(r) = queue.dequeue() {
            records.push(r);
        }
        
        // Should get all 4 publication types
        assert_eq!(records.len(), 4);
        
        let titles: Vec<&str> = records.iter().map(|r| r.title.as_str()).collect();
        assert!(titles.contains(&"Journal Article"));
        assert!(titles.contains(&"Conference Paper"));
        assert!(titles.contains(&"Test Book"));
        assert!(titles.contains(&"PhD Thesis"));
        
        // Check venues are captured correctly
        let article = records.iter().find(|r| r.title == "Journal Article").unwrap();
        assert_eq!(article.venue, Some("Test Journal".to_string()));
        
        let conf = records.iter().find(|r| r.title == "Conference Paper").unwrap();
        assert_eq!(conf.venue, Some("Test Conference".to_string()));
    }
}
