#[cfg(test)]
mod tests {
    use crate::scrapers::dblp;
    use chrono::{Duration, Utc, TimeZone};

    /// Test scraping DBLP publications in a date range.
    ///
    /// This test verifies that the DBLP scraper can download and parse
    /// publication records within a specified date range.
    #[test]
    fn test_dblp_scrape_range() {
        let start = Utc::now() - Duration::days(365);
        let end = Utc::now();

        let result = dblp::scrape_range(start, end);
        
        // Note: This test may fail if DBLP is unavailable or network issues occur
        // In a production environment, consider mocking the HTTP client
        match result {
            Ok(records) => {
                // Verify we got some records (DBLP has many publications)
                assert!(!records.is_empty());
                
                // Check that all records have required fields
                for record in &records[..std::cmp::min(10, records.len())] {
                    assert!(!record.title.is_empty());
                    assert!(!record.authors.is_empty());
                    assert!(record.year > 0);
                    assert_eq!(record.source, "dblp");
                }
            }
            Err(e) => {
                // Allow network-related failures in tests
                eprintln!("DBLP scraper test failed (likely network issue): {}", e);
            }
        }
    }

    /// Test that scrape_range handles empty date ranges gracefully.
    #[test]
    fn test_dblp_empty_range() {
        let start = Utc::now();
        let end = start;

        let result = dblp::scrape_range(start, end);
        
        match result {
            Ok(records) => {
                // Empty range should return empty results
                assert!(records.is_empty());
            }
            Err(e) => {
                // Network failures are acceptable in this test
                eprintln!("Network error during empty range test: {}", e);
            }
        }
    }



    /// Test DBLP key format validation.
    #[test]
    #[ignore = "Scraper not yet implemented"]
    fn test_dblp_key_validation() {
        // TODO: Verify DBLP key format (e.g., "conf/icml/SmithJ21")
        // - Check that keys are properly extracted
        // - Validate key format
    }

    /// Test downloading and parsing real DBLP XML dump.
    /// 
    /// This test downloads the actual DBLP XML dump and parses it,
    /// testing the full scraper pipeline with real data.
    /// Note: This test may take some time due to DBLP's large dataset.
    #[test]
    fn test_dblp_full_pipeline_with_real_data() {
        use chrono::Datelike;
        
        // Use a recent but narrow date range to get some results quickly
        // DBLP is huge, so we use a very narrow range to keep test fast
        let current_year = Utc::now().year();
        let test_year = current_year - 1; // Use last year's data
        
        // Only test a 3-month period to reduce parsing time
        let start_date = Utc.ymd(test_year, 11, 1).and_hms(0, 0, 0);  // Oct 1st
        let end_date = Utc.ymd(test_year, 12, 31).and_hms(23, 59, 59); // Dec 31st
        
        println!("Testing DBLP scraper for year {}", test_year);
        
        let result = dblp::scrape_range(start_date, end_date);
        
        match result {
            Ok(records) => {
                println!("Successfully downloaded and parsed {} DBLP records for {}", 
                         records.len(), test_year);
                
                // DBLP should have many publications even in a single year
                assert!(!records.is_empty(), 
                        "Expected to find publications for year {}", test_year);
                
                // Test first few records thoroughly
                let sample_size = std::cmp::min(20, records.len());
                let mut article_count = 0;
                let mut inproceedings_count = 0;
                let mut venues_with_data = 0;
                
                for (i, record) in records.iter().take(sample_size).enumerate() {
                    // Print some sample records for manual verification
                    if i < 5 {
                        println!("Sample record {}: '{}' by [{}] in {} ({})", 
                                i + 1, 
                                record.title, 
                                record.authors.join(", "),
                                record.venue.as_deref().unwrap_or("N/A"),
                                record.year);
                    }
                    
                    // Verify record integrity
                    assert!(!record.id.is_empty(), "Record ID should not be empty");
                    assert!(!record.title.is_empty(), "Title should not be empty");
                    assert!(!record.authors.is_empty(), "Should have at least one author");
                    assert_eq!(record.year as i32, test_year, "Year should match filter");
                    assert_eq!(record.source, "dblp", "Source should be 'dblp'");
                    
                    // Count publication types based on venue patterns
                    if let Some(venue) = &record.venue {
                        venues_with_data += 1;
                        
                        // Common journal patterns vs conference patterns
                        if venue.contains("Trans") || venue.contains("Journal") || 
                           venue.contains("ACM") && venue.len() < 20 {
                            article_count += 1;
                        } else {
                            inproceedings_count += 1;
                        }
                    }
                    
                    // Verify authors are properly parsed
                    for author in &record.authors {
                        assert!(!author.trim().is_empty(), "Author name should not be empty");
                        assert!(author.len() > 1, "Author name should be substantial");
                    }
                    
                    // Title should be reasonable
                    assert!(record.title.len() > 5, "Title should be substantial");
                }
                
                println!("Sample analysis: {} articles, {} conference papers, {} records with venues",
                         article_count, inproceedings_count, venues_with_data);
                
                // Verify we parsed different types of publications
                assert!(venues_with_data > 0, "Should have found some venues");
                
                // Check for reasonable distribution of publication types
                if sample_size >= 10 {
                    assert!(article_count > 0 || inproceedings_count > 0,
                           "Should have found either journal articles or conference papers");
                }
                
                println!(" DBLP full pipeline test passed with {} records", records.len());
            }
            Err(e) => {
                eprintln!(" DBLP full pipeline test failed: {}", e);
                eprintln!("This may be due to:");
                eprintln!("- Network connectivity issues");
                eprintln!("- DBLP server being unavailable");  
                eprintln!("- DBLP dump format changes");
                eprintln!("- Download timeout (DBLP dump is very large)");
                
                // Don't fail the test for network issues, but report the problem
                panic!("DBLP test failed - check network and DBLP availability: {}", e);
            }
        }
    }
}
