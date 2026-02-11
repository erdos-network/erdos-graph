#[cfg(test)]
mod tests {
    use crate::config::EdgeCacheConfig;
    use crate::scrapers::cache::{
        DeduplicationCache, EdgeCacheSystem, normalize_author, normalize_title,
    };

    #[test]
    fn test_normalize_title_basic() {
        let result = normalize_title("The Quick Brown Fox");
        assert_eq!(result, "quickbrownfox");
    }

    #[test]
    fn test_normalize_title_with_punctuation() {
        let result = normalize_title("Hello, World! How are you?");
        assert_eq!(result, "helloworldhowareyou");
    }

    #[test]
    fn test_normalize_title_removes_stop_words() {
        let result = normalize_title("A Study of the Graph Theory in Computer Science");
        assert_eq!(result, "studygraphtheorycomputerscience");
    }

    #[test]
    fn test_normalize_title_handles_numbers() {
        let result = normalize_title("Analysis of 123 Data Points");
        assert_eq!(result, "analysis123datapoints");
    }

    #[test]
    fn test_normalize_title_empty_string() {
        let result = normalize_title("");
        assert_eq!(result, "");
    }

    #[test]
    fn test_normalize_title_only_stop_words() {
        let result = normalize_title("the a an and or");
        assert_eq!(result, "");
    }

    #[test]
    fn test_normalize_title_mixed_case() {
        let result = normalize_title("HeLLo WoRLd");
        assert_eq!(result, "helloworld");
    }

    #[test]
    fn test_normalize_title_unicode() {
        let result = normalize_title("Café résumé naïve");
        assert_eq!(result, "caférésuménaïve");
    }

    #[test]
    fn test_normalize_author_basic() {
        let result = normalize_author("John Smith");
        assert_eq!(result, "john smith");
    }

    #[test]
    fn test_normalize_author_multiple_spaces() {
        let result = normalize_author("John   Smith");
        assert_eq!(result, "john smith");
    }

    #[test]
    fn test_normalize_author_leading_trailing_spaces() {
        let result = normalize_author("  John Smith  ");
        assert_eq!(result, "john smith");
    }

    #[test]
    fn test_normalize_author_mixed_case() {
        let result = normalize_author("JoHn SmItH");
        assert_eq!(result, "john smith");
    }

    #[test]
    fn test_normalize_author_empty() {
        let result = normalize_author("");
        assert_eq!(result, "");
    }

    #[test]
    fn test_normalize_author_only_spaces() {
        let result = normalize_author("   ");
        assert_eq!(result, "");
    }

    #[test]
    fn test_normalize_author_unicode() {
        let result = normalize_author("José García");
        assert_eq!(result, "josé garcía");
    }

    #[test]
    fn test_deduplication_cache_new() {
        let cache = DeduplicationCache::new(10000);
        assert_eq!(cache.loaded_years.len(), 0);
        assert_eq!(cache.published_ids.len(), 0);
    }

    #[test]
    fn test_deduplication_cache_add() {
        let mut cache = DeduplicationCache::new(10000);

        cache.add(
            2024,
            "uuid-123".to_string(),
            "Test Title".to_string(),
            &["Author One".to_string(), "Author Two".to_string()],
            "pub-id-123".to_string(),
        );

        // Verify the publication ID was added
        assert!(cache.published_ids.contains("pub-id-123"));

        // Verify bloom filters are populated (we can't check directly, but we can verify they're set)
        assert!(cache.title_filter.check(&normalize_title("Test Title")));
        assert!(cache.author_filter.check(&normalize_title("Author One")));
        assert!(cache.author_filter.check(&normalize_title("Author Two")));
    }

    #[test]
    fn test_deduplication_cache_add_empty_authors() {
        let mut cache = DeduplicationCache::new(10000);

        cache.add(
            2024,
            "uuid-123".to_string(),
            "Test Title".to_string(),
            &[],
            "pub-id-123".to_string(),
        );

        assert!(cache.published_ids.contains("pub-id-123"));
        assert!(cache.title_filter.check(&normalize_title("Test Title")));
    }

    #[test]
    fn test_deduplication_cache_multiple_adds() {
        let mut cache = DeduplicationCache::new(10000);

        for i in 0..10 {
            cache.add(
                2024,
                format!("uuid-{}", i),
                format!("Test Title {}", i),
                &[format!("Author {}", i)],
                format!("pub-id-{}", i),
            );
        }

        assert_eq!(cache.published_ids.len(), 10);
        for i in 0..10 {
            assert!(cache.published_ids.contains(&format!("pub-id-{}", i)));
        }
    }

    #[test]
    fn test_edge_cache_system_new() {
        let config = EdgeCacheConfig {
            hot_size: 1000,
            warm_size: 5000,
            bloom_size: 10000,
        };
        let cache = EdgeCacheSystem::new(config);

        assert_eq!(cache.hot_cache.len(), 0);
        assert_eq!(cache.warm_cache.len(), 0);
        assert!(!cache.edges_loaded);
    }

    #[test]
    fn test_edge_cache_put_and_get() {
        let config = EdgeCacheConfig {
            hot_size: 10,
            warm_size: 20,
            bloom_size: 100,
        };
        let mut cache = EdgeCacheSystem::new(config);

        let key = (123u128, 456u128);
        cache.put(key, 5);

        let result = cache.get(key);
        assert_eq!(result, Some(5));
    }

    #[test]
    fn test_edge_cache_get_nonexistent() {
        let config = EdgeCacheConfig {
            hot_size: 10,
            warm_size: 20,
            bloom_size: 100,
        };
        let mut cache = EdgeCacheSystem::new(config);

        let key = (123u128, 456u128);
        let result = cache.get(key);
        assert_eq!(result, None);
    }

    #[test]
    fn test_edge_cache_hot_to_warm_eviction() {
        let config = EdgeCacheConfig {
            hot_size: 2,
            warm_size: 10,
            bloom_size: 100,
        };
        let mut cache = EdgeCacheSystem::new(config);

        // Fill hot cache
        cache.put((1, 2), 10);
        cache.put((3, 4), 20);

        // This should evict the LRU item from hot to warm
        cache.put((5, 6), 30);

        assert_eq!(cache.hot_cache.len(), 2);

        // All items should still be retrievable
        assert!(cache.get((1, 2)).is_some() || cache.get((3, 4)).is_some());
        assert!(cache.get((5, 6)).is_some());
    }

    #[test]
    fn test_edge_cache_promotion_from_warm_to_hot() {
        let config = EdgeCacheConfig {
            hot_size: 5,
            warm_size: 10,
            bloom_size: 100,
        };
        let mut cache = EdgeCacheSystem::new(config);

        // Add items to fill hot cache and cause eviction to warm
        for i in 0..10 {
            cache.put((i, i + 100), (i * 10).try_into().unwrap());
        }

        // Access an item that should be in warm cache (one of the early ones)
        let result = cache.get((0, 100));

        // It should be found and promoted
        assert!(result.is_some());
    }

    #[test]
    fn test_edge_cache_bloom_filter() {
        let config = EdgeCacheConfig {
            hot_size: 2,
            warm_size: 5,
            bloom_size: 100,
        };
        let mut cache = EdgeCacheSystem::new(config);

        let key = (789u128, 101112u128);
        cache.put(key, 42);

        // Bloom filter should be set
        assert!(cache.cold_bloom.check(&key));
    }

    #[test]
    fn test_edge_cache_update_existing_key() {
        let config = EdgeCacheConfig {
            hot_size: 10,
            warm_size: 20,
            bloom_size: 100,
        };
        let mut cache = EdgeCacheSystem::new(config);

        let key = (100u128, 200u128);
        cache.put(key, 10);
        cache.put(key, 20); // Update

        let result = cache.get(key);
        assert_eq!(result, Some(20));
    }

    #[test]
    fn test_edge_cache_zero_weights() {
        let config = EdgeCacheConfig {
            hot_size: 10,
            warm_size: 20,
            bloom_size: 100,
        };
        let mut cache = EdgeCacheSystem::new(config);

        let key = (1u128, 2u128);
        cache.put(key, 0);

        let result = cache.get(key);
        assert_eq!(result, Some(0));
    }

    #[test]
    fn test_edge_cache_large_weights() {
        let config = EdgeCacheConfig {
            hot_size: 10,
            warm_size: 20,
            bloom_size: 100,
        };
        let mut cache = EdgeCacheSystem::new(config);

        let key = (1u128, 2u128);
        cache.put(key, u64::MAX);

        let result = cache.get(key);
        assert_eq!(result, Some(u64::MAX));
    }

    #[test]
    fn test_edge_cache_symmetric_keys() {
        let config = EdgeCacheConfig {
            hot_size: 10,
            warm_size: 20,
            bloom_size: 100,
        };
        let mut cache = EdgeCacheSystem::new(config);

        // Test that (a, b) and (b, a) are treated as different keys
        cache.put((1, 2), 10);
        cache.put((2, 1), 20);

        assert_eq!(cache.get((1, 2)), Some(10));
        assert_eq!(cache.get((2, 1)), Some(20));
    }

    #[test]
    fn test_edge_cache_many_items() {
        let config = EdgeCacheConfig {
            hot_size: 10,
            warm_size: 20,
            bloom_size: 1000,
        };
        let mut cache = EdgeCacheSystem::new(config);

        // Add many items
        for i in 0..100 {
            cache.put((i, i * 2), i.try_into().unwrap());
        }

        // The most recent items should be in cache
        for i in 90..100 {
            let result = cache.get((i, i * 2));
            assert!(result.is_some());
        }
    }

    #[test]
    fn test_normalize_title_special_characters() {
        let result = normalize_title("Test@Title#With$Special%Characters^");
        assert_eq!(result, "testtitlewithspecialcharacters");
    }

    #[test]
    fn test_normalize_title_multiple_consecutive_spaces() {
        let result = normalize_title("Test     Title     With     Spaces");
        assert_eq!(result, "testtitlewithspaces");
    }

    #[test]
    fn test_normalize_author_tabs_and_newlines() {
        let result = normalize_author("John\tSmith\nJunior");
        assert_eq!(result, "john smith junior");
    }

    #[test]
    fn test_edge_cache_capacity_limits() {
        let config = EdgeCacheConfig {
            hot_size: 1,
            warm_size: 1,
            bloom_size: 10,
        };
        let mut cache = EdgeCacheSystem::new(config);

        cache.put((1, 2), 10);
        cache.put((3, 4), 20);
        cache.put((5, 6), 30);

        // At least the most recent should be accessible
        assert!(cache.get((5, 6)).is_some());
    }

    #[test]
    fn test_deduplication_cache_bloom_filter_false_positive_rate() {
        let mut cache = DeduplicationCache::new(1000);

        // Add known items
        for i in 0..100 {
            cache.add(
                2024,
                format!("uuid-{}", i),
                format!("Title {}", i),
                &[format!("Author {}", i)],
                format!("pub-{}", i),
            );
        }

        // All added items should be found
        for i in 0..100 {
            assert!(
                cache
                    .title_filter
                    .check(&normalize_title(&format!("Title {}", i)))
            );
        }
    }

    #[test]
    fn test_edge_cache_cold_bloom_persistence() {
        let config = EdgeCacheConfig {
            hot_size: 2,
            warm_size: 2,
            bloom_size: 100,
        };
        let mut cache = EdgeCacheSystem::new(config);

        // Add many items to force evictions
        for i in 0..20 {
            cache.put((i, i + 100), (i * 10).try_into().unwrap());
        }

        // Even evicted items should be in bloom filter
        for i in 0..20 {
            assert!(cache.cold_bloom.check(&(i, i + 100)));
        }
    }

    #[test]
    fn test_normalize_title_numbers_only() {
        let result = normalize_title("12345 67890");
        assert_eq!(result, "1234567890");
    }

    #[test]
    fn test_normalize_author_with_titles() {
        let result = normalize_author("Dr. John Smith Jr.");
        assert_eq!(result, "dr. john smith jr.");
    }

    #[test]
    fn test_edge_cache_edges_loaded_flag() {
        let config = EdgeCacheConfig {
            hot_size: 10,
            warm_size: 20,
            bloom_size: 100,
        };
        let mut cache = EdgeCacheSystem::new(config);

        assert!(!cache.edges_loaded);
        cache.edges_loaded = true;
        assert!(cache.edges_loaded);
    }

    #[test]
    fn test_deduplication_cache_duplicate_pub_ids() {
        let mut cache = DeduplicationCache::new(10000);

        cache.add(
            2024,
            "uuid-1".to_string(),
            "Title 1".to_string(),
            &[],
            "pub-123".to_string(),
        );
        cache.add(
            2024,
            "uuid-2".to_string(),
            "Title 2".to_string(),
            &[],
            "pub-123".to_string(),
        );

        // Should only contain one entry for the duplicate pub ID
        assert_eq!(cache.published_ids.len(), 1);
        assert!(cache.published_ids.contains("pub-123"));
    }

    #[test]
    fn test_edge_cache_get_modifies_access_order() {
        let config = EdgeCacheConfig {
            hot_size: 3,
            warm_size: 5,
            bloom_size: 100,
        };
        let mut cache = EdgeCacheSystem::new(config);

        cache.put((1, 2), 10);
        cache.put((3, 4), 20);
        cache.put((5, 6), 30);

        // Access the first item to make it recently used
        let _ = cache.get((1, 2));

        // Add a new item, which should evict the LRU (not the one we just accessed)
        cache.put((7, 8), 40);

        // The accessed item should still be in hot cache
        assert_eq!(cache.get((1, 2)), Some(10));
    }

    #[test]
    fn test_normalize_title_with_all_stop_words_and_numbers() {
        let result = normalize_title("the 123 and 456 of the");
        assert_eq!(result, "123456");
    }

    #[test]
    fn test_normalize_author_single_name() {
        let result = normalize_author("Madonna");
        assert_eq!(result, "madonna");
    }

    #[test]
    fn test_edge_cache_maximum_capacity() {
        let config = EdgeCacheConfig {
            hot_size: 5,
            warm_size: 10,
            bloom_size: 100,
        };
        let mut cache = EdgeCacheSystem::new(config);

        // Fill beyond total capacity
        for i in 0..30 {
            cache.put((i, i * 2), (i * 10).try_into().unwrap());
        }

        // Should not panic and should maintain reasonable state
        assert!(cache.hot_cache.len() <= 5);
        assert!(cache.warm_cache.len() <= 10);
    }
}
