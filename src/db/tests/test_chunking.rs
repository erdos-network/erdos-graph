use crate::db::ingestion::chunk_date_range;
use chrono::{TimeZone, Utc};

/// Test chunking a date range into multiple chunks.
#[test]
fn test_chunk_date_range_multiple_chunks() {
    let start = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2023, 1, 8, 0, 0, 0).unwrap();
    let chunk_size_days = 2;

    let chunks = chunk_date_range(start, end, chunk_size_days).unwrap();

    assert_eq!(chunks.len(), 4);
    assert_eq!(
        chunks[0],
        (start, Utc.with_ymd_and_hms(2023, 1, 3, 0, 0, 0).unwrap())
    );
    assert_eq!(
        chunks[1],
        (
            Utc.with_ymd_and_hms(2023, 1, 3, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2023, 1, 5, 0, 0, 0).unwrap()
        )
    );
    assert_eq!(
        chunks[2],
        (
            Utc.with_ymd_and_hms(2023, 1, 5, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2023, 1, 7, 0, 0, 0).unwrap()
        )
    );
    assert_eq!(
        chunks[3],
        (Utc.with_ymd_and_hms(2023, 1, 7, 0, 0, 0).unwrap(), end)
    );
}

/// Test chunking when the range is smaller than chunk size.
#[test]
fn test_chunk_date_range_single_chunk() {
    let start = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2023, 1, 2, 0, 0, 0).unwrap();
    let chunk_size_days = 2;

    let chunks = chunk_date_range(start, end, chunk_size_days).unwrap();

    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0], (start, end));
}

/// Test chunking when the range exactly matches chunk size.
#[test]
fn test_chunk_date_range_exact_chunk() {
    let start = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2023, 1, 3, 0, 0, 0).unwrap();
    let chunk_size_days = 2;

    let chunks = chunk_date_range(start, end, chunk_size_days).unwrap();

    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0], (start, end));
}

/// Test that zero chunk size returns a single chunk.
#[test]
fn test_chunk_date_range_zero_size() {
    let start = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2023, 1, 8, 0, 0, 0).unwrap();
    let chunk_size_days = 0;

    let chunks = chunk_date_range(start, end, chunk_size_days).unwrap();

    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0], (start, end));
}
