//! Tests for the logging primitives.

// TODO: Add tests for StdoutLogger formatting once format is stabilized.

use super::super::logger::{LogLevel, Logger, NoopLogger};

struct TestLogger {
    records: std::sync::Mutex<Vec<(LogLevel, String)>>,
}

impl TestLogger {
    fn new() -> Self {
        Self {
            records: std::sync::Mutex::new(Vec::new()),
        }
    }
}

impl Logger for TestLogger {
    fn log(&self, level: LogLevel, message: &str) {
        let mut records = self.records.lock().unwrap();
        records.push((level, message.to_string()));
    }
}

#[test]
fn noop_logger_does_not_panic() {
    let logger = NoopLogger;
    logger.info("hello");
    logger.flush();
}

#[test]
fn convenience_methods_delegate_to_log() {
    let logger = TestLogger::new();
    logger.trace("t");
    logger.debug("d");
    logger.info("i");
    logger.warn("w");
    logger.error("e");

    let records = logger.records.lock().unwrap();
    let levels: Vec<LogLevel> = records.iter().map(|(lvl, _)| *lvl).collect();
    assert_eq!(
        levels,
        vec![
            LogLevel::Trace,
            LogLevel::Debug,
            LogLevel::Info,
            LogLevel::Warn,
            LogLevel::Error,
        ]
    );
}
