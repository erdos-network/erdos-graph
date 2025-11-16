use crate::logger::{LogLevel, Logger, init_logger, log};
use std::sync::{Arc, Mutex};

struct CapturingLogger {
    records: Arc<Mutex<Vec<(LogLevel, String)>>>,
}

impl CapturingLogger {
    fn new() -> Self {
        Self {
            records: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn records_clone(&self) -> Arc<Mutex<Vec<(LogLevel, String)>>> {
        self.records.clone()
    }
}

impl Logger for CapturingLogger {
    fn log(&self, level: LogLevel, message: &str) {
        let mut guard = self.records.lock().unwrap();
        guard.push((level, message.to_string()));
    }
}

#[test]
fn facade_records_messages() {
    let cap = CapturingLogger::new();
    let records = cap.records_clone();
    // Install as global logger
    init_logger(cap);

    log(LogLevel::Info, "hello world");
    log(LogLevel::Warn, "something happened");

    let guard = records.lock().unwrap();
    assert_eq!(guard.len(), 2);
    assert_eq!(guard[0].0, LogLevel::Info);
    assert_eq!(guard[0].1, "hello world");
    assert_eq!(guard[1].0, LogLevel::Warn);
    assert_eq!(guard[1].1, "something happened");
}
