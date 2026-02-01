//! Custom logging primitives for the Erdős Graph project.
//!
//! Responsibilities:
//! - Provide a lightweight `Logger` trait used across crates
//! - Offer a baseline no-op implementation for tests and benchmarking
//! - Centralize log level semantics without pulling a full logging framework
//!
//! TODOs:
//! - Add an environment-configurable global logger facade
//! - Add structured fields (key=value) support
//! - Add async, buffered writer with backpressure and flush control
//!

use crossbeam_channel::{Sender, bounded};
use serde::{Deserialize, Serialize};
use std::thread;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    /// Return a short string representation suitable for logs.
    pub fn as_str(&self) -> &'static str {
        match self {
            LogLevel::Trace => "TRACE",
            LogLevel::Debug => "DEBUG",
            LogLevel::Info => "INFO",
            LogLevel::Warn => "WARN",
            LogLevel::Error => "ERROR",
        }
    }
}

/// Minimal logger interface used throughout the project.
///
/// Implementors should be cheap to clone and thread-safe if used in multithreaded contexts.
/// For stubs, implement `log` and optionally override convenience methods.
/// TODO: consider adding `Send + Sync` bounds once we finalize the threading model.
/// The logger trait used across the project.
///
/// Implementations must be `Send + Sync` so they can be shared across threads
/// when the global facade is used.
pub trait Logger: Send + Sync + 'static {
    /// Emit a log record at the given level.
    fn log(&self, level: LogLevel, message: &str);

    /// Flush any buffered records.
    fn flush(&self) {}

    /// Get the current minimum log level.
    fn min_level(&self) -> LogLevel {
        LogLevel::Info
    }

    /// Convenience: trace level.
    fn trace(&self, message: &str) {
        if self.min_level() <= LogLevel::Trace {
            self.log(LogLevel::Trace, message);
        }
    }
    /// Convenience: debug level.
    fn debug(&self, message: &str) {
        if self.min_level() <= LogLevel::Debug {
            self.log(LogLevel::Debug, message);
        }
    }
    /// Convenience: info level.
    fn info(&self, message: &str) {
        if self.min_level() <= LogLevel::Info {
            self.log(LogLevel::Info, message);
        }
    }
    /// Convenience: warn level.
    fn warn(&self, message: &str) {
        if self.min_level() <= LogLevel::Warn {
            self.log(LogLevel::Warn, message);
        }
    }
    /// Convenience: error level.
    fn error(&self, message: &str) {
        if self.min_level() <= LogLevel::Error {
            self.log(LogLevel::Error, message);
        }
    }
}

/// No-op logger used by default in tests and when logging is disabled.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopLogger;

impl Logger for NoopLogger {
    fn log(&self, _level: LogLevel, _message: &str) {
        // intentionally no-op
    }
}

enum LogMsg {
    Record {
        level: LogLevel,
        message: String,
        timestamp: String,
    },
    Flush(Sender<()>),
}

/// Async threaded logger for minimal overhead on the calling thread.
///
/// This logger spawns a background thread that prints logs to stdout.
/// It uses a bounded channel to prevent unbounded memory usage if the
/// writer cannot keep up.
#[derive(Debug, Clone)]
pub struct AsyncLogger {
    sender: Sender<LogMsg>,
    min_level: LogLevel,
}

impl AsyncLogger {
    pub fn new(min_level: LogLevel) -> Self {
        let (tx, rx) = bounded(2048);

        thread::spawn(move || {
            while let Ok(msg) = rx.recv() {
                match msg {
                    LogMsg::Record {
                        level,
                        message,
                        timestamp,
                    } => {
                        let json = serde_json::json!({
                            "ts": timestamp,
                            "level": level.as_str(),
                            "msg": message,
                        });
                        println!("{}", json);
                    }
                    LogMsg::Flush(ack_tx) => {
                        let _ = ack_tx.send(());
                    }
                }
            }
        });

        Self {
            sender: tx,
            min_level,
        }
    }
}

impl Default for AsyncLogger {
    fn default() -> Self {
        Self::new(LogLevel::Info)
    }
}

impl Logger for AsyncLogger {
    fn log(&self, level: LogLevel, message: &str) {
        if level < self.min_level {
            return;
        }
        let ts = chrono::Utc::now().to_rfc3339();
        let _ = self.sender.send(LogMsg::Record {
            level,
            message: message.to_string(),
            timestamp: ts,
        });
    }

    fn flush(&self) {
        let (ack_tx, ack_rx) = bounded(1);
        if self.sender.send(LogMsg::Flush(ack_tx)).is_ok() {
            let _ = ack_rx.recv();
        }
    }

    fn min_level(&self) -> LogLevel {
        self.min_level
    }
}
