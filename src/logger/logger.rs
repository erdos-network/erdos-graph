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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

/// Minimal logger interface used throughout the project.
///
/// Implementors should be cheap to clone and thread-safe if used in multithreaded contexts.
/// For stubs, implement `log` and optionally override convenience methods.
/// TODO: consider adding `Send + Sync` bounds once we finalize the threading model.
pub trait Logger {
    /// Emit a log record at the given level.
    fn log(&self, level: LogLevel, message: &str);

    /// Flush any buffered records.
    fn flush(&self) {}

    /// Convenience: trace level.
    fn trace(&self, message: &str) {
        self.log(LogLevel::Trace, message);
    }
    /// Convenience: debug level.
    fn debug(&self, message: &str) {
        self.log(LogLevel::Debug, message);
    }
    /// Convenience: info level.
    fn info(&self, message: &str) {
        self.log(LogLevel::Info, message);
    }
    /// Convenience: warn level.
    fn warn(&self, message: &str) {
        self.log(LogLevel::Warn, message);
    }
    /// Convenience: error level.
    fn error(&self, message: &str) {
        self.log(LogLevel::Error, message);
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

/// Very small stdout logger for quick debugging.
///
/// NOTE: This is a stub and formatting is intentionally minimal.
/// TODO: Add timestamps and consistent formatting; gate by minimum level.
#[derive(Debug, Default, Clone, Copy)]
pub struct StdoutLogger;

impl Logger for StdoutLogger {
    fn log(&self, level: LogLevel, message: &str) {
        println!("[{:?}] {}", level, message);
    }

    fn flush(&self) {
        // stdout is line-buffered; nothing to do for the stub
    }
}
