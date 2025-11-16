//! Custom logging primitives for the Erdős Graph project.
//!
//! This module provides a tiny, purposely minimal logging surface used by the
//! codebase. The goals are to remain dependency-light while offering a
//! consistently-typed `LogLevel` and a `Logger` trait that is easy to implement
//! in tests and small binaries. For production-grade structured logging or
//! filtering, replace or wrap these primitives with a more featureful logger
//! (for example `tracing` or `log` + `env_logger`).
//!
//! Responsibilities:
//! - Provide a lightweight `Logger` trait used across crates
//! - Offer a baseline no-op implementation for tests and benchmarking
//! - Centralize log level semantics without pulling a full logging framework
//!
//! Notes on thread-safety and bounds:
//! Implementors of `Logger` must be `Send + Sync + 'static` so the trait
//! objects can be stored in global contexts and shared between threads. If you
//! need dynamically-swappable global loggers, add synchronization around the
//! global facade and consider weaker bounds.

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
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
/// Implementors must satisfy `Send + Sync + 'static` which allows boxed trait
/// objects to be stored in globals and referenced across threads. The core
/// requirement is a single `log` method; convenience helpers like `info` and
/// `warn` are implemented in terms of `log` so tests can provide a tiny
/// implementation without implementing all helpers.
pub trait Logger: Send + Sync + 'static {
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
///
/// `NoopLogger` implements `Logger` but drops all messages. It's useful in
/// unit tests where you want to assert behavior without emitting output. The
/// type is `Copy + Default` to make it lightweight to pass around.
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
/// It writes a compact JSON object to stdout with a timestamp, level and message.
/// The implementation intentionally uses `chrono` and `serde_json` for simplicity
/// and should be replaced or augmented for production use (filtering, batching,
/// and non-blocking IO are not supported).
#[derive(Debug, Default, Clone, Copy)]
pub struct StdoutLogger;

impl Logger for StdoutLogger {
    fn log(&self, level: LogLevel, message: &str) {
        // Emit a small JSON object to stdout so logs are easier to parse
        // by structured log collectors. Keep the shape minimal for now.
        let ts = chrono::Utc::now().to_rfc3339();
        // Example: {"ts":"...","level":"INFO","msg":"..."}
        let json = serde_json::json!({
            "ts": ts,
            "level": level.as_str(),
            "msg": message,
        });
        println!("{}", json);
    }

    fn flush(&self) {
        // stdout is line-buffered; nothing to do for the stub
    }
}
