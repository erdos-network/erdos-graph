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

use std::any::Any;
use std::os::fd::{AsFd, AsRawFd};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

/// Returns a short string representation suitable for logs.
impl LogLevel {
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
/// Must be `Send + Sync + 'static` for global usage.
/// The core
/// requirement is a single `log` method; convenience helpers like `info` and
/// `warn` are implemented in terms of `log` so tests can provide a tiny
/// implementation without implementing all helpers.
pub trait Logger: Send + Sync + 'static {
    /// Emit a log record at the given level.
    fn log(&self, _level: LogLevel, _message: &str) {}

    /// Flush any buffered records.
    fn flush(&self) {}

    /// Convenience methods
    fn trace(&self, message: &str) {
        self.log(LogLevel::Trace, message);
    }
    fn debug(&self, message: &str) {
        self.log(LogLevel::Debug, message);
    }
    fn info(&self, message: &str) {
        self.log(LogLevel::Info, message);
    }
    fn warn(&self, message: &str) {
        self.log(LogLevel::Warn, message);
    }
    fn error(&self, message: &str) {
        self.log(LogLevel::Error, message);
    }

    /// Downcasting helpers
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
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
        // intentionally do nothing
    }
    fn as_any(&self) -> &dyn Any { self }
    fn as_any_mut(&mut self) -> &mut dyn Any { self }
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
    // Emit a small JSON object to stdout so logs are easier to parse
    // by structured log collectors. Keep the shape minimal for now.
    // Example: {"ts":"...","level":"INFO","msg":"..."}
    fn log(&self, level: LogLevel, message: &str) {
        let ts = chrono::Utc::now().to_rfc3339();
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
    fn as_any(&self) -> &dyn Any { self }
    fn as_any_mut(&mut self) -> &mut dyn Any { self }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========== LogLevel tests ==========

    #[test]
    fn test_loglevel_as_str_success() {
        assert_eq!(LogLevel::Trace.as_str(), "TRACE");
        assert_eq!(LogLevel::Debug.as_str(), "DEBUG");
        assert_eq!(LogLevel::Info.as_str(), "INFO");
        assert_eq!(LogLevel::Warn.as_str(), "WARN");
        assert_eq!(LogLevel::Error.as_str(), "ERROR");
    }

    #[test]
    fn test_loglevel_ordering_is_monotonic() {
        assert!(LogLevel::Trace < LogLevel::Debug);
        assert!(LogLevel::Debug < LogLevel::Info);
        assert!(LogLevel::Info < LogLevel::Warn);
        assert!(LogLevel::Warn < LogLevel::Error);
    }

    // edge case: ensure different variants are actually unequal
    #[test]
    fn test_loglevel_not_equal() {
        assert_ne!(LogLevel::Info, LogLevel::Error);
    }

    // ========== NoopLogger tests ==========

    #[test]
    fn test_nooplogger_accepts_all_levels() {
        let logger = NoopLogger;
        logger.trace("trace");
        logger.debug("debug");
        logger.info("info");
        logger.warn("warn");
        logger.error("error");
        logger.flush();
        assert!(true);
    }

    // ========== StdoutLogger tests ==========

    fn capture_stdout<F: FnOnce()>(f: F) -> String {
        use std::io::Read;

        let mut reader = tempfile::tempfile().unwrap();
        let writer = reader.try_clone().unwrap();

        let saved = unsafe { libc::dup(libc::STDOUT_FILENO) };
        unsafe { libc::dup2(writer.as_fd().as_raw_fd(), libc::STDOUT_FILENO) };

        f();

        unsafe { libc::dup2(saved, libc::STDOUT_FILENO) };
        unsafe { libc::close(saved) };

        let mut output = String::new();
        reader.read_to_string(&mut output).unwrap();
        output
    }

    #[test]
    fn test_stdoutlogger_prints_json_success() {
        let out = capture_stdout(|| {
            StdoutLogger.log(LogLevel::Info, "hello");
        });

        assert!(out.contains("\"level\":\"INFO\""));
        assert!(out.contains("\"msg\":\"hello\""));
    }

    #[test]
    fn test_stdoutlogger_prints_different_level() {
        let out = capture_stdout(|| {
            StdoutLogger.log(LogLevel::Error, "bad");
        });

        assert!(out.contains("\"level\":\"ERROR\""));
        assert!(out.contains("\"msg\":\"bad\""));
    }

    #[test]
    fn test_stdoutlogger_flush_noop() {
        StdoutLogger.flush();
        assert!(true);
    }

    // ========== Logger trait default methods ==========

    #[derive(Default)]
    struct TestLogger {
        pub entries: std::sync::Mutex<Vec<(LogLevel, String)>>,
    }

    impl Logger for TestLogger {
        fn log(&self, level: LogLevel, msg: &str) {
            self.entries.lock().unwrap().push((level, msg.to_string()));
        }
        fn as_any(&self) -> &dyn Any { self }
        fn as_any_mut(&mut self) -> &mut dyn Any { self }
    }

    #[test]
    fn test_trait_default_methods_success() {
        let logger = TestLogger::default();
        logger.info("info");
        logger.warn("warn");

        let entries = logger.entries.lock().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, LogLevel::Info);
        assert_eq!(entries[0].1, "info");
    }

    // Edge case: empty message
    #[test]
    fn test_trait_handles_empty_message() {
        let logger = TestLogger::default();
        logger.info("");

        let entries = logger.entries.lock().unwrap();
        assert_eq!(entries[0].1, "");
    }
}