//! Top-level logger exports and a small global facade.
//!
//! This module re-exports the core logging primitives and exposes a simple
//! global facade for programs that prefer a process-wide logger instance.
//!
//! - `Logger`: trait defining the logging surface
//! - `LogLevel`: enum of levels
//! - `NoopLogger`: default no-op implementation
//! - `StdoutLogger`: simple stdout-backed stub useful during development
//!
//! ```rust,no_run
//! use erdos_graph::logger;
//! logger::init_logger(logger::StdoutLogger);
//! logger::info("app started");
//! ```

pub mod core;

pub use core::{LogLevel, Logger, NoopLogger, StdoutLogger};

/// Global logger facade.
/// A process-wide logger reference used by the convenience facade below.
///
/// This is intentionally a very small global facade to make tests and simple
/// binaries ergonomic. It stores a leaked, boxed logger as a static pointer so
/// the facade can hand out a reference with a 'static lifetime. The implementation
/// intentionally avoids interior mutability and complex lifecycle management —
/// callers should call `init_logger` once early in `main` (or `set_logger_for_tests`
/// from test setup) and then use the convenience helpers like `info` and `error`.
///
/// Safety: access to `GLOBAL_LOGGER` uses `unsafe` but only to read an immutable
/// reference; initialization leaks the logger to give it a program-static lifetime.
/// This pattern is simple and acceptable for a global, program-lifetime singleton
/// used by small utilities. If you need a resettable or swappable logger, replace
/// this facade with a synchronization primitive (Mutex/RwLock) and stronger bounds.
static mut GLOBAL_LOGGER: Option<&'static dyn Logger> = None;

/// Initialize the global logger for the lifetime of the program.
/// Returns a guard-like value in the future; for now this sets a static.
pub fn init_logger<L: Logger>(logger: L) {
    // Leak the logger so it can be referenced via a static pointer safely.
    let boxed: Box<dyn Logger> = Box::new(logger);
    let leaked: &'static dyn Logger = Box::leak(boxed);
    unsafe {
		GLOBAL_LOGGER = Some(leaked);
	}
}

/// For tests: set a logger that will be used by the global facade.
pub fn set_logger_for_tests<L: Logger>(logger: L) {
    init_logger(logger);
}

/// Log using the global logger if set, otherwise no-op.
pub fn log(level: LogLevel, message: &str) {
    unsafe {
        if let Some(logger) = GLOBAL_LOGGER {
            logger.log(level, message);
        }
    }
}

/// Convenience functions
pub fn info(msg: &str) {
    log(LogLevel::Info, msg);
}

pub fn debug(msg: &str) {
    log(LogLevel::Debug, msg);
}

pub fn error(msg: &str) {
    log(LogLevel::Error, msg);
}

#[cfg(test)]
mod tests {

}