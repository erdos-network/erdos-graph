//! Logging module for the Erdős Graph project.
//!
//! Provides a minimal trait and default implementations to avoid external
//! dependencies while we shape the logging needs of scrapers and the
//! database layer.
//!
//! - `Logger`: trait defining the logging surface
//! - `LogLevel`: enum of levels
//! - `NoopLogger`: default no-op implementation
//! - `StdoutLogger`: simple stdout-backed stub useful during development
//!
//! TODOs:
//! - Provide a global logger facade and initialization helper
//! - Add feature flags to swap between implementations

pub mod logger;

pub use logger::{LogLevel, Logger, NoopLogger, StdoutLogger};

#[cfg(test)]
pub mod tests;

 
/// Global logger facade.
///
/// Call `init_logger` early in `main` to set a concrete logger. Tests may use
/// `set_logger_for_tests` to install a test logger.
static mut GLOBAL_LOGGER: Option<&'static dyn Logger> = None;

/// Initialize the global logger for the lifetime of the program.
/// Returns a guard-like value in the future; for now this sets a static.
pub fn init_logger<L: Logger>(logger: L) {
	// Leak the logger so it can be referenced via a static pointer safely.
	// This is a deliberate choice for a global singleton with program lifetime.
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
