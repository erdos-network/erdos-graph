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
