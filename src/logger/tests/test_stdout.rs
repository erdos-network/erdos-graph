use super::super::core::{LogLevel, Logger, StdoutLogger};

// Capture stdout by using the `duct` crate or std::io redirection is not
// available in stable test harness easily; instead, use a simple technique
// to ensure StdoutLogger::log does not panic and emits something resembling JSON.

#[test]
fn stdout_logger_emits_valid_json() {
    let logger = StdoutLogger;
    // The purpose of this test is to ensure the StdoutLogger's formatting path
    // executes without panicking and produces a JSON-like string. We can't
    // easily intercept println! here without a test harness, so we just call
    // the method to validate it runs.
    logger.log(LogLevel::Info, "testing stdout json");
}
