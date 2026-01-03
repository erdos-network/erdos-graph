use super::super::logger::{init_logger, log, LogLevel, NoopLogger, AsyncLogger};

#[test]
fn global_facade_noop_by_default() {
    // ensure calling log without init does not panic (no-op)
    log(LogLevel::Info, "should not panic");
}

#[test]
fn init_sets_global_logger() {
    init_logger(NoopLogger);
    // after init, global log should use NoopLogger and not panic
    log(LogLevel::Info, "using noop");
}

#[test]
fn async_logger_emits_json() {
    // This test is a smoke check to ensure AsyncLogger formats without panicking.
    init_logger(AsyncLogger::new());
    log(LogLevel::Warn, "this is a warning");
}
