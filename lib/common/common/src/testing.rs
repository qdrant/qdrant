/// True if the current test is running within a separate process.
///
/// Used to skip non-hermetic tests, e.g., tests that modify global/env vars.
///
/// - On CI we use `cargo nextest` which isolates each test.
/// - Tests under `cargo test` are not isolated.
pub fn is_process_isolated() -> bool {
    std::env::var_os("NEXTEST_EXECUTION_MODE").is_some_and(|v| v == "process-per-test")
}
