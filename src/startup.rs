//! Contains a collection of functions that are called at the start of the program.

use log::LevelFilter;

pub fn setup_logger(log_level: &str) {
    let is_info = log_level.to_ascii_uppercase() == "INFO";
    let mut log_builder = env_logger::Builder::new();

    log_builder
        // Timestamp in millis
        .format_timestamp_millis()
        // Parse user defined log level configuration
        .parse_filters(log_level)
        // h2 is very verbose and we have many network operations,
        // so it is limited to only errors
        .filter_module("h2", LevelFilter::Error)
        .filter_module("tower", LevelFilter::Warn);

    if is_info {
        // Additionally filter verbose modules if no extended logging configuration is provided
        log_builder
            .filter_module("wal", LevelFilter::Warn)
            .filter_module("raft::raft", LevelFilter::Warn);
    };

    log_builder.init();
}
