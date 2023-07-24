//! Contains a collection of functions that are called at the start of the program.

use std::backtrace::Backtrace;
use std::panic;

use log::LevelFilter;

use crate::common::error_reporting::ErrorReporter;

const DEFAULT_INITIALIZED_FILE: &str = ".qdrant-initialized";

fn get_init_file_path() -> String {
    std::env::var("QDRANT_INIT_FILE_PATH").unwrap_or_else(|_| DEFAULT_INITIALIZED_FILE.to_owned())
}

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

pub fn setup_panic_hook(reporting_enabled: bool, reporting_id: String) {
    panic::set_hook(Box::new(move |panic_info| {
        let backtrace = Backtrace::force_capture().to_string();
        let loc = if let Some(loc) = panic_info.location() {
            format!(" in file {} at line {}", loc.file(), loc.line())
        } else {
            String::new()
        };
        let message = if let Some(s) = panic_info.payload().downcast_ref::<&str>() {
            s
        } else if let Some(s) = panic_info.payload().downcast_ref::<String>() {
            s
        } else {
            "Payload not captured as it is not a string."
        };

        log::error!("Panic backtrace: \n{}", backtrace);
        log::error!("Panic occurred{loc}: {message}");

        if reporting_enabled {
            ErrorReporter::report(message, &reporting_id, Some(&loc));
        }
    }));
}

/// Creates a file that indicates that the server has been started.
/// This file is used to check if the server has been been successfully started before potential kill.
pub fn touch_started_file_indicator() {
    if let Err(err) = std::fs::write(get_init_file_path(), "") {
        log::warn!("Failed to create init file indicator: {}", err);
    }
}

/// Removes a file that indicates that the server has been started.
/// Use before server initialization to avoid false positives.
pub fn remove_started_file_indicator() {
    let path = get_init_file_path();
    if std::path::Path::new(&path).exists() {
        if let Err(err) = std::fs::remove_file(path) {
            log::warn!("Failed to remove init file indicator: {}", err);
        }
    }
}
