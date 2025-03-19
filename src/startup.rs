//! Contains a collection of functions that are called at the start of the program.

use std::backtrace::Backtrace;
use std::panic;
use std::path::PathBuf;

use crate::common::error_reporting::ErrorReporter;
use crate::settings::Settings;

fn get_init_file_path(settings: &Settings) -> PathBuf {
    std::env::var("QDRANT_INIT_FILE_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(&settings.init_file_path))
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
/// This file is used to check if the server has been successfully started before potential kill.
pub fn touch_started_file_indicator(settings: &Settings) {
    if let Err(err) = std::fs::write(get_init_file_path(settings), "") {
        log::warn!("Failed to create init file indicator: {}", err);
    }
}

/// Removes a file that indicates that the server has been started.
/// Use before server initialization to avoid false positives.
pub fn remove_started_file_indicator(settings: &Settings) {
    let path = get_init_file_path(settings);
    if path.exists() {
        if let Err(err) = std::fs::remove_file(path) {
            log::warn!("Failed to remove init file indicator: {}", err);
        }
    }
}
