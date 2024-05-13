#![allow(dead_code)] // `schema_generator` binary target produce warnings

pub mod config;
pub mod default;
pub mod handle;

#[cfg(test)]
mod test;

use std::fmt::Write as _;
use std::str::FromStr as _;

use tracing_subscriber::prelude::*;
use tracing_subscriber::{filter, reload};

pub use self::config::LoggerConfig;
pub use self::handle::LoggerHandle;

const DEFAULT_LOG_LEVEL: log::LevelFilter = log::LevelFilter::Info;

const DEFAULT_FILTERS: &[(&str, log::LevelFilter)] = &[
    ("hyper", log::LevelFilter::Info),
    ("h2", log::LevelFilter::Error),
    ("tower", log::LevelFilter::Warn),
    ("rustls", log::LevelFilter::Info),
    ("wal", log::LevelFilter::Warn),
    ("raft", log::LevelFilter::Warn),
];

pub fn setup(config: config::LoggerConfig) -> anyhow::Result<LoggerHandle> {
    let default_logger = default::new_logger(&config.default);
    let (default_logger, default_logger_handle) = reload::Layer::new(default_logger);
    let reg = tracing_subscriber::registry().with(default_logger);

    let logger_handle = LoggerHandle::new(config, default_logger_handle);

    // Use `console` or `console-subscriber` feature to enable `console-subscriber`
    //
    // Note, that `console-subscriber` requires manually enabling
    // `--cfg tokio_unstable` rust flags during compilation!
    //
    // Otherwise `console_subscriber::spawn` call panics!
    //
    // See https://docs.rs/tokio/latest/tokio/#unstable-features
    #[cfg(all(feature = "console-subscriber", tokio_unstable))]
    let reg = reg.with(console_subscriber::spawn());

    #[cfg(all(feature = "console-subscriber", not(tokio_unstable)))]
    eprintln!(
        "`console-subscriber` requires manually enabling \
         `--cfg tokio_unstable` rust flags during compilation!"
    );

    // Use `tracy` or `tracing-tracy` feature to enable `tracing-tracy`
    #[cfg(feature = "tracing-tracy")]
    let reg = reg.with(
        tracing_tracy::TracyLayer::new(tracing_tracy::DefaultConfig::default()).with_filter(
            tracing_subscriber::filter::filter_fn(|metadata| metadata.is_span()),
        ),
    );

    tracing::subscriber::set_global_default(reg)?;
    tracing_log::LogTracer::init()?;

    Ok(logger_handle)
}

fn filter(user_filters: &str) -> filter::EnvFilter {
    let mut filter = String::new();

    let user_log_level = user_filters
        .rsplit(',')
        .find_map(|dir| log::LevelFilter::from_str(dir).ok());

    if user_log_level.is_none() {
        write!(&mut filter, "{DEFAULT_LOG_LEVEL}").unwrap(); // Writing into `String` never fails
    }

    for &(target, log_level) in DEFAULT_FILTERS {
        if user_log_level.unwrap_or(DEFAULT_LOG_LEVEL) > log_level {
            let comma = if filter.is_empty() { "" } else { "," };
            write!(&mut filter, "{comma}{target}={log_level}").unwrap(); // Writing into `String` never fails
        }
    }

    let comma = if filter.is_empty() { "" } else { "," };
    write!(&mut filter, "{comma}{user_filters}").unwrap(); // Writing into `String` never fails

    filter::EnvFilter::builder()
        .with_regex(false)
        .parse_lossy(filter)
}
