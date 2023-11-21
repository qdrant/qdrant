use std::fmt::Write as _;
use std::str::FromStr as _;

use colored::control::ShouldColorize;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{filter, fmt};

const DEFAULT_LOG_LEVEL: log::LevelFilter = log::LevelFilter::Info;

const DEFAULT_FILTERS: &[(&str, log::LevelFilter)] = &[
    ("hyper", log::LevelFilter::Info),
    ("h2", log::LevelFilter::Error),
    ("tower", log::LevelFilter::Warn),
    ("rustls", log::LevelFilter::Info),
    ("wal", log::LevelFilter::Warn),
    ("raft", log::LevelFilter::Warn),
];

pub fn setup(user_filters: &str) -> anyhow::Result<()> {
    tracing_log::LogTracer::init()?;

    let mut filters = DEFAULT_LOG_LEVEL.to_string();

    let user_log_level = user_filters
        .rsplit(',')
        .find_map(|dir| log::LevelFilter::from_str(dir).ok());

    for (target, log_level) in DEFAULT_FILTERS.iter().copied() {
        if user_log_level.unwrap_or(DEFAULT_LOG_LEVEL) > log_level {
            write!(&mut filters, ",{target}={log_level}").unwrap(); // Writing into `String` never fails
        }
    }

    write!(&mut filters, ",{user_filters}").unwrap(); // Writing into `String` never fails

    let reg = tracing_subscriber::registry().with(
        fmt::layer()
            // Only use ANSI if we should colorize
            .with_ansi(ShouldColorize::from_env().should_colorize())
            .with_span_events(fmt::format::FmtSpan::NEW)
            .with_filter(
                filter::EnvFilter::builder()
                    .with_regex(false)
                    .parse_lossy(filters),
            ),
    );

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
    let reg = reg.with(tracing_tracy::TracyLayer::new().with_filter(
        tracing_subscriber::filter::filter_fn(|metadata| metadata.is_span()),
    ));

    tracing::subscriber::set_global_default(reg)?;

    Ok(())
}
