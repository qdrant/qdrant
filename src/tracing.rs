#![allow(dead_code)] // `schema_generator` binary target produce warnings

use std::fmt::Write as _;
use std::str::FromStr as _;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;
use tokio::sync::RwLock;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{filter, fmt, registry, reload, Registry};

pub use self::config::{LoggerConfig, LoggerConfigDiff};

pub fn setup(config: &config::LoggerConfig) -> anyhow::Result<LoggerHandle> {
    let config = config.clone();

    let default_logger = default::new(&config.default);
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
    let reg = reg.with(tracing_tracy::TracyLayer::new().with_filter(
        tracing_subscriber::filter::filter_fn(|metadata| metadata.is_span()),
    ));

    tracing::subscriber::set_global_default(reg)?;
    tracing_log::LogTracer::init()?;

    Ok(logger_handle)
}

#[derive(Clone)]
pub struct LoggerHandle {
    config: Arc<RwLock<config::LoggerConfig>>,
    default: DefaultLoggerReloadHandle,
}

#[rustfmt::skip] // `rustfmt` formats this into unreadable single line
type DefaultLoggerReloadHandle<S = Registry> = reload::Handle<
    default::Logger<S>,
    S,
>;

impl LoggerHandle {
    pub fn new(config: config::LoggerConfig, default: DefaultLoggerReloadHandle) -> Self {
        Self {
            config: Arc::new(RwLock::new(config)),
            default,
        }
    }

    pub async fn get_config(&self) -> config::LoggerConfig {
        self.config.read().await.clone()
    }

    pub async fn update_config(&self, mut diff: config::LoggerConfigDiff) -> anyhow::Result<()> {
        let mut config = self.config.write().await;

        // `tracing-subscriber` does not support `reload`ing `Filtered` layers, so we *have to* use
        // `modify`. However, `modify` would *deadlock* if provided closure logs anything or produce
        // any `tracing` event.
        //
        // So, we structure `update_config` to only do an absolute minimum of changes and only use
        // the most trivial operations during `modify`, to guarantee we won't deadlock.
        //
        // See:
        // - https://docs.rs/tracing-subscriber/latest/tracing_subscriber/reload/struct.Handle.html#method.reload
        // - https://github.com/tokio-rs/tracing/issues/1629
        // - https://github.com/tokio-rs/tracing/pull/2657

        // Only update `config` field, if `diff` contains *new* value
        diff.filter(&config);

        // Parse `diff` and prepare `update` *outside* of `modify` call
        if let Some(update) = diff.default.prepare_update() {
            // Apply prepared `update` using trivial code that should never trigger a deadlock
            self.default.modify(move |logger| update.apply(logger))?;
        }

        // Update `config`
        config.default.update(diff.default);

        Ok(())
    }
}

pub mod config {
    use super::*;

    #[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize, Serialize)]
    #[serde(default)]
    pub struct LoggerConfig {
        #[serde(flatten)]
        pub default: default::Config,
    }

    impl LoggerConfig {
        pub fn with_top_level_directive(&mut self, log_level: Option<String>) -> &mut Self {
            if self.default.log_level.is_some() && log_level.is_some() {
                eprintln!(
                    "Both top-level `log_level` and `logger.log_level` config directives are used. \
                     `logger.log_level` takes priority, so top-level `log_level` will be ignored."
                );
            }

            self.default.log_level = self.default.log_level.take().or(log_level);
            self
        }

        pub fn update(&mut self, diff: LoggerConfigDiff) {
            self.default.update(diff.default);
        }
    }

    #[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize)]
    #[serde(default)]
    pub struct LoggerConfigDiff {
        #[serde(flatten)]
        pub default: default::ConfigDiff,
    }

    impl LoggerConfigDiff {
        pub fn filter(&mut self, config: &LoggerConfig) {
            self.default.filter(&config.default);
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, SmartDefault)]
    #[serde(from = "helpers::SpanEvents", into = "helpers::SpanEvents")]
    pub struct SpanEvents {
        #[default(fmt::format::FmtSpan::NONE)]
        events: fmt::format::FmtSpan,
    }

    impl From<fmt::format::FmtSpan> for SpanEvents {
        fn from(events: fmt::format::FmtSpan) -> Self {
            Self { events }
        }
    }

    impl From<SpanEvents> for fmt::format::FmtSpan {
        fn from(events: SpanEvents) -> Self {
            events.events
        }
    }

    #[derive(Copy, Clone, Debug, Eq, PartialEq, Deserialize, Serialize, SmartDefault)]
    #[serde(from = "helpers::Color", into = "helpers::Color")]
    pub enum Color {
        #[default]
        Auto,
        Enable,
        Disable,
    }

    impl Color {
        pub fn to_bool(self) -> bool {
            match self {
                Self::Auto => colored::control::SHOULD_COLORIZE.should_colorize(),
                Self::Enable => true,
                Self::Disable => false,
            }
        }
    }

    mod helpers {
        use super::*;

        #[derive(Clone, Debug, Deserialize, Serialize)]
        #[serde(untagged)]
        pub enum SpanEvents {
            Some(Vec<SpanEvent>),
            None(NoneTag),
            Null,
        }

        impl SpanEvents {
            pub fn from_fmt_span(events: fmt::format::FmtSpan) -> Self {
                let events = SpanEvent::from_fmt_span(events);

                if !events.is_empty() {
                    Self::Some(events)
                } else {
                    Self::None(NoneTag::None)
                }
            }

            pub fn to_fmt_span(&self) -> fmt::format::FmtSpan {
                self.as_slice()
                    .iter()
                    .copied()
                    .fold(fmt::format::FmtSpan::NONE, |events, event| {
                        events | event.to_fmt_span()
                    })
            }

            fn as_slice(&self) -> &[SpanEvent] {
                match self {
                    SpanEvents::Some(events) => events,
                    _ => &[],
                }
            }
        }

        impl From<super::SpanEvents> for SpanEvents {
            fn from(events: super::SpanEvents) -> Self {
                Self::from_fmt_span(events.into())
            }
        }

        impl From<SpanEvents> for super::SpanEvents {
            fn from(events: SpanEvents) -> Self {
                events.to_fmt_span().into()
            }
        }

        #[derive(Copy, Clone, Debug, Deserialize, Serialize)]
        #[serde(rename_all = "lowercase")]
        pub enum SpanEvent {
            New,
            Enter,
            Exit,
            Close,
        }

        impl SpanEvent {
            pub fn from_fmt_span(events: fmt::format::FmtSpan) -> Vec<Self> {
                const EVENTS: &[SpanEvent] = &[
                    SpanEvent::New,
                    SpanEvent::Enter,
                    SpanEvent::Exit,
                    SpanEvent::Close,
                ];

                EVENTS
                    .iter()
                    .copied()
                    .filter(|event| events.clone() & event.to_fmt_span() == event.to_fmt_span())
                    .collect()
            }

            pub fn to_fmt_span(self) -> fmt::format::FmtSpan {
                match self {
                    SpanEvent::New => fmt::format::FmtSpan::NEW,
                    SpanEvent::Enter => fmt::format::FmtSpan::ENTER,
                    SpanEvent::Exit => fmt::format::FmtSpan::EXIT,
                    SpanEvent::Close => fmt::format::FmtSpan::CLOSE,
                }
            }
        }

        #[derive(Copy, Clone, Debug, Deserialize, Serialize)]
        #[serde(rename_all = "lowercase")]
        pub enum NoneTag {
            None,
        }

        #[derive(Copy, Clone, Debug, Deserialize, Serialize)]
        #[serde(untagged)]
        pub enum Color {
            Auto(AutoTag),
            Bool(bool),
        }

        impl From<super::Color> for Color {
            fn from(color: super::Color) -> Self {
                match color {
                    super::Color::Auto => Self::Auto(AutoTag::Auto),
                    super::Color::Enable => Self::Bool(true),
                    super::Color::Disable => Self::Bool(false),
                }
            }
        }

        impl From<Color> for super::Color {
            fn from(color: Color) -> Self {
                match color {
                    Color::Auto(_) => Self::Auto,
                    Color::Bool(true) => Self::Enable,
                    Color::Bool(false) => Self::Disable,
                }
            }
        }

        #[derive(Copy, Clone, Debug, Deserialize, Serialize)]
        #[serde(rename_all = "lowercase")]
        pub enum AutoTag {
            Auto,
        }
    }
}

mod default {
    use super::*;

    #[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize, Serialize)]
    #[serde(default)]
    pub struct Config {
        pub log_level: Option<String>,
        pub span_events: config::SpanEvents,
        pub color: config::Color,
    }

    impl Config {
        pub fn update(&mut self, diff: ConfigDiff) {
            if let Some(log_level) = diff.log_level {
                self.log_level = log_level;
            }

            if let Some(span_events) = diff.span_events {
                self.span_events = span_events;
            }

            if let Some(color) = diff.color {
                self.color = color;
            }
        }
    }

    #[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize)]
    pub struct ConfigDiff {
        // Distinguish between unspecified field (`None`) and explicit `null` (`Some(None)`)
        // See https://github.com/serde-rs/serde/issues/984#issuecomment-314143738
        #[serde(default, deserialize_with = "deserialize_some")]
        pub log_level: Option<Option<String>>,
        pub span_events: Option<config::SpanEvents>,
        pub color: Option<config::Color>,
    }

    impl ConfigDiff {
        pub fn has_changes(&self) -> bool {
            self.log_level.is_some() || self.span_events.is_some() || self.color.is_some()
        }

        pub fn filter(&mut self, config: &Config) {
            if self.log_level.as_ref() == Some(&config.log_level) {
                self.log_level = None;
            }

            if self.span_events.as_ref() == Some(&config.span_events) {
                self.span_events = None;
            }

            if self.color.as_ref() == Some(&config.color) {
                self.color = None;
            }
        }

        pub fn prepare_update(&self) -> Option<Update> {
            if self.has_changes() {
                Some(Update::from_diff(self))
            } else {
                None
            }
        }
    }

    #[rustfmt::skip] // `rustfmt` formats this into unreadable single line
    pub type Logger<S> = filter::Filtered<
        Option<fmt::Layer<S>>,
        filter::EnvFilter,
        S,
    >;

    pub fn new<S>(config: &Config) -> Logger<S>
    where
        S: tracing::Subscriber + for<'span> registry::LookupSpan<'span>,
    {
        let layer = fmt::Layer::default()
            .with_ansi(config.color.to_bool())
            .with_span_events(config.span_events.clone().into());

        let filter = filter(config.log_level.as_deref().unwrap_or(""));

        Some(layer).with_filter(filter)
    }

    #[derive(Debug, Default)]
    pub struct Update {
        filter: Option<filter::EnvFilter>,
        span_events: Option<fmt::format::FmtSpan>,
        ansi: Option<bool>,
    }

    impl Update {
        pub fn from_diff(diff: &ConfigDiff) -> Self {
            let mut update = Self::default();
            update.prepare(diff);
            update
        }

        fn prepare(&mut self, diff: &ConfigDiff) {
            if let Some(log_level) = &diff.log_level {
                self.filter = Some(filter(log_level.as_deref().unwrap_or("")));
            }

            if let Some(span_events) = diff.span_events.clone() {
                self.span_events = Some(span_events.into())
            }

            if let Some(color) = diff.color {
                self.ansi = Some(color.to_bool());
            }
        }

        // `apply` should *never* log anything or produce any `tracing` events!
        pub fn apply<S>(self, logger: &mut Logger<S>) {
            if let Some(filter) = self.filter {
                *logger.filter_mut() = filter;
            }

            if let Some(span_events) = self.span_events {
                let mut layer = logger.inner_mut().take().expect("valid logger state");
                layer = layer.with_span_events(span_events);
                *logger.inner_mut() = Some(layer);
            }

            if let Some(ansi) = self.ansi {
                logger
                    .inner_mut()
                    .as_mut()
                    .expect("valid logger state")
                    .set_ansi(ansi);
            }
        }
    }

    fn filter(user_filters: &str) -> filter::EnvFilter {
        const DEFAULT_LOG_LEVEL: log::LevelFilter = log::LevelFilter::Info;

        const DEFAULT_FILTERS: &[(&str, log::LevelFilter)] = &[
            ("hyper", log::LevelFilter::Info),
            ("h2", log::LevelFilter::Error),
            ("tower", log::LevelFilter::Warn),
            ("rustls", log::LevelFilter::Info),
            ("wal", log::LevelFilter::Warn),
            ("raft", log::LevelFilter::Warn),
        ];

        super::filter(DEFAULT_LOG_LEVEL, DEFAULT_FILTERS, user_filters)
    }
}

fn filter<'a>(
    default_log_level: log::LevelFilter,
    default_filters: impl IntoIterator<Item = &'a (&'a str, log::LevelFilter)>,
    user_filters: &str,
) -> filter::EnvFilter {
    let mut filter = String::new();

    let user_log_level = user_filters
        .rsplit(',')
        .find_map(|dir| log::LevelFilter::from_str(dir).ok());

    if user_log_level.is_none() {
        write!(&mut filter, "{default_log_level}").unwrap(); // Writing into `String` never fails
    }

    for &(target, log_level) in default_filters {
        if user_log_level.unwrap_or(default_log_level) > log_level {
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

// Helper to distinguish between unspecified field and explicit `null`
// See https://github.com/serde-rs/serde/issues/984#issuecomment-314143738
fn deserialize_some<'de, T, D>(deserializer: D) -> Result<Option<T>, D::Error>
where
    T: serde::Deserialize<'de>,
    D: serde::Deserializer<'de>,
{
    Deserialize::deserialize(deserializer).map(Some)
}

#[cfg(test)]
mod test {
    use serde_json::json;

    use super::*;

    #[test]
    fn deseriailze_logger_config() {
        let config = deserialize_config(config());

        let expected = LoggerConfig {
            default: default::Config {
                log_level: Some("debug".into()),
                span_events: (fmt::format::FmtSpan::NEW | fmt::format::FmtSpan::CLOSE).into(),
                color: config::Color::Enable,
            },
        };

        assert_eq!(config, expected);
    }

    #[test]
    fn deserialize_empty_config() {
        let config = deserialize_config(empty_config());
        assert_eq!(config, LoggerConfig::default());
    }

    #[test]
    fn deserialize_logger_config_diff() {
        let diff = deserialize_diff(config());

        let expected = LoggerConfigDiff {
            default: default::ConfigDiff {
                log_level: Some(Some("debug".into())),
                span_events: Some((fmt::format::FmtSpan::NEW | fmt::format::FmtSpan::CLOSE).into()),
                color: Some(config::Color::Enable),
            },
        };

        assert_eq!(diff, expected);
    }

    #[test]
    fn deserialize_empty_diff() {
        let diff = deserialize_diff(empty_config());
        assert_eq!(diff, LoggerConfigDiff::default());
    }

    #[test]
    fn deserialize_diff_with_explicit_nulls() {
        let diff = deserialize_diff(json!({
            "log_level": null,
            "span_events": null,
            "color": null,
        }));

        let expected = LoggerConfigDiff {
            default: default::ConfigDiff {
                log_level: Some(None),
                ..Default::default()
            },
        };

        assert_eq!(diff, expected);
    }

    fn deserialize_config(json: serde_json::Value) -> LoggerConfig {
        serde_json::from_value(json).unwrap()
    }

    fn deserialize_diff(json: serde_json::Value) -> LoggerConfigDiff {
        serde_json::from_value(json).unwrap()
    }

    fn config() -> serde_json::Value {
        json!({
            "log_level": "debug",
            "span_events": ["new", "close"],
            "color": true,
        })
    }

    fn empty_config() -> serde_json::Value {
        json!({})
    }
}
