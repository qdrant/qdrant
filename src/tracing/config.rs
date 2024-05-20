use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use tracing_subscriber::fmt;

use super::*;

#[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize, Serialize)]
#[serde(default)]
pub struct LoggerConfig {
    #[serde(flatten)]
    pub default: default::Config,
    #[serde(default)]
    pub on_disk: on_disk::Config,
}

impl LoggerConfig {
    pub fn with_top_level_directive(&self, log_level: Option<String>) -> Self {
        let mut logger_config = self.clone();

        if logger_config.default.log_level.is_some() && log_level.is_some() {
            eprintln!(
                "Both top-level `log_level` and `logger.log_level` config directives are used. \
                 `logger.log_level` takes priority, so top-level `log_level` will be ignored."
            );
        }

        logger_config.default.log_level = logger_config.default.log_level.take().or(log_level);
        logger_config
    }

    pub fn merge(&mut self, other: Self) {
        self.default.merge(other.default);
        self.on_disk.merge(other.on_disk);
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Deserialize, Serialize, Hash)]
#[serde(rename_all = "lowercase")]
pub enum SpanEvent {
    New,
    Enter,
    Exit,
    Close,
}

impl SpanEvent {
    pub fn unwrap_or_default_config(events: &Option<HashSet<Self>>) -> fmt::format::FmtSpan {
        Self::into_fmt_span(events.as_ref().unwrap_or(&HashSet::new()).iter().copied())
    }

    pub fn into_fmt_span(events: impl IntoIterator<Item = Self>) -> fmt::format::FmtSpan {
        events
            .into_iter()
            .fold(fmt::format::FmtSpan::NONE, |events, event| {
                events | event.into()
            })
    }
}

impl From<SpanEvent> for fmt::format::FmtSpan {
    fn from(event: SpanEvent) -> Self {
        match event {
            SpanEvent::New => fmt::format::FmtSpan::NEW,
            SpanEvent::Enter => fmt::format::FmtSpan::ENTER,
            SpanEvent::Exit => fmt::format::FmtSpan::EXIT,
            SpanEvent::Close => fmt::format::FmtSpan::CLOSE,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Deserialize, Serialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Color {
    #[default]
    Auto,
    #[serde(untagged)]
    Explicit(bool),
}

impl Color {
    pub fn to_bool(self) -> bool {
        match self {
            Self::Auto => colored::control::SHOULD_COLORIZE.should_colorize(),
            Self::Explicit(bool) => bool,
        }
    }
}
