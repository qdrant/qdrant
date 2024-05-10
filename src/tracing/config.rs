use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use tracing_subscriber::fmt;

use super::*;

#[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize, Serialize)]
#[serde(default)]
pub struct LoggerConfig {
    #[serde(flatten)]
    pub default: default::Config,
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
    pub fn from_fmt_span(events: fmt::format::FmtSpan) -> HashSet<Self> {
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

#[derive(Copy, Clone, Debug, Eq, PartialEq, Deserialize, Serialize, Default)]
#[serde(rename_all = "lowercase")]
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
