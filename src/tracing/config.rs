use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;
use tracing_subscriber::fmt;

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
