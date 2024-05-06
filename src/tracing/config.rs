use serde::Deserialize;
use smart_default::SmartDefault;
use tracing_subscriber::fmt;

use super::*;

#[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize)]
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
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, SmartDefault)]
#[serde(from = "helpers::SpanEvents")]
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

#[derive(Copy, Clone, Debug, Eq, PartialEq, Deserialize, SmartDefault)]
#[serde(from = "helpers::Color")]
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

    #[derive(Clone, Debug, Deserialize)]
    #[serde(untagged)]
    pub enum SpanEvents {
        Some(Vec<SpanEvent>),
        None(NoneTag),
        Null,
    }

    impl SpanEvents {
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

    impl From<SpanEvents> for super::SpanEvents {
        fn from(events: SpanEvents) -> Self {
            events.to_fmt_span().into()
        }
    }

    #[derive(Copy, Clone, Debug, Deserialize)]
    #[serde(rename_all = "lowercase")]
    pub enum SpanEvent {
        New,
        Enter,
        Exit,
        Close,
    }

    impl SpanEvent {
        pub fn to_fmt_span(self) -> fmt::format::FmtSpan {
            match self {
                SpanEvent::New => fmt::format::FmtSpan::NEW,
                SpanEvent::Enter => fmt::format::FmtSpan::ENTER,
                SpanEvent::Exit => fmt::format::FmtSpan::EXIT,
                SpanEvent::Close => fmt::format::FmtSpan::CLOSE,
            }
        }
    }

    #[derive(Copy, Clone, Debug, Deserialize)]
    #[serde(rename_all = "lowercase")]
    pub enum NoneTag {
        None,
    }

    #[derive(Copy, Clone, Debug, Deserialize)]
    #[serde(untagged)]
    pub enum Color {
        Auto(AutoTag),
        Bool(bool),
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

    #[derive(Copy, Clone, Debug, Deserialize)]
    #[serde(rename_all = "lowercase")]
    pub enum AutoTag {
        Auto,
    }
}
