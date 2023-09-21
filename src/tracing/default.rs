use serde::{Deserialize, Serialize};
use tracing_subscriber::prelude::*;
use tracing_subscriber::{filter, fmt, registry};

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
