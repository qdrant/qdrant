use std::collections::HashSet;

use common::ext::OptionExt;
use serde::{Deserialize, Serialize};
use tracing_subscriber::{Layer, fmt, registry};

use super::*;

#[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize, Serialize)]
#[serde(default)]
pub struct Config {
    pub log_level: Option<String>,
    pub span_events: Option<HashSet<config::SpanEvent>>,
    pub format: Option<config::LogFormat>,
    pub color: Option<config::Color>,
}

impl Config {
    pub fn merge(&mut self, other: Self) {
        let Self {
            log_level,
            span_events,
            format,
            color,
        } = other;

        self.log_level.replace_if_some(log_level);
        self.span_events.replace_if_some(span_events);
        self.format.replace_if_some(format);
        self.color.replace_if_some(color);
    }
}

pub fn new_logger<S>(config: &Config) -> Logger<S>
where
    S: tracing::Subscriber + for<'span> registry::LookupSpan<'span>,
{
    let layer = new_layer(config);
    let filter = new_filter(config);
    Some(layer).with_filter(filter)
}

pub fn new_layer<S>(config: &Config) -> Box<dyn Layer<S> + Send + Sync>
where
    S: tracing::Subscriber + for<'span> registry::LookupSpan<'span>,
{
    let layer = fmt::Layer::default()
        .with_span_events(config::SpanEvent::unwrap_or_default_config(
            &config.span_events,
        ))
        .with_ansi(config.color.unwrap_or_default().to_bool());

    match config.format {
        None | Some(config::LogFormat::Text) => Box::new(layer),
        Some(config::LogFormat::Json) => Box::new(layer.json()),
    }
}

pub fn new_filter(config: &Config) -> filter::EnvFilter {
    filter(config.log_level.as_deref().unwrap_or(""))
}
