use std::collections::HashSet;

use common::ext::OptionExt;
use serde::{Deserialize, Serialize};
use tracing_subscriber::fmt::{self};
use tracing_subscriber::{filter, registry, Layer};

use super::*;

#[derive(Default, Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    #[default]
    Text,
    Json,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize, Serialize)]
#[serde(default)]
pub struct Config {
    pub log_level: Option<String>,
    pub span_events: Option<HashSet<config::SpanEvent>>,
    pub color: Option<config::Color>,
    pub log_format: Option<LogFormat>,
}

impl Config {
    pub fn merge(&mut self, other: Self) {
        let Self {
            log_level,
            span_events,
            color,
            log_format,
        } = other;

        self.log_level.replace_if_some(log_level);
        self.span_events.replace_if_some(span_events);
        self.color.replace_if_some(color);
        self.log_format.replace_if_some(log_format);
    }
}

pub type Logger<S> =
    filter::Filtered<Option<Box<dyn Layer<S> + Send + Sync>>, filter::EnvFilter, S>;

pub fn new_logger<S>(config: &Config) -> Logger<S>
where
    S: tracing::Subscriber + for<'span> registry::LookupSpan<'span>,
{
    let layer: Box<dyn Layer<S> + Send + Sync> = match config.log_format {
        Some(LogFormat::Json) => Box::new(new_layer_with_json(config)),
        Some(LogFormat::Text) | None => Box::new(new_layer(config)),
    };
    let filter = new_filter(config);
    Some(layer).with_filter(filter)
}

pub fn new_layer<S>(config: &Config) -> Box<dyn Layer<S> + Send + Sync>
where
    S: tracing::Subscriber + for<'span> registry::LookupSpan<'span>,
{
    Box::new(
        fmt::Layer::default()
            .with_span_events(config::SpanEvent::unwrap_or_default_config(
                &config.span_events,
            ))
            .with_ansi(config.color.unwrap_or_default().to_bool()),
    )
}

pub fn new_layer_with_json<S>(config: &Config) -> Box<dyn Layer<S> + Send + Sync>
where
    S: tracing::Subscriber + for<'span> registry::LookupSpan<'span>,
{
    Box::new(
        fmt::Layer::default()
            .json()
            .with_span_events(config::SpanEvent::unwrap_or_default_config(
                &config.span_events,
            ))
            .with_ansi(config.color.unwrap_or_default().to_bool()),
    )
}

pub fn new_filter(config: &Config) -> filter::EnvFilter {
    filter(config.log_level.as_deref().unwrap_or(""))
}
