use serde::Deserialize;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{filter, fmt, registry};

use super::*;

#[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize)]
#[serde(default)]
pub struct Config {
    pub log_level: Option<String>,
    pub span_events: config::SpanEvents,
    pub color: config::Color,
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
