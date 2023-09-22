use std::collections::HashSet;
use std::fs;

use anyhow::Context as _;
use serde::{Deserialize, Serialize};
use tracing_subscriber::prelude::*;
use tracing_subscriber::{filter, fmt, registry};

use super::*;

#[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize, Serialize)]
#[serde(default)]
pub struct Config {
    pub enabled: Option<bool>,
    pub log_file: Option<String>,
    pub log_level: Option<String>,
    pub span_events: Option<HashSet<config::SpanEvent>>,
}

impl Config {
    pub fn merge(&mut self, other: Self) {
        self.enabled = other.enabled.or(self.enabled.take());
        self.log_file = other.log_file.or(self.log_file.take());
        self.log_level = other.log_level.or(self.log_level.take());
        self.span_events = other.span_events.or(self.span_events.take());
    }
}

#[rustfmt::skip] // `rustfmt` formats this into unreadable single line :/
pub type Logger<S> = filter::Filtered<
    Option<Layer<S>>,
    filter::EnvFilter,
    S,
>;

#[rustfmt::skip] // `rustfmt` formats this into unreadable single line :/
pub type Layer<S> = fmt::Layer<
    S,
    fmt::format::DefaultFields,
    fmt::format::Format,
    fs::File,
>;

pub fn new_logger<S>(config: &mut Config) -> Logger<S>
where
    S: tracing::Subscriber + for<'span> registry::LookupSpan<'span>,
{
    let layer = match new_layer(config) {
        Ok(layer) => layer,
        Err(err) => {
            eprintln!(
                "failed to enable loggin into {} log-file: {err}",
                config.log_file.as_deref().unwrap_or(""),
            );

            config.enabled = Some(false);
            None
        }
    };

    let filter = new_filter(config);
    layer.with_filter(filter)
}

pub fn new_layer<S>(config: &Config) -> anyhow::Result<Option<Layer<S>>>
where
    S: tracing::Subscriber + for<'span> registry::LookupSpan<'span>,
{
    if !config.enabled.unwrap_or_default() {
        return Ok(None);
    }

    let Some(log_file) = &config.log_file else {
        return Ok(None);
    };

    let writer = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_file)
        .with_context(|| format!("failed to open {} log-file", log_file))?;

    let layer = fmt::Layer::default()
        .with_writer(writer)
        .with_span_events(config::SpanEvent::unwrap_or_default_config(
            &config.span_events,
        ))
        .with_ansi(false);

    Ok(Some(layer))
}

pub fn new_filter(config: &Config) -> filter::EnvFilter {
    filter(config.log_level.as_deref().unwrap_or(""))
}
