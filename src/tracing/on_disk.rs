use std::collections::HashSet;
use std::sync::Mutex;
use std::{fs, io};

use anyhow::Context as _;
use common::ext::OptionExt;
use serde::{Deserialize, Serialize};
use tracing_subscriber::{Layer, fmt, registry};

use super::*;

#[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize, Serialize)]
#[serde(default)]
pub struct Config {
    pub enabled: Option<bool>,
    pub log_file: Option<String>,
    pub log_level: Option<String>,
    pub format: Option<config::LogFormat>,
    pub span_events: Option<HashSet<config::SpanEvent>>,
}

impl Config {
    pub fn merge(&mut self, other: Self) {
        let Self {
            enabled,
            log_file,
            log_level,
            span_events,
            format,
        } = other;

        self.enabled.replace_if_some(enabled);
        self.log_file.replace_if_some(log_file);
        self.log_level.replace_if_some(log_level);
        self.span_events.replace_if_some(span_events);
        self.format.replace_if_some(format);
    }
}

pub fn new_logger<S>(config: &mut Config) -> Logger<S>
where
    S: tracing::Subscriber + for<'span> registry::LookupSpan<'span>,
{
    let layer = match new_layer(config) {
        Ok(layer) => layer,
        Err(err) => {
            eprintln!(
                "failed to enable logging into {} log-file: {err}",
                config.log_file.as_deref().unwrap_or(""),
            );

            config.enabled = Some(false);
            None
        }
    };

    let filter = new_filter(config);
    layer.with_filter(filter)
}

pub fn new_layer<S>(config: &Config) -> anyhow::Result<Option<Box<dyn Layer<S> + Send + Sync>>>
where
    S: tracing::Subscriber + for<'span> registry::LookupSpan<'span>,
{
    if !config.enabled.unwrap_or_default() {
        return Ok(None);
    }

    let Some(log_file) = &config.log_file else {
        return Err(anyhow::format_err!("log file is not specified"));
    };

    let writer = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_file)
        .with_context(|| format!("failed to open {log_file} log-file"))?;

    let layer = fmt::Layer::default()
        .with_writer(Mutex::new(io::BufWriter::new(writer)))
        .with_span_events(config::SpanEvent::unwrap_or_default_config(
            &config.span_events,
        ))
        .with_ansi(false);

    let layer = match config.format {
        None | Some(config::LogFormat::Text) => Box::new(layer) as _,
        Some(config::LogFormat::Json) => Box::new(layer.json()) as _,
    };

    Ok(Some(layer))
}

pub fn new_filter(config: &Config) -> filter::EnvFilter {
    filter(config.log_level.as_deref().unwrap_or(""))
}
