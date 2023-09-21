use serde_json::json;
use tracing_subscriber::fmt;

use super::*;

#[test]
fn deseriailze_logger_config() {
    let config = deserialize_config(config());

    let expected = LoggerConfig {
        default: default::Config {
            log_level: Some("debug".into()),
            span_events: (fmt::format::FmtSpan::NEW | fmt::format::FmtSpan::CLOSE).into(),
            color: config::Color::Enable,
        },
    };

    assert_eq!(config, expected);
}

#[test]
fn deserialize_empty_config() {
    let config = deserialize_config(empty_config());
    assert_eq!(config, LoggerConfig::default());
}

#[test]
fn deserialize_logger_config_diff() {
    let diff = deserialize_diff(config());

    let expected = LoggerConfigDiff {
        default: default::ConfigDiff {
            log_level: Some(Some("debug".into())),
            span_events: Some((fmt::format::FmtSpan::NEW | fmt::format::FmtSpan::CLOSE).into()),
            color: Some(config::Color::Enable),
        },
    };

    assert_eq!(diff, expected);
}

#[test]
fn deserialize_empty_diff() {
    let diff = deserialize_diff(empty_config());
    assert_eq!(diff, LoggerConfigDiff::default());
}

#[test]
fn deserialize_diff_with_explicit_nulls() {
    let diff = deserialize_diff(json!({
        "log_level": null,
        "span_events": null,
        "color": null,
    }));

    let expected = LoggerConfigDiff {
        default: default::ConfigDiff {
            log_level: Some(None),
            ..Default::default()
        },
    };

    assert_eq!(diff, expected);
}

fn deserialize_config(json: serde_json::Value) -> LoggerConfig {
    serde_json::from_value(json).unwrap()
}

fn deserialize_diff(json: serde_json::Value) -> LoggerConfigDiff {
    serde_json::from_value(json).unwrap()
}

fn config() -> serde_json::Value {
    json!({
        "log_level": "debug",
        "span_events": ["new", "close"],
        "color": true,
    })
}

fn empty_config() -> serde_json::Value {
    json!({})
}
