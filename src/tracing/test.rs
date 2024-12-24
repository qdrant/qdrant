use std::collections::HashSet;

use serde_json::json;

use super::*;

#[test]
fn deserialize_logger_config() {
    let json = json!({
        "log_level": "debug",
        "span_events": ["new", "close"],
        "color": true,

        "on_disk": {
            "enabled": true,
            "log_file": "/logs/qdrant",
            "log_level": "tracing",
            "span_events": ["new", "close"],
        }
    });

    let config = deserialize_config(json);

    let expected = LoggerConfig {
        default: default::Config {
            log_level: Some("debug".into()),
            span_events: Some(HashSet::from([
                config::SpanEvent::New,
                config::SpanEvent::Close,
            ])),
            format: None,
            color: Some(config::Color::Explicit(true)),
        },

        on_disk: on_disk::Config {
            enabled: Some(true),
            log_file: Some("/logs/qdrant".into()),
            log_level: Some("tracing".into()),
            span_events: Some(HashSet::from([
                config::SpanEvent::New,
                config::SpanEvent::Close,
            ])),
            format: None,
        },
    };

    assert_eq!(config, expected);
}

#[test]
fn deserialize_json_logger_config() {
    let json = json!({
        "log_level": "debug",
        "span_events": ["new", "close"],
        "format": "json",
        "color": true,

        "on_disk": {
            "enabled": true,
            "log_file": "/logs/qdrant",
            "log_level": "tracing",
            "span_events": ["new", "close"],
            "format": "text",
        }
    });

    let config = deserialize_config(json);

    let expected = LoggerConfig {
        default: default::Config {
            log_level: Some("debug".into()),
            span_events: Some(HashSet::from([
                config::SpanEvent::New,
                config::SpanEvent::Close,
            ])),
            format: Some(config::LogFormat::Json),
            color: Some(config::Color::Explicit(true)),
        },

        on_disk: on_disk::Config {
            enabled: Some(true),
            log_file: Some("/logs/qdrant".into()),
            log_level: Some("tracing".into()),
            span_events: Some(HashSet::from([
                config::SpanEvent::New,
                config::SpanEvent::Close,
            ])),
            format: Some(config::LogFormat::Text),
        },
    };

    assert_eq!(config, expected);
}

#[test]
fn deserialize_empty_config() {
    let config = deserialize_config(json!({}));
    assert_eq!(config, LoggerConfig::default());
}

#[test]
fn deserialize_config_with_empty_on_disk() {
    let config = deserialize_config(json!({ "on_disk": {} }));
    assert_eq!(config, LoggerConfig::default());
}

#[test]
fn deseriailze_config_with_explicit_nulls() {
    let json = json!({
        "log_level": null,
        "span_events": null,
        "format": null,
        "color": null,

        "on_disk": {
            "enabled": null,
            "log_file": null,
            "log_level": null,
            "span_events": null,
            "format": null,
        }
    });

    let config = deserialize_config(json);
    assert_eq!(config, LoggerConfig::default());
}

fn deserialize_config(json: serde_json::Value) -> LoggerConfig {
    serde_json::from_value(json).unwrap()
}
