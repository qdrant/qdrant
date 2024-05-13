use std::collections::HashSet;

use serde_json::json;

use super::*;

#[test]
fn deseriailze_logger_config() {
    let json = json!({
        "log_level": "debug",
        "span_events": ["new", "close"],
        "color": true,
    });

    let config = deserialize_config(json);

    let expected = LoggerConfig {
        default: default::Config {
            log_level: Some("debug".into()),
            span_events: Some(HashSet::from_iter([
                config::SpanEvent::New,
                config::SpanEvent::Close,
            ])),
            color: Some(config::Color::Explicit(true)),
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
fn deseriailze_config_with_explicit_nulls() {
    let json = json!({
        "log_level": null,
        "span_events": null,
        "color": null,
    });

    let config = deserialize_config(json);
    assert_eq!(config, LoggerConfig::default());
}

fn deserialize_config(json: serde_json::Value) -> LoggerConfig {
    serde_json::from_value(json).unwrap()
}
