use std::collections::HashSet;

use serde_json::json;

use super::*;

#[test]
fn deseriailze_logger_config() {
    let json = json!({
        "log_level": "debug",
        "span_events": ["new", "close"],
        "color": "enable",
    });

    let config = deserialize_config(json);

    let expected = LoggerConfig {
        default: default::Config {
            log_level: Some("debug".into()),
            span_events: Some(HashSet::from_iter([
                config::SpanEvent::New,
                config::SpanEvent::Close,
            ])),
            color: Some(config::Color::Enable),
        },
    };

    assert_eq!(config, expected);
}

fn deserialize_config(json: serde_json::Value) -> LoggerConfig {
    serde_json::from_value(json).unwrap()
}
