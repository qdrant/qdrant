use serde_json::json;
use tracing_subscriber::fmt;

use super::*;

#[test]
fn deseriailze_logger_config() {
    let config = deserialize_config(config());

    let expected = LoggerConfig {
        default: default::Config {
            log_level: Some("debug".to_string()),
            span_events: Some(config::SpanEvent::from_fmt_span(
                fmt::format::FmtSpan::NEW | fmt::format::FmtSpan::CLOSE,
            )),
            color: Some(config::Color::Enable),
        },
    };

    assert_eq!(config, expected);
}

fn deserialize_config(json: serde_json::Value) -> LoggerConfig {
    serde_json::from_value(json).unwrap()
}

fn config() -> serde_json::Value {
    json!({
        "log_level": "debug",
        "span_events": ["new", "close"],
        "color": "enable",
    })
}
