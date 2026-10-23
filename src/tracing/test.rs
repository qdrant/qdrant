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
            "buffer_size_bytes": 1024,
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
            buffer_size_bytes: Some(1024),
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
            "buffer_size_bytes": 1024,
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
            buffer_size_bytes: Some(1024),
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
            "buffer_size_bytes": null,
        }
    });

    let config = deserialize_config(json);
    assert_eq!(config, LoggerConfig::default());
}

#[test]
fn utc_timer_matches_default_timer_format() {
    let utc = format_time(&TimeFormat::Utc);
    let default = format_time(&tracing_subscriber::fmt::time::SystemTime);

    // e.g. `2026-10-13T02:04:27.604091Z`
    assert_eq!(utc.len(), default.len(), "{utc} vs {default}");
    assert!(utc.ends_with('Z'), "{utc}");

    let utc = chrono::DateTime::parse_from_rfc3339(&utc).unwrap();
    let default = chrono::DateTime::parse_from_rfc3339(&default).unwrap();
    assert!((utc - default).abs() < chrono::TimeDelta::seconds(1));
}

#[test]
fn write_rfc3339_matches_chrono() {
    let timestamps = [
        (1_791_857_067, 604_091_123),
        (1_791_857_067, 999),
        (0, 0),
        (1_798_761_599, 999_999_999),
    ];
    let offsets = [
        0,
        8 * 3600,
        -4 * 3600,
        5 * 3600 + 45 * 60,
        -(9 * 3600 + 30 * 60),
    ];

    for (secs, nanos) in timestamps {
        for offset in offsets {
            let time = DateTime::from_timestamp(secs, nanos)
                .unwrap()
                .with_timezone(&FixedOffset::east_opt(offset).unwrap());

            let mut buf = String::new();
            write_rfc3339(&mut buf, time).unwrap();
            assert_eq!(
                buf,
                time.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
            );
        }
    }
}

fn format_time(timer: &impl FormatTime) -> String {
    let mut buf = String::new();
    timer.format_time(&mut Writer::new(&mut buf)).unwrap();
    buf
}

fn deserialize_config(json: serde_json::Value) -> LoggerConfig {
    serde_json::from_value(json).unwrap()
}
