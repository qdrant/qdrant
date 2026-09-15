//! Minimal jq-like path filter for JSON values.
//!
//! Supports a small subset of jq path syntax intended for extracting nested
//! telemetry fields without pulling in a full jq implementation:
//!
//! - `.foo.bar` — object key traversal (leading `.` optional)
//! - `.foo.*` / `.foo[]` — iterate object values or array elements
//! - `.foo.bar[]` — `[]` glued to a key is treated as key then iterate
//!
//! An optional `result.` prefix is accepted and stripped so paths can be
//! copied from a full API response while filtering only the telemetry payload.

use serde_json::Value;

const MAX_JQ_LEN: usize = 512;

#[derive(Debug, Clone, PartialEq, Eq)]
enum Segment {
    Key(String),
    Iterate,
}

/// Apply a minimal jq-like path to `value` and return the matched subset.
///
/// - Zero matches → [`Value::Null`]
/// - One match → that value
/// - Multiple matches → JSON array of values (jq stream collected into one value)
pub fn apply_jq(value: &Value, path: &str) -> Result<Value, String> {
    let path = path.trim();
    if path.is_empty() {
        return Err("jq must not be empty".to_string());
    }
    if path.len() > MAX_JQ_LEN {
        return Err(format!("jq exceeds maximum length of {MAX_JQ_LEN}"));
    }

    let segments = parse_path(path)?;
    let mut matches = Vec::new();
    collect_matches(value, &segments, &mut matches);

    Ok(match matches.len() {
        0 => Value::Null,
        1 => matches.pop().unwrap(),
        _ => Value::Array(matches),
    })
}

fn parse_path(path: &str) -> Result<Vec<Segment>, String> {
    let path = path.strip_prefix('.').unwrap_or(path);

    // Allow paths copied from a full API envelope: `result.collections...`
    let path = path
        .strip_prefix("result.")
        .or_else(|| (path == "result").then_some(""))
        .unwrap_or(path);

    if path.is_empty() {
        return Err(
            "jq path must select a field inside telemetry (e.g. `.collections`)".to_string(),
        );
    }

    let mut segments = Vec::new();
    for raw in path.split('.') {
        if raw.is_empty() {
            return Err("jq path contains an empty segment".to_string());
        }

        if raw == "*" || raw == "[]" {
            segments.push(Segment::Iterate);
            continue;
        }

        if let Some(key) = raw.strip_suffix("[]") {
            if key.is_empty() || key == "*" {
                return Err(format!("invalid jq segment `{raw}`"));
            }
            if !is_ident(key) {
                return Err(format!(
                    "invalid jq key `{key}`: only alphanumeric and underscore are allowed"
                ));
            }
            segments.push(Segment::Key(key.to_string()));
            segments.push(Segment::Iterate);
            continue;
        }

        if !is_ident(raw) {
            return Err(format!(
                "invalid jq key `{raw}`: only alphanumeric and underscore are allowed"
            ));
        }
        segments.push(Segment::Key(raw.to_string()));
    }

    Ok(segments)
}

fn is_ident(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn collect_matches(value: &Value, segments: &[Segment], out: &mut Vec<Value>) {
    let Some((head, rest)) = segments.split_first() else {
        out.push(value.clone());
        return;
    };

    match head {
        Segment::Key(key) => {
            if let Some(child) = value.get(key) {
                collect_matches(child, rest, out);
            }
        }
        Segment::Iterate => match value {
            Value::Array(items) => {
                for item in items {
                    collect_matches(item, rest, out);
                }
            }
            Value::Object(map) => {
                for child in map.values() {
                    collect_matches(child, rest, out);
                }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
        },
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn extracts_nested_field() {
        let value = json!({
            "collections": {
                "number_of_collections": 2,
                "collections": [
                    {"id": "a", "transfers": [1]},
                    {"id": "b", "transfers": [2, 3]}
                ]
            }
        });

        assert_eq!(
            apply_jq(&value, ".collections.number_of_collections").unwrap(),
            json!(2)
        );
    }

    #[test]
    fn iterates_arrays_with_star_and_brackets() {
        let value = json!({
            "collections": {
                "collections": [
                    {"transfers": [{"shard_id": 0}]},
                    {"transfers": []}
                ]
            }
        });

        let expected = json!([[{"shard_id": 0}], []]);
        assert_eq!(
            apply_jq(&value, "collections.collections.*.transfers").unwrap(),
            expected
        );
        assert_eq!(
            apply_jq(&value, ".collections.collections[].transfers").unwrap(),
            expected
        );
    }

    #[test]
    fn strips_optional_result_prefix() {
        let value = json!({"app": {"version": "1.0"}});
        assert_eq!(
            apply_jq(&value, "result.app.version").unwrap(),
            json!("1.0")
        );
    }

    #[test]
    fn missing_path_returns_null() {
        let value = json!({"a": 1});
        assert_eq!(apply_jq(&value, ".b.c").unwrap(), Value::Null);
    }

    #[test]
    fn rejects_empty_and_invalid_paths() {
        let value = json!({});
        assert!(apply_jq(&value, "").is_err());
        assert!(apply_jq(&value, ".foo..bar").is_err());
        assert!(apply_jq(&value, ".foo-bar").is_err());
        assert!(apply_jq(&value, "result").is_err());
    }
}
