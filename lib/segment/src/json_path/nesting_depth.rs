use serde_json::{Map, Value};

use super::JsonPath;
use crate::common::operation_error::{OperationError, OperationResult};

/// Maximum nesting depth of a payload, counting the payload object itself.
///
/// Payloads are stored as JSON, and `serde_json` does not parse documents nested any deeper. A
/// deeper payload could be written, but not read back.
pub const MAX_PAYLOAD_NESTING_DEPTH: usize = 127;

/// Maximum nesting depth of a payload in a new operation, including the key it is set at.
///
/// Lower than [`MAX_PAYLOAD_NESTING_DEPTH`]: the WAL stores operations in a format with the same
/// depth limit, and its records wrap the payload in a few more levels.
pub const MAX_NEW_PAYLOAD_NESTING_DEPTH: usize = 100;

impl JsonPath {
    /// Check that [`JsonPath::value_set`] with `path` and `src` creates a payload that is not
    /// nested deeper than [`MAX_PAYLOAD_NESTING_DEPTH`].
    ///
    /// `value_set` adds one level of nesting for each path component, recursing once per
    /// component. Call this before `value_set` on user input.
    pub fn check_value_set_depth(
        path: Option<&Self>,
        src: &Map<String, Value>,
    ) -> OperationResult<()> {
        check_depth(path, src, MAX_PAYLOAD_NESTING_DEPTH)
    }

    /// Check that a payload in a new operation, set at `path`, is not nested deeper than
    /// [`MAX_NEW_PAYLOAD_NESTING_DEPTH`]. Call this before the operation is written to the WAL.
    pub fn check_new_payload_depth(
        path: Option<&Self>,
        payload: &Map<String, Value>,
    ) -> OperationResult<()> {
        check_depth(path, payload, MAX_NEW_PAYLOAD_NESTING_DEPTH)
    }
}

fn check_depth(
    path: Option<&JsonPath>,
    src: &Map<String, Value>,
    max_depth: usize,
) -> OperationResult<()> {
    let path_depth = path.map_or(0, |path| path.rest.len() + 1);
    let depth = path_depth.saturating_add(nesting_depth(src));
    if depth > max_depth {
        let subject = if path.is_some() {
            "payload and key are"
        } else {
            "payload is"
        };
        return Err(OperationError::validation_error(format!(
            "{subject} nested {depth} levels deep, at most {max_depth} levels are supported",
        )));
    }
    Ok(())
}

/// Nesting depth of a JSON object, the way `serde_json` counts it: one level for the object
/// itself and for each nested object or array.
fn nesting_depth(map: &Map<String, Value>) -> usize {
    // Iterate rather than recurse, the value comes from user input
    let mut max_depth = 1;
    let mut stack: Vec<(&Value, usize)> = map.values().map(|value| (value, 2)).collect();
    while let Some((value, depth)) = stack.pop() {
        match value {
            Value::Object(map) => {
                max_depth = max_depth.max(depth);
                stack.extend(map.values().map(|value| (value, depth + 1)));
            }
            Value::Array(array) => {
                max_depth = max_depth.max(depth);
                stack.extend(array.iter().map(|value| (value, depth + 1)));
            }
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
        }
    }
    max_depth
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::types::Payload;

    const MAX: usize = MAX_PAYLOAD_NESTING_DEPTH;

    fn object(value: Value) -> Map<String, Value> {
        let Value::Object(map) = value else {
            panic!("not an object: {value}");
        };
        map
    }

    /// Path of `len` keys: `a.a.a…`
    fn key_path(len: usize) -> JsonPath {
        vec!["a"; len].join(".").parse().unwrap()
    }

    /// Object nested `depth` levels deep, counting itself
    fn nested_object(depth: usize) -> Map<String, Value> {
        let mut map = Map::new();
        for _ in 1..depth {
            let mut outer = Map::new();
            outer.insert("b".to_string(), Value::Object(map));
            map = outer;
        }
        map
    }

    /// Whether the payload can be read back after storing it
    fn readable(map: &Map<String, Value>) -> bool {
        let bytes = serde_json::to_vec(map).unwrap();
        serde_json::from_slice::<Payload>(&bytes).is_ok()
    }

    #[test]
    fn test_nesting_depth() {
        assert_eq!(nesting_depth(&object(json!({}))), 1);
        assert_eq!(
            nesting_depth(&object(json!({"a": 1, "b": "c", "d": null}))),
            1
        );
        assert_eq!(nesting_depth(&object(json!({"a": {}}))), 2);
        assert_eq!(nesting_depth(&object(json!({"a": []}))), 2);
        assert_eq!(
            nesting_depth(&object(json!({"a": [1, {"b": [[]]}], "c": {"d": 1}}))),
            5,
        );
        assert_eq!(nesting_depth(&nested_object(MAX)), MAX);
    }

    #[test]
    fn test_max_depth_matches_serde_json() {
        assert!(readable(&nested_object(MAX)));
        assert!(!readable(&nested_object(MAX + 1)));
    }

    /// Accepts exactly the operations that create a payload that can be read back
    #[test]
    fn test_check_value_set_depth() {
        let lengths = [0, 1, 2, 63, 64, MAX - 2, MAX - 1, MAX, MAX + 1];
        for path_len in lengths {
            for src_depth in lengths.into_iter().filter(|&depth| depth > 0) {
                let path = (path_len > 0).then(|| key_path(path_len));
                let src = nested_object(src_depth);

                let result = JsonPath::check_value_set_depth(path.as_ref(), &src);
                let mut dest = Map::new();
                JsonPath::value_set(path.as_ref(), &mut dest, &src);

                let msg = format!("path length {path_len}, payload depth {src_depth}");
                assert_eq!(nesting_depth(&dest), path_len + src_depth, "{msg}");
                assert_eq!(result.is_ok(), readable(&dest), "{msg}");
                if let Err(err) = result {
                    assert!(
                        matches!(err, OperationError::ValidationError { .. }),
                        "{msg}: {err}",
                    );
                }
            }
        }
    }

    /// `value_set` does not nest deeper than the check assumes, also with array indices and
    /// existing values
    #[test]
    fn test_value_set_depth_bound() {
        let payloads = [
            json!({}),
            json!({"a": 1}),
            json!({"a": [{"b": {"c": 1}}, 2], "x": {"y": {"z": [[1]]}}}),
            json!({"a": {"b": [[[{}]]]}}),
        ];
        let paths = [
            "a",
            "a.b",
            "a[0]",
            "a[1]",
            "a[5]",
            "a[]",
            "a[].b",
            "a[0].b.c",
            "a.b[0][0]",
            "a.b[][]",
            "a.b[0][0][0][0]",
            "x.y.z[0][0]",
        ];
        let srcs = [
            json!({}),
            json!({"k": 1}),
            json!({"k": {"l": [1]}}),
            json!({"k": null}),
        ];

        for payload in &payloads {
            for path in paths {
                let path: JsonPath = path.parse().unwrap();
                for src in &srcs {
                    let src = object(src.clone());
                    let mut dest = object(payload.clone());
                    let before = nesting_depth(&dest);
                    JsonPath::value_set(Some(&path), &mut dest, &src);

                    let bound = path.rest.len() + 1 + nesting_depth(&src);
                    assert!(
                        nesting_depth(&dest) <= before.max(bound),
                        "{payload} with {path} = {src:?} gives {dest:?}",
                    );
                }
            }
        }
    }

    /// Rejects long paths without recursing once per path component
    #[test]
    fn test_check_value_set_depth_long_path() {
        let path = key_path(1_000_000);
        assert!(JsonPath::check_value_set_depth(Some(&path), &Map::new()).is_err());
    }
}
