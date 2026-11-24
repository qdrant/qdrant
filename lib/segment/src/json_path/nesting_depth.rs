use serde_json::{Map, Value};

use super::JsonPath;
use crate::common::operation_error::{OperationError, OperationResult};

/// Maximum nesting depth of a payload, counting the payload object itself.
///
/// Payloads are stored as JSON, and `serde_json` does not parse documents nested any deeper. A
/// deeper payload could be written, but not read back.
pub const MAX_PAYLOAD_NESTING_DEPTH: usize = 127;

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
        let path_depth = path.map_or(0, |path| path.rest.len() + 1);
        let depth = path_depth.saturating_add(nesting_depth(src));
        if depth > MAX_PAYLOAD_NESTING_DEPTH {
            return Err(OperationError::validation_error(format!(
                "payload is nested {depth} levels deep including the key, \
                 at most {MAX_PAYLOAD_NESTING_DEPTH} levels are supported",
            )));
        }
        Ok(())
    }
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
