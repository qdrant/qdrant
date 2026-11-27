use std::borrow::Cow;
use std::fmt;

use serde::de::{Deserialize, DeserializeSeed, Deserializer, IgnoredAny, MapAccess, Visitor};
use serde_json::{Map, Value};

use crate::common::operation_error::{OperationError, OperationResult};
use crate::types::OwnedPayloadRef;

/// Payload holding only the top-level `field` of a stored payload, given in its JSON encoding.
///
/// A point without stored payload (`None`) reads as an empty payload.
pub fn payload_field_from_bytes(
    bytes: Option<&[u8]>,
    field: &str,
) -> OperationResult<OwnedPayloadRef<'static>> {
    let fields = match bytes {
        Some(bytes) => parse_payload_field(bytes, field).map_err(|err| {
            OperationError::service_error(format!("Failed to deserialize payload: {err}"))
        })?,
        None => Map::new(),
    };
    Ok(OwnedPayloadRef::from(fields))
}

/// Parse only the top-level `field` of a JSON object payload.
///
/// The other values are scanned but not materialized, which skips their allocations.
fn parse_payload_field(bytes: &[u8], field: &str) -> serde_json::Result<Map<String, Value>> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let map = FieldSeed(field).deserialize(&mut deserializer)?;
    deserializer.end()?;
    Ok(map)
}

struct FieldSeed<'f>(&'f str);

impl<'de> DeserializeSeed<'de> for FieldSeed<'_> {
    type Value = Map<String, Value>;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for FieldSeed<'_> {
    type Value = Map<String, Value>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut fields = Map::new();
        while let Some(Key(key)) = map.next_key()? {
            if key == self.0 {
                // Same as a full parse: the last duplicate wins
                fields.insert(key.into_owned(), map.next_value()?);
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        Ok(fields)
    }
}

/// Object key, borrowed from the input when it has no escapes.
struct Key<'de>(Cow<'de, str>);

impl<'de> Deserialize<'de> for Key<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_str(KeyVisitor)
    }
}

struct KeyVisitor;

impl<'de> Visitor<'de> for KeyVisitor {
    type Value = Key<'de>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("an object key")
    }

    fn visit_borrowed_str<E>(self, key: &'de str) -> Result<Self::Value, E> {
        Ok(Key(Cow::Borrowed(key)))
    }

    fn visit_str<E>(self, key: &str) -> Result<Self::Value, E> {
        Ok(Key(Cow::Owned(key.to_owned())))
    }

    fn visit_string<E>(self, key: String) -> Result<Self::Value, E> {
        Ok(Key(Cow::Owned(key)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_only_the_field() {
        let json = br#"{"a": [1, {"x": "y"}], "bA": 2, "c": {"d": null}, "bA": 3}"#;
        let full: Map<String, Value> = serde_json::from_slice(json).unwrap();

        for field in ["a", "bA", "c", "missing"] {
            let partial = parse_payload_field(json, field).unwrap();
            assert_eq!(partial.get(field), full.get(field), "{field}");
            assert!(partial.len() <= 1);
        }
    }

    /// Quoted path keys and escaped JSON keys resolve like on the full payload.
    #[test]
    fn quoted_and_escaped_keys() {
        use crate::json_path::JsonPath;

        // `a.b` appears twice, the second time with an escaped dot: the last one wins
        let json = r#"{
            "a.b": {"c": 1},
            "x[]": [1, 2],
            "\u0061": "escaped a",
            "tab\tkey": true,
            "q\"k": {"c": "quote"},
            "a\u002eb": {"c": 2},
            "ключ": [{"v": 3}]
        }"#
        .as_bytes();
        let full: Map<String, Value> = serde_json::from_slice(json).unwrap();

        // (path, expected to match something)
        let cases = [
            (r#""a.b".c"#, true),
            (r#""a.b""#, true),
            (r#""x[]"[1]"#, true),
            ("a", true),
            ("\"tab\tkey\"", true),
            ("ключ[].v", true),
            // `a.b` as a path means key `a`, then key `b`
            ("a.b", false),
            ("missing.c", false),
        ];

        for (path, expect_found) in cases {
            let path: JsonPath = path.parse().unwrap_or_else(|()| panic!("bad path {path}"));
            let partial = parse_payload_field(json, &path.first_key).unwrap();

            let from_partial = path.value_get(&partial);
            assert_eq!(from_partial, path.value_get(&full), "{path}");
            assert_eq!(!from_partial.is_empty(), expect_found, "{path}");
            assert!(partial.len() <= 1, "{path}");
        }

        // Duplicate written with an escape: the last value wins, like a full parse
        let partial = parse_payload_field(json, "a.b").unwrap();
        assert_eq!(partial["a.b"], serde_json::json!({"c": 2}));
    }

    /// Deep paths resolve like on the full payload: only their first key is parsed.
    #[test]
    fn deep_paths() {
        use crate::json_path::JsonPath;

        // `b` mirrors the shape of `a`, so a path that leaked into it would see other values
        let json = br#"{
            "a": {
                "b": {"c": {"d": 1, "e": [10, 20]}},
                "arr": [
                    {"c": {"d": 2}, "m": [[1, 2], [3]]},
                    {"c": {"d": 3}, "m": []},
                    {"c": null},
                    5
                ],
                "n": null,
                "empty": {}
            },
            "b": {
                "b": {"c": {"d": 100, "e": [100]}},
                "arr": [{"c": {"d": 200}}]
            },
            "a": {
                "b": {"c": {"d": 4, "e": [30]}},
                "arr": [{"c": {"d": 5}, "m": [[6]]}, {"c": {"d": 7}}],
                "n": null,
                "empty": {}
            }
        }"#;
        let full: Map<String, Value> = serde_json::from_slice(json).unwrap();

        // (path, number of values found). `a` appears twice: the last one wins.
        let cases = [
            ("a.b.c.d", 1),
            ("a.b.c.e", 1),
            ("a.b.c.e[1]", 0),
            ("a.b.c.e[0]", 1),
            ("a.b.c", 1),
            ("a.arr[].c.d", 2),
            ("a.arr[1].c.d", 1),
            ("a.arr[5].c.d", 0),
            ("a.arr[].m[]", 1),
            ("a.arr[0].m[0]", 1),
            ("a.n", 1),
            ("a.n.x", 0),
            ("a.empty.x.y.z", 0),
            ("a.b.c.d.too_deep", 0),
            ("b.b.c.d", 1),
            ("b.arr[].c.d", 1),
            ("missing.b.c.d", 0),
        ];

        for (path, expected_count) in cases {
            let path: JsonPath = path.parse().unwrap_or_else(|()| panic!("bad path {path}"));
            let partial = parse_payload_field(json, &path.first_key).unwrap();

            let from_partial = path.value_get(&partial);
            assert_eq!(from_partial, path.value_get(&full), "{path}");
            assert_eq!(from_partial.len(), expected_count, "{path}");
            assert!(partial.len() <= 1, "{path}");
        }

        // Spot-check values, to tell the last `a` from the first one and from `b`
        let get = |path: &str| {
            let path: JsonPath = path.parse().unwrap();
            let partial = parse_payload_field(json, &path.first_key).unwrap();
            path.value_get(&partial)
                .into_iter()
                .cloned()
                .collect::<Vec<_>>()
        };
        assert_eq!(get("a.b.c.d"), [serde_json::json!(4)]);
        assert_eq!(
            get("a.arr[].c.d"),
            [serde_json::json!(5), serde_json::json!(7)]
        );
        assert_eq!(get("b.b.c.d"), [serde_json::json!(100)]);
    }

    #[test]
    fn rejects_invalid_json() {
        assert!(parse_payload_field(br#"{"a": 1"#, "a").is_err());
        assert!(parse_payload_field(br#"{"a": 1} x"#, "a").is_err());
        assert!(parse_payload_field(b"[1]", "a").is_err());
    }
}
