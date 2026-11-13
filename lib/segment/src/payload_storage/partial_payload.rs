use std::borrow::Cow;
use std::fmt;

use serde::de::{Deserialize, DeserializeSeed, Deserializer, IgnoredAny, MapAccess, Visitor};
use serde_json::{Map, Value};

/// Parse only the top-level `field` of a JSON object payload.
///
/// The other values are scanned but not materialized, which skips their allocations.
pub fn parse_payload_field(bytes: &[u8], field: &str) -> serde_json::Result<Map<String, Value>> {
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

    #[test]
    fn rejects_invalid_json() {
        assert!(parse_payload_field(br#"{"a": 1"#, "a").is_err());
        assert!(parse_payload_field(br#"{"a": 1} x"#, "a").is_err());
        assert!(parse_payload_field(b"[1]", "a").is_err());
    }
}
