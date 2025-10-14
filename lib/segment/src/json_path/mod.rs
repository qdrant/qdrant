use std::fmt::{Display, Formatter};
use std::hash::Hash;

use data_encoding::BASE32_DNSSEC;
use itertools::Itertools as _;
use schemars::JsonSchema;
use schemars::r#gen::SchemaGenerator;
use schemars::schema::Schema;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use crate::common::anonymize::Anonymize;
use crate::common::utils::{MultiValue, merge_map};

mod parse;

#[derive(Debug, Clone, PartialEq, Eq, Anonymize, Ord, Hash, PartialOrd)]
pub struct JsonPath {
    pub first_key: String,
    pub rest: Vec<JsonPathItem>,
}

#[derive(Debug, PartialEq, Clone, Eq, Anonymize, Ord, Hash, PartialOrd)]
pub enum JsonPathItem {
    /// A key in a JSON object, e.g. `.foo`
    Key(String),
    /// An index in a JSON array, e.g. `[3]`
    #[anonymize(false)]
    Index(usize),
    /// All indices in a JSON array, i.e. `[]`
    WildcardIndex,
}

impl JsonPath {
    /// Create a new `JsonPath` from a string. For production code, use `FromStr::parse` instead.
    ///
    /// # Panics
    ///
    /// Panics if the string is not a valid path. Thus, this function should only be used in tests.
    #[cfg(feature = "testing")]
    pub fn new(p: &str) -> Self {
        p.parse().unwrap()
    }

    /// Get values at a given JSON path from a JSON map.
    pub fn value_get<'a>(
        &self,
        json_map: &'a serde_json::Map<String, Value>,
    ) -> MultiValue<&'a Value> {
        let mut result = MultiValue::new();
        if let Some(value) = json_map.get(&self.first_key) {
            value_get(&self.rest, Some(value), &mut result);
        }
        result
    }

    /// Set values at a given JSON path in a JSON map.
    pub fn value_set<'a>(
        path: Option<&Self>,
        dest: &'a mut serde_json::Map<String, Value>,
        src: &'a serde_json::Map<String, Value>,
    ) {
        if let Some(path) = path {
            value_set_map(&path.first_key, &path.rest, dest, src);
        } else {
            merge_map(dest, src);
        }
    }

    /// Remove values at a given JSON path from a JSON map. Returns values that were removed.
    pub fn value_remove(&self, json_map: &mut serde_json::Map<String, Value>) -> MultiValue<Value> {
        let mut result = MultiValue::new();
        if let Some((rest1, restn)) = self.rest.split_first() {
            if let Some(value) = json_map.get_mut(&self.first_key) {
                value_remove(rest1, restn, value, &mut result);
            }
        } else if let Some(value) = json_map.remove(&self.first_key) {
            result.push(value);
        }
        result
    }

    /// Filter values in a JSON map based on a predicate.
    pub fn value_filter(
        json_map: &serde_json::Map<String, Value>,
        filter: impl Fn(&Self, &Value) -> bool,
    ) -> serde_json::Map<String, Value> {
        let mut new_map = serde_json::Map::new();
        let mut path = JsonPath {
            first_key: "".to_string(),
            rest: Vec::new(),
        };
        for (key, value) in json_map.iter() {
            path.first_key.clone_from(key);
            if filter(&path, value) {
                let value = run_filter(&mut path, value, &filter);
                new_map.insert(key.clone(), value);
            }
        }
        new_map
    }

    /// Remove the wildcard suffix from the path, if it exists.
    /// E.g. `a.b[]` -> `a.b`.
    pub fn strip_wildcard_suffix(&self) -> Self {
        match self.rest.split_last() {
            Some((JsonPathItem::WildcardIndex, rest)) => JsonPath {
                first_key: self.first_key.clone(),
                rest: rest.to_vec(),
            },
            _ => self.clone(),
        }
    }

    /// If `self` starts with `prefix`, returns a new path with the prefix removed.
    pub fn strip_prefix(&self, prefix: &Self) -> Option<Self> {
        if self.first_key != prefix.first_key {
            return None;
        }
        let mut self_it = self.rest.iter().peekable();
        let mut prefix_it = prefix.rest.iter().peekable();
        loop {
            match (self_it.peek(), prefix_it.peek()) {
                (Some(self_item), Some(prefix_item)) if self_item == prefix_item => {
                    self_it.next();
                    prefix_it.next();
                }
                (Some(_), Some(_)) => return None,
                (Some(JsonPathItem::Key(k)), None) => {
                    return Some(JsonPath {
                        first_key: k.clone(),
                        rest: self_it.skip(1).cloned().collect(),
                    });
                }
                (Some(_), None) => {
                    // We don't support json paths starting with `[`. So
                    // `strip_prefix("foo[]", "foo")` is not possible.
                    return None;
                }
                (None, Some(_)) => return None,
                (None, None) => {
                    // Paths are equal. We don't support empty json paths.
                    return None;
                }
            }
        }
    }

    /// Extend the path with another path.
    pub fn extend(&self, other: &Self) -> Self {
        let mut rest = Vec::with_capacity(self.rest.len() + 1 + other.rest.len());
        rest.extend_from_slice(&self.rest);
        rest.push(JsonPathItem::Key(other.first_key.clone()));
        rest.extend_from_slice(&other.rest);
        JsonPath {
            first_key: self.first_key.clone(),
            rest,
        }
    }

    /// Returns a new path with an array key appended to the end.
    /// E.g. `a.b` -> `a.b[]`.
    pub fn array_key(&self) -> Self {
        let mut result = JsonPath {
            first_key: self.first_key.clone(),
            rest: Vec::with_capacity(self.rest.len() + 1),
        };
        result.rest.extend_from_slice(&self.rest);
        if result.rest.last() != Some(&JsonPathItem::WildcardIndex) {
            result.rest.push(JsonPathItem::WildcardIndex);
        }
        result
    }

    pub fn has_wildcard_suffix(&self) -> bool {
        self.rest.last() == Some(&JsonPathItem::WildcardIndex)
    }

    /// Check if a path is included in a list of patterns.
    ///
    /// Basically, it checks if either the pattern or path is a prefix of the other.
    pub fn check_include_pattern(&self, pattern: &Self) -> bool {
        self.first_key == pattern.first_key
            && self.rest.iter().zip(&pattern.rest).all(|(a, b)| a == b)
    }

    /// Check if a path should be excluded by a pattern.
    ///
    /// Basically, it checks if pattern is a prefix of path, but not the other way around.
    pub fn check_exclude_pattern(&self, pattern: &Self) -> bool {
        self.first_key == pattern.first_key && pattern.rest.starts_with(&self.rest)
    }

    pub fn extend_or_new(base: Option<&Self>, other: &Self) -> Self {
        base.map_or_else(|| other.clone(), |base| base.extend(other))
    }

    /// Check if a path is a compatible prefix of another path or vice versa.
    pub fn compatible(&self, other: &Self) -> bool {
        if self.first_key != other.first_key {
            return false;
        }
        self.rest
            .iter()
            .zip(&other.rest)
            .all(|(a, b)| match (a, b) {
                (JsonPathItem::Key(a), JsonPathItem::Key(b)) => a == b,
                (JsonPathItem::Index(a), JsonPathItem::Index(b)) => a == b,
                (JsonPathItem::WildcardIndex, JsonPathItem::WildcardIndex) => true,
                (JsonPathItem::Index(_), JsonPathItem::WildcardIndex) => true,
                (JsonPathItem::WildcardIndex, JsonPathItem::Index(_)) => true,
                _ => false,
            })
    }

    /// Check if the path will be affected by a call to `path_to_remove.value_remove(_)`.
    pub fn is_affected_by_value_remove(&self, path_to_remove: &JsonPath) -> bool {
        // If we have, e.g., indexed field "a.b", then it is not safe to delete any of of "a",
        // "a.b", or "a.b.c".
        path_to_remove.compatible(self)
    }

    /// Check if the path will be affected by a call to `path_to_set.value_set(_, payload)`.
    pub fn is_affected_by_value_set(
        &self,
        payload: &serde_json::Map<String, Value>,
        path_to_set: Option<&JsonPath>,
    ) -> bool {
        // Suppose we have a `path_to_set=a.b.c` and a `payload={"x": 1, "y": 2, "z": {"q": 0}}`.
        // It's safe to set the payload if the indexed fields doesn't intersect[^1] with the
        // following paths:
        // - `a.b.c.x`
        // - `a.b.c.y`
        // - `a.b.c.z` // Note that only top-level keys of the payload are considered.
        //
        // [^1]: In simple cases, we consider two paths to intersect if one of them is a prefix of
        // the other.  For example, `a.b` and `a.b.c` intersect, but `a.b` and `a.c` don't. More
        // nuanced cases include wildcard indexes, e.g., `a[0].b` and `a[].b` intersect.
        // Additionally, we consider path with incompatible types (e.g. `a[0]` and `a.b`) to
        // intersect because `valuse_set` could override the subtree by replacing an array with an
        // object (or vice versa), deleting indexed fields.

        let Some(path_to_set) = path_to_set else {
            return payload.contains_key(&self.first_key);
        };
        if self.first_key != path_to_set.first_key {
            return false;
        }
        let mut it_a = self.rest.iter();
        let mut it_b = path_to_set.rest.iter();
        loop {
            let (a, b) = match (it_a.next(), it_b.next()) {
                (Some(a), Some(b)) => (a, b),
                (None, _) => return true, // indexed_path is a compatible prefix of path_to_set

                (Some(JsonPathItem::Key(a)), None) => return payload.contains_key(a),
                (Some(JsonPathItem::Index(_)), None) => return true,
                (Some(JsonPathItem::WildcardIndex), None) => return true,
            };

            match (a, b) {
                // Paths items match each other => continue.
                (JsonPathItem::Key(a), JsonPathItem::Key(b)) if a == b => (),
                (JsonPathItem::Index(a), JsonPathItem::Index(b)) if a == b => (),
                (JsonPathItem::WildcardIndex, JsonPathItem::WildcardIndex) => (),
                (JsonPathItem::Index(_), JsonPathItem::WildcardIndex) => (),
                (JsonPathItem::WildcardIndex, JsonPathItem::Index(_)) => (),

                // Paths diverge, but their types are compatible, e.g. `a.b` and `a.c`, or `a[0]`
                // and `a[1]`.  This means that payload and indexed fields point to different
                // subtrees, so it's safe to set the payload.
                (JsonPathItem::Key(_), JsonPathItem::Key(_)) => return false,
                (JsonPathItem::Index(_), JsonPathItem::Index(_)) => return false,

                // Types are not compatible. This means that `value_set` could override the
                // subtree, deleting indexed fields.
                (JsonPathItem::Key(_), JsonPathItem::Index(_) | JsonPathItem::WildcardIndex) => {
                    return true;
                }
                (JsonPathItem::Index(_) | JsonPathItem::WildcardIndex, JsonPathItem::Key(_)) => {
                    return true;
                }
            }
        }
    }

    /// Convert the path into a string suitable for use as a filename by adhering to the following
    /// restrictions: max length, limited character set, but still being relatively unique.
    pub fn filename(&self) -> String {
        const MAX_LENGTH: usize = 33;
        const HASH_LENGTH: usize = 24; // In base32 characters, i.e. 5 bits per character.

        let text = self.to_string();
        let mut result = String::with_capacity(MAX_LENGTH);

        BASE32_DNSSEC.encode_append(
            &Sha256::digest(text.as_bytes())[0..(HASH_LENGTH * 5).div_ceil(8)],
            &mut result,
        );
        debug_assert_eq!(result.len(), HASH_LENGTH);

        result.push('-');

        text.chars()
            .map(|c| match c {
                'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => c.to_ascii_lowercase(),
                _ => '_',
            })
            .dedup_by(|&a, &b| a == '_' && b == '_')
            .take(MAX_LENGTH - result.len())
            .for_each(|c| result.push(c));

        debug_assert!(result.len() <= MAX_LENGTH);
        result
    }
}

fn value_get<'a>(
    path: &[JsonPathItem],
    value: Option<&'a Value>,
    result: &mut MultiValue<&'a Value>,
) {
    if let Some((head, tail)) = path.split_first() {
        match (head, value) {
            (JsonPathItem::Key(key), Some(Value::Object(map))) => {
                value_get(tail, map.get(key), result)
            }
            (JsonPathItem::Index(index), Some(Value::Array(array))) => {
                if let Some(value) = array.get(*index) {
                    value_get(tail, Some(value), result);
                }
            }
            (JsonPathItem::WildcardIndex, Some(Value::Array(array))) => array
                .iter()
                .for_each(|value| value_get(tail, Some(value), result)),
            _ => (),
        }
    } else if let Some(value) = value {
        result.push(value);
    }
}

fn value_set(path: &[JsonPathItem], dest: &mut Value, src: &serde_json::Map<String, Value>) {
    if let Some((head, rest)) = path.split_first() {
        match head {
            JsonPathItem::Key(key) => {
                if !dest.is_object() {
                    *dest = Value::Object(serde_json::Map::new());
                }
                let map = dest.as_object_mut().unwrap();
                value_set_map(key, rest, map, src);
            }
            &JsonPathItem::Index(i) => {
                if !dest.is_array() {
                    *dest = Value::Array(Vec::new());
                }
                let array = dest.as_array_mut().unwrap();
                if let Some(v) = array.get_mut(i) {
                    value_set(rest, v, src);
                }
            }
            JsonPathItem::WildcardIndex => {
                if dest.is_array() {
                    for value in dest.as_array_mut().unwrap() {
                        value_set(rest, value, src);
                    }
                } else {
                    *dest = Value::Array(Vec::new());
                }
            }
        }
    } else {
        if !dest.is_object() {
            *dest = Value::Object(serde_json::Map::new());
        }
        let map = dest.as_object_mut().unwrap();
        merge_map(map, src);
    }
}

fn value_set_map(
    key: &str,
    path: &[JsonPathItem],
    dest_map: &mut serde_json::Map<String, Value>,
    src: &serde_json::Map<String, Value>,
) {
    if let Some(value) = dest_map.get_mut(key) {
        value_set(path, value, src);
    } else {
        let mut value = Value::Null;
        value_set(path, &mut value, src);
        dest_map.insert(key.to_string(), value);
    }
}

fn value_remove(
    head: &JsonPathItem,
    rest: &[JsonPathItem],
    value: &mut Value,
    result: &mut MultiValue<Value>,
) {
    if let Some((rest1, restn)) = rest.split_first() {
        match (head, value) {
            (JsonPathItem::Key(k), Value::Object(map)) => {
                if let Some(value) = map.get_mut(k) {
                    value_remove(rest1, restn, value, result);
                }
            }
            (JsonPathItem::Index(i), Value::Array(array)) => {
                if let Some(value) = array.get_mut(*i) {
                    value_remove(rest1, restn, value, result);
                }
            }
            (JsonPathItem::WildcardIndex, Value::Array(array)) => {
                for value in array {
                    value_remove(rest1, restn, value, result);
                }
            }
            _ => (),
        }
    } else {
        match (head, value) {
            (JsonPathItem::Key(k), Value::Object(map)) => {
                if let Some(v) = map.remove(k) {
                    result.push(v);
                }
            }
            (JsonPathItem::Index(_), Value::Array(_)) => {
                // Deleting array indices is not idempotent, so we don't support it.
            }
            (JsonPathItem::WildcardIndex, Value::Array(array)) => {
                result.push(Value::Array(std::mem::take(array)));
            }
            _ => (),
        }
    }
}

fn run_filter<'a>(
    path: &mut JsonPath,
    value: &'a Value,
    filter: &dyn Fn(&JsonPath, &Value) -> bool,
) -> Value {
    match &value {
        Value::Null => value.clone(),
        Value::Bool(_) => value.clone(),
        Value::Number(_) => value.clone(),
        Value::String(_) => value.clone(),
        Value::Array(array) => {
            let mut new_array = Vec::new();
            path.rest.push(JsonPathItem::WildcardIndex);
            for value in array.iter() {
                if filter(path, value) {
                    let value = run_filter(path, value, filter);
                    new_array.push(value);
                }
            }
            path.rest.pop();
            Value::Array(new_array)
        }
        Value::Object(object) => {
            let mut new_object = serde_json::Map::new();
            for (key, value) in object.iter() {
                path.rest.push(JsonPathItem::Key(key.clone()));
                if filter(path, value) {
                    let value = run_filter(path, value, filter);
                    new_object.insert(key.clone(), value);
                }
                path.rest.pop();
            }
            Value::Object(new_object)
        }
    }
}

impl Display for JsonPath {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let write_key = |f: &mut Formatter<'_>, key: &str| {
            if parse::key_needs_quoting(key) {
                write!(f, "\"{key}\"")
            } else {
                f.write_str(key)
            }
        };

        write_key(f, &self.first_key)?;
        for item in &self.rest {
            match item {
                JsonPathItem::Key(key) => {
                    f.write_str(".")?;
                    write_key(f, key)?;
                }
                JsonPathItem::Index(index) => write!(f, "[{index}]")?,
                JsonPathItem::WildcardIndex => f.write_str("[]")?,
            }
        }
        Ok(())
    }
}

impl TryFrom<&str> for JsonPath {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl Serialize for JsonPath {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for JsonPath {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let string = String::deserialize(deserializer)?;
        string
            .parse()
            .map_err(|_| serde::de::Error::custom(format!("Invalid json path: \'{string}\'")))
    }
}

impl JsonSchema for JsonPath {
    fn is_referenceable() -> bool {
        false
    }

    fn schema_name() -> String {
        "JsonPath".to_string()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        String::json_schema(generator)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::utils::check_is_empty;

    fn json(str: &str) -> serde_json::Map<String, Value> {
        serde_json::from_str(str).unwrap()
    }

    #[test]
    fn test_is_affected_by_value_set() {
        assert!(!JsonPath::new("a").is_affected_by_value_set(&json(r#"{"b": 1, "c": 1}"#), None));
        assert!(JsonPath::new("a").is_affected_by_value_set(&json(r#"{"a": 1, "b": 1}"#), None));
        assert!(JsonPath::new("a.x").is_affected_by_value_set(&json(r#"{"a": {"y": 1}}"#), None));
        assert!(!JsonPath::new("a.x").is_affected_by_value_set(&json(r#"{"b": {"x": 1}}"#), None));
    }

    #[test]
    fn test_is_affected_by_value_remove() {
        assert!(JsonPath::new("a").is_affected_by_value_remove(&JsonPath::new("a")));
        assert!(!JsonPath::new("a").is_affected_by_value_remove(&JsonPath::new("b")));
        assert!(JsonPath::new("a.b").is_affected_by_value_remove(&JsonPath::new("a")));
        assert!(JsonPath::new("a.b").is_affected_by_value_remove(&JsonPath::new("a.b")));
        assert!(JsonPath::new("a.b").is_affected_by_value_remove(&JsonPath::new("a.b.c")));
    }

    /// This test checks that `is_affected_by_value_set` and `is_affected_by_value_remove` don't
    /// produce false negatives.
    /// The penalty for a false positive is just degraded performance, but the penalty for a false
    /// negative is inconsistency in the indexed fields.
    #[test]
    fn test_no_false_negatives() {
        let paths: Vec<JsonPath> = ["a", "a.a", "a[]", "a[0]", "a[0].a", "a[0].a[]"]
            .iter()
            .map(|s| s.parse().unwrap())
            .collect();
        let payloads = vec![
            json(r#"{"b": 1}"#),
            json(r#"{"a": 1, "b": 2}"#),
            json(r#"{"a": [], "b": 1}"#),
            json(r#"{"a": [1], "b": 2}"#),
            json(r#"{"a": {}, "b": 1}"#),
            json(r#"{"a": {"a": 1, "b": 2}, "b": 3}"#),
            json(r#"{"a": [{"a": 1, "b": 2}, {"a": 3, "b": 4}], "b": 5}"#),
            json(r#"{"a": [{"a": [1], "b": 2}, {"a": [3], "b": 4}], "b": 5}"#),
        ];

        for init_payload in &payloads {
            for indexed_path in &paths {
                for value_key in &["a", "b"] {
                    check_set(init_payload, indexed_path, None, value_key);
                    for path_to_set in &paths {
                        check_set(init_payload, indexed_path, Some(path_to_set), value_key);
                    }
                }
                for path_to_remove in &paths {
                    check_remove(init_payload, indexed_path, path_to_remove);
                }
            }
        }
    }

    fn check_set(
        init_payload: &serde_json::Map<String, serde_json::Value>,
        indexed_path: &JsonPath,
        path_to_set: Option<&JsonPath>,
        value_key: &str,
    ) {
        let mut new_payload = init_payload.clone();
        let init_values = indexed_path.value_get(init_payload);

        JsonPath::value_set(
            path_to_set,
            &mut new_payload,
            &json(r#"{"value_key": 100}"#),
        );
        let new_values = indexed_path.value_get(&new_payload);

        // Ground truth
        let indexed_value_changed = init_values != new_values;

        // Our prediction
        let is_affected =
            indexed_path.is_affected_by_value_set(&json(r#"{"value_key": 100}"#), path_to_set);

        assert!(
            is_affected || !indexed_value_changed,
            "init_payload: {:?}\nnew_payload: {:?}\nindex_path: {:?}\npath_to_set: {:?}\nvalue_key: {:?}",
            init_payload,
            new_payload,
            indexed_path.to_string(),
            path_to_set.map(|p| p.to_string()),
            value_key,
        );
    }

    fn check_remove(
        init_payload: &serde_json::Map<String, serde_json::Value>,
        indexed_path: &JsonPath,
        path_to_remove: &JsonPath,
    ) {
        let mut new_payload = init_payload.clone();
        let init_values = indexed_path.value_get(init_payload);
        path_to_remove.value_remove(&mut new_payload);
        let new_values = indexed_path.value_get(&new_payload);

        // Ground truth
        let indexed_value_changed = init_values != new_values;

        // Our prediction
        let is_affected = indexed_path.is_affected_by_value_remove(path_to_remove);

        assert!(
            is_affected || !indexed_value_changed,
            "init_payload: {:?}\nnew_payload: {:?}\nindex_path: {:?}\npath_to_remove: {:?}",
            init_payload,
            new_payload,
            indexed_path.to_string(),
            path_to_remove.to_string(),
        );
    }

    #[test]
    fn test_get_nested_value_from_json_map() {
        let map = json(
            r#"
            {
                "a": {"b": {"c": 1}},
                "d": 2
            }
            "#,
        );

        assert_eq!(
            JsonPath::new("a.b").value_get(&map).into_vec(),
            vec![&Value::Object(serde_json::Map::from_iter(vec![(
                "c".to_string(),
                Value::Number(1.into())
            )]))]
        );

        // going deeper
        assert_eq!(
            JsonPath::new("a.b.c").value_get(&map).into_vec(),
            vec![&Value::Number(1.into())]
        );

        // missing path
        assert!(check_is_empty(
            JsonPath::new("a.b.c.d").value_get(&map).iter().copied()
        ));
    }

    #[test]
    fn test_is_empty() {
        let map = json(
            r#"
                {
                   "a": [
                     { "b": 1 },
                     { "b": 2 },
                     { "b": null },
                     { "d": [] },
                     { "d": [] },
                     { "f": null }
                   ]
                }
            "#,
        );
        let multivalue = JsonPath::new("a[].b").value_get(&map);
        let is_empty = check_is_empty(multivalue.iter().copied());

        assert!(!is_empty, "a[].b is not empty");

        let multivalue = JsonPath::new("a[].c").value_get(&map);
        let is_empty = check_is_empty(multivalue.iter().copied());

        assert!(is_empty, "a[].c is empty");

        let multivalue = JsonPath::new("a[].d").value_get(&map);
        let is_empty = check_is_empty(multivalue.iter().copied());
        assert!(is_empty, "a[].d is empty");

        let multivalue = JsonPath::new("a[].f").value_get(&map);
        let is_empty = check_is_empty(multivalue.iter().copied());
        assert!(is_empty, "a[].f is empty");
    }

    #[test]
    fn test_get_nested_array_value_from_json_map() {
        let map = json(
            r#"
            {
                "a": {
                    "b": [
                        { "c": 1 },
                        { "c": 2 },
                        { "d": { "e": 3 } }
                    ]
                },
                "f": 3,
                "g": ["g0", "g1", "g2"]
            }
            "#,
        );

        // get JSON array
        assert_eq!(
            JsonPath::new("a.b").value_get(&map).into_vec(),
            vec![&Value::Array(vec![
                Value::Object(serde_json::Map::from_iter(vec![(
                    "c".to_string(),
                    Value::Number(1.into())
                )])),
                Value::Object(serde_json::Map::from_iter(vec![(
                    "c".to_string(),
                    Value::Number(2.into())
                )])),
                Value::Object(serde_json::Map::from_iter(vec![(
                    "d".to_string(),
                    Value::Object(serde_json::Map::from_iter(vec![(
                        "e".to_string(),
                        Value::Number(3.into())
                    )]))
                )]))
            ])]
        );

        // a.b[] extract all elements from array
        assert_eq!(
            JsonPath::new("a.b[]").value_get(&map).into_vec(),
            vec![
                &Value::Object(serde_json::Map::from_iter(vec![(
                    "c".to_string(),
                    Value::Number(1.into())
                )])),
                &Value::Object(serde_json::Map::from_iter(vec![(
                    "c".to_string(),
                    Value::Number(2.into())
                )])),
                &Value::Object(serde_json::Map::from_iter(vec![(
                    "d".to_string(),
                    Value::Object(serde_json::Map::from_iter(vec![(
                        "e".to_string(),
                        Value::Number(3.into())
                    )]))
                )]))
            ]
        );

        // project scalar field through array
        assert_eq!(
            JsonPath::new("a.b[].c").value_get(&map).into_vec(),
            vec![&Value::Number(1.into()), &Value::Number(2.into())]
        );

        // project object field through array
        assert_eq!(
            JsonPath::new("a.b[].d").value_get(&map).into_vec(),
            vec![&Value::Object(serde_json::Map::from_iter(vec![(
                "e".to_string(),
                Value::Number(3.into())
            )]))]
        );

        // select scalar element from array
        assert_eq!(
            JsonPath::new("a.b[0]").value_get(&map).into_vec(),
            vec![&Value::Object(serde_json::Map::from_iter(vec![(
                "c".to_string(),
                Value::Number(1.into())
            )]))]
        );

        // select scalar object from array different index
        assert_eq!(
            JsonPath::new("a.b[1]").value_get(&map).into_vec(),
            vec![&Value::Object(serde_json::Map::from_iter(vec![(
                "c".to_string(),
                Value::Number(2.into())
            )]))]
        );

        // select field element from array different index
        assert_eq!(
            JsonPath::new("a.b[1].c").value_get(&map).into_vec(),
            vec![&Value::Number(2.into())]
        );

        // select scalar element from array different index
        assert_eq!(
            JsonPath::new("g[2]").value_get(&map).into_vec(),
            vec![&Value::String("g2".to_string())]
        );

        // select object element from array
        assert_eq!(
            JsonPath::new("a.b[2]").value_get(&map).into_vec(),
            vec![&Value::Object(serde_json::Map::from_iter(vec![(
                "d".to_string(),
                Value::Object(serde_json::Map::from_iter(vec![(
                    "e".to_string(),
                    Value::Number(3.into())
                )]))
            )]))]
        );

        // select out of bound index from array
        assert!(check_is_empty(
            JsonPath::new("a.b[3]").value_get(&map).iter().copied()
        ));
    }

    #[test]
    fn test_get_deeply_nested_array_value_from_json_map() {
        let map = json(
            r#"
            {
                "arr1": [
                    {
                        "arr2": [
                            {"a": 1, "b": 2}
                        ]
                    },
                    {
                        "arr2": [
                            {"a": 3, "b": 4},
                            {"a": 5, "b": 6}
                        ]
                    }
                ]
            }
            "#,
        );

        // extract and flatten all elements from arrays
        assert_eq!(
            JsonPath::new("arr1[].arr2[].a").value_get(&map).into_vec(),
            vec![
                &Value::Number(1.into()),
                &Value::Number(3.into()),
                &Value::Number(5.into()),
            ]
        );
    }

    #[test]
    fn test_no_flatten_array_value_from_json_map() {
        let map = json(
            r#"
            {
                "arr": [
                    { "a": [1, 2, 3] },
                    { "a": 4 },
                    { "b": 5 }
                ]
            }
            "#,
        );

        // extract and retain structure for arrays arrays
        assert_eq!(
            JsonPath::new("arr[].a").value_get(&map).into_vec(),
            vec![
                &Value::Array(vec![
                    Value::Number(1.into()),
                    Value::Number(2.into()),
                    Value::Number(3.into()),
                ]),
                &Value::Number(4.into()),
            ]
        );

        // expect an array as leaf, ignore non arrays
        assert_eq!(
            JsonPath::new("arr[].a[]").value_get(&map).into_vec(),
            vec![
                &Value::Number(1.into()),
                &Value::Number(2.into()),
                &Value::Number(3.into()),
            ]
        );
    }

    #[test]
    fn test_get_null_and_absent_values() {
        let map = json(
            r#"
            {
                "a": null,
                "b": [null, null],
                "c": []
            }
            "#,
        );

        assert_eq!(
            JsonPath::new("a").value_get(&map).as_slice(),
            &[&Value::Null],
        );

        assert!(JsonPath::new("a[]").value_get(&map).is_empty());

        assert_eq!(
            JsonPath::new("b").value_get(&map).as_slice(),
            &[&Value::Array(vec![Value::Null, Value::Null])],
        );

        assert_eq!(
            JsonPath::new("b[]").value_get(&map).as_slice(),
            &[&Value::Null, &Value::Null],
        );

        assert_eq!(
            JsonPath::new("c").value_get(&map).as_slice(),
            &[&Value::Array(vec![])],
        );

        assert!(JsonPath::new("c[]").value_get(&map).is_empty());

        assert!(JsonPath::new("d").value_get(&map).is_empty());

        assert!(JsonPath::new("d[]").value_get(&map).is_empty());
    }

    #[test]
    fn test_filter_json() {
        let map = json(
            r#"
            {
                "a": {
                    "b": [
                        { "c": 1 },
                        { "c": 2 },
                        { "d": { "e": 3 } }
                    ]
                },
                "f": 3,
                "g": ["g0", "g1", "g2"]
            }
            "#,
        );

        let res = JsonPath::value_filter(&map, |path, _value| {
            let path = path.to_string();
            path.starts_with("a.b[].c") || "a.b[].c".starts_with(&path)
        });

        assert_eq!(
            res,
            json(
                r#"
                {
                    "a": {
                        "b": [
                            { "c": 1 },
                            { "c": 2 },
                            {}
                        ]
                    }
                }
                "#,
            ),
        );
    }

    #[test]
    fn test_check_include_pattern() {
        assert!(JsonPath::new("a.b.c").check_include_pattern(&JsonPath::new("a.b.c")));
        assert!(JsonPath::new("a.b.c").check_include_pattern(&JsonPath::new("a.b")));
        assert!(!JsonPath::new("a.b.c").check_include_pattern(&JsonPath::new("a.b.d")));
        assert!(JsonPath::new("a.b.c").check_include_pattern(&JsonPath::new("a")));
        assert!(JsonPath::new("a").check_include_pattern(&JsonPath::new("a.d")));
    }

    #[test]
    fn test_check_exclude_pattern() {
        assert!(JsonPath::new("a.b.c").check_exclude_pattern(&JsonPath::new("a.b.c")));
        assert!(!JsonPath::new("a.b.c").check_exclude_pattern(&JsonPath::new("a.b")));
        assert!(!JsonPath::new("a.b.c").check_exclude_pattern(&JsonPath::new("a.b.d")));
        assert!(!JsonPath::new("a.b.c").check_exclude_pattern(&JsonPath::new("a")));
        assert!(JsonPath::new("a").check_exclude_pattern(&JsonPath::new("a.d")));
    }

    #[test]
    fn test_set_value_to_json_with_empty_key() {
        let mut map = json(
            r#"
            {
                "a": {
                    "b": [
                        { "c": 1 },
                        { "c": 2 },
                        { "d": { "e": 3 } }
                    ]
                },
                "f": 3,
                "g": ["g0", "g1", "g2"]
            }
            "#,
        );

        JsonPath::value_set(None, &mut map, &json(r#"{"c": 5}"#));

        assert_eq!(
            map,
            json(
                r#"
                {
                    "a": {
                        "b": [
                            { "c": 1 },
                            { "c": 2 },
                            { "d": { "e": 3 } }
                        ]
                    },
                    "f": 3,
                    "g": ["g0", "g1", "g2"],
                    "c": 5
                }
                "#,
            ),
        );
    }

    #[test]
    fn test_set_value_to_json_with_one_level_key() {
        let mut map = json(
            r#"
            {
                "a": {
                    "b": [
                        { "c": 1 },
                        { "c": 2 },
                        { "d": { "e": 3 } }
                    ]
                },
                "f": 3,
                "g": ["g0", "g1", "g2"]
            }
            "#,
        );

        JsonPath::value_set(Some(&JsonPath::new("a")), &mut map, &json(r#"{"b": 5}"#));

        assert_eq!(
            map,
            json(
                r#"
                {
                    "a": {
                        "b": 5
                    },
                    "f": 3,
                    "g": ["g0", "g1", "g2"]
                }
                "#,
            ),
        );
    }

    #[test]
    fn test_set_value_to_json_with_array_index() {
        let mut map = json(
            r#"
            {
                "a": {
                    "b": [
                        { "c": 1 },
                        { "c": 2 },
                        { "d": { "e": 3 } }
                    ]
                },
                "f": 3,
                "g": ["g0", "g1", "g2"]
            }
            "#,
        );

        JsonPath::value_set(
            Some(&JsonPath::new("a.b[1]")),
            &mut map,
            &json(r#"{"c": 5}"#),
        );

        assert_eq!(
            map,
            json(
                r#"
                {
                    "a": {
                        "b": [
                            { "c": 1 },
                            { "c": 5 },
                            { "d": { "e": 3 } }
                        ]
                    },
                    "f": 3,
                    "g": ["g0", "g1", "g2"]
                }
                "#,
            ),
        );
    }

    #[test]
    fn test_set_value_to_json_with_empty_src() {
        let mut map = json(
            r#"
            {
                "a": {
                    "b": [
                        { "c": 1 },
                        { "c": 2 },
                        { "d": { "e": 3 } }
                    ]
                },
                "f": 3,
                "g": ["g0", "g1", "g2"]
            }
            "#,
        );

        JsonPath::value_set(Some(&JsonPath::new("a.b[1]")), &mut map, &json("{}"));

        assert_eq!(
            map,
            json(
                r#"
                {
                    "a": {
                        "b": [
                            { "c": 1 },
                            { "c": 2 },
                            { "d": { "e": 3 } }
                        ]
                    },
                    "f": 3,
                    "g": ["g0", "g1", "g2"]
                }
                "#,
            ),
        );
    }

    #[test]
    fn test_set_value_to_json_with_empty_dest() {
        let mut map = json("{}");

        JsonPath::value_set(None, &mut map, &json(r#"{"c": 1}"#));

        assert_eq!(map, json(r#"{"c": 1}"#));
    }

    #[test]
    fn test_set_value_to_json_with_empty_dest_nested_key() {
        let mut map = json("{}");

        JsonPath::value_set(
            Some(&JsonPath::new("key1.key2")),
            &mut map,
            &json(r#"{"c": 1}"#),
        );

        assert_eq!(map, json(r#"{"key1": {"key2": {"c": 1}}}"#));
    }

    #[test]
    fn test_set_value_to_json_with_empty_dest_nested_array_index_key() {
        let mut map = json("{}");
        let src = json(r#"{"c": 1}"#);
        JsonPath::value_set(Some(&JsonPath::new("key1.key2[3]")), &mut map, &src);
        assert_eq!(map, json(r#" {"key1": {"key2": []}} "#));

        let mut map = json("{}");
        let src = json(r#"{"c": 1}"#);
        JsonPath::value_set(Some(&JsonPath::new("key1.key2[0]")), &mut map, &src);
        assert_eq!(map, json(r#" {"key1": {"key2": []}} "#));
    }

    #[test]
    fn test_expand_payload_with_non_existing_array() {
        let mut map = json("{}");

        JsonPath::value_set(
            Some(&JsonPath::new("key1.key2[].key3")),
            &mut map,
            &json(r#"{"c": 1}"#),
        );

        assert_eq!(map, json(r#"{"key1": {"key2": [] }}"#));
    }

    #[test]
    fn test_replace_scalar_key_with_object() {
        let mut map = json(r#"{"a": 10}"#);

        JsonPath::value_set(
            Some(&JsonPath::new("a.b.c")),
            &mut map,
            &json(r#"{"x": 1}"#),
        );

        assert_eq!(map, json(r#"{"a": {"b": {"c": {"x": 1}}}}"#));
    }

    #[test]
    fn test_remove_key() {
        let mut payload = json(
            r#"
            {
                "a": 1,
                "b": {
                    "c": 123,
                    "e": {
                        "f": [1,2,3],
                        "g": 7,
                        "h": "text",
                        "i": [
                            {
                                "j": 1,
                                "k": 2
                            },
                            {
                                "j": 3,
                                "k": 4
                            }
                        ]
                    }
                }
            }
            "#,
        );
        let removed = JsonPath::new("b.c").value_remove(&mut payload).into_vec();
        assert_eq!(removed, vec![Value::Number(123.into())]);
        assert_ne!(payload, Default::default());

        let removed = JsonPath::new("b.e.i[0].j")
            .value_remove(&mut payload)
            .into_vec();
        assert_eq!(removed, vec![Value::Number(1.into())]);
        assert_ne!(payload, Default::default());

        let removed = JsonPath::new("b.e.i[].k")
            .value_remove(&mut payload)
            .into_vec();
        assert_eq!(
            removed,
            vec![Value::Number(2.into()), Value::Number(4.into())]
        );
        assert_ne!(payload, Default::default());

        let removed = JsonPath::new("b.e.i[]")
            .value_remove(&mut payload)
            .into_vec();
        assert_eq!(
            removed,
            vec![Value::Array(vec![
                Value::Object(serde_json::Map::from_iter(vec![])),
                Value::Object(serde_json::Map::from_iter(vec![(
                    "j".to_string(),
                    Value::Number(3.into())
                ),])),
            ])]
        );
        assert_ne!(payload, Default::default());

        let removed = JsonPath::new("b.e.i").value_remove(&mut payload).into_vec();
        assert_eq!(removed, vec![Value::Array(vec![])]);
        assert_ne!(payload, Default::default());

        let removed = JsonPath::new("b.e.f").value_remove(&mut payload).into_vec();
        assert_eq!(
            removed,
            vec![Value::Array(vec![1.into(), 2.into(), 3.into()])]
        );
        assert_ne!(payload, Default::default());

        let removed = JsonPath::new("k").value_remove(&mut payload);
        assert!(check_is_empty(&removed));
        assert_ne!(payload, Default::default());

        let removed = JsonPath::new("b.e.l").value_remove(&mut payload);
        assert!(check_is_empty(&removed));
        assert_ne!(payload, Default::default());

        let removed = JsonPath::new("a").value_remove(&mut payload).into_vec();
        assert_eq!(removed, vec![Value::Number(1.into())]);
        assert_ne!(payload, Default::default());

        let removed = JsonPath::new("b.e").value_remove(&mut payload).into_vec();
        assert_eq!(
            removed,
            vec![Value::Object(serde_json::Map::from_iter(vec![
                // ("f".to_string(), Value::Array(vec![1.into(), 2.into(), 3.into()])), has been removed
                ("g".to_string(), Value::Number(7.into())),
                ("h".to_string(), Value::String("text".to_owned())),
            ]))]
        );
        assert_ne!(payload, Default::default());

        let removed = JsonPath::new("b").value_remove(&mut payload).into_vec();
        assert_eq!(
            removed,
            vec![Value::Object(serde_json::Map::from_iter(vec![]))]
        ); // empty object left
        assert_eq!(payload, Default::default());
    }

    #[test]
    fn test_filename() {
        assert_eq!(
            JsonPath::new("foo").filename(),
            "5gjb8qr8vv38vucr8ku1qc21-foo",
        );
        assert_eq!(
            JsonPath::new("a.\"b c\".d").filename(),
            "59if87e118rvurkl7j0q10mc-a_b_c_d",
        );
        assert_eq!(
            JsonPath::new("a.b[0][]").filename(),
            "vk82udqfa8drecufa7j5mo6v-a_b_0_",
        );
        assert_eq!(
            JsonPath::new("really.loooooooooooooooooooooooooooooooooooooooooooong.path").filename(),
            "sh47i3hjfgn44gch5jvm3bum-really_l",
        );
        assert_eq!(
            JsonPath::new("Müesli").filename(),
            "4huj4rn1fflrtriqo0tieqhh-m_esli",
        );
    }
}
