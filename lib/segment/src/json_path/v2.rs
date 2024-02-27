use std::fmt::{Display, Formatter};

use schemars::gen::SchemaGenerator;
use schemars::schema::Schema;
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use validator::ValidationError;

use super::JsonPathInterface;
use crate::common::anonymize::Anonymize;
use crate::common::utils::{merge_map, MultiValue};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JsonPathV2 {
    pub first_key: String,
    pub rest: Vec<JsonPathItem>,
}

#[derive(Debug, PartialEq, Clone, Hash, Eq)]
pub enum JsonPathItem {
    /// A key in a JSON object, e.g. ".foo"
    Key(String),
    /// An index in a JSON array, e.g. "[3]"
    Index(usize),
    /// All indices in a JSON array, i.e. "[]"
    WildcardIndex,
}

impl JsonPathInterface for JsonPathV2 {
    fn value_get<'a>(&self, json_map: &'a serde_json::Map<String, Value>) -> MultiValue<&'a Value> {
        let mut result = MultiValue::new();
        if let Some(value) = json_map.get(&self.first_key) {
            value_get(&self.rest, Some(value), &mut result);
        }
        result
    }

    fn value_set<'a>(
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

    fn value_remove(&self, json_map: &mut serde_json::Map<String, Value>) -> MultiValue<Value> {
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

    fn value_filter(
        json_map: &serde_json::Map<String, Value>,
        filter: impl Fn(&Self, &Value) -> bool,
    ) -> serde_json::Map<String, Value> {
        let mut new_map = serde_json::Map::new();
        let mut path = JsonPathV2 {
            first_key: "".to_string(),
            rest: Vec::new(),
        };
        for (key, value) in json_map.iter() {
            path.first_key = key.clone();
            if filter(&path, value) {
                let value = run_filter(&mut path, value, &filter);
                new_map.insert(key.clone(), value);
            }
        }
        new_map
    }

    fn validate_not_empty(&self) -> Result<(), ValidationError> {
        Ok(()) // It's validated during parsing
    }

    fn head(&self) -> &str {
        &self.first_key
    }

    fn strip_wildcard_suffix(&self) -> Self {
        match self.rest.split_last() {
            Some((JsonPathItem::WildcardIndex, rest)) => JsonPathV2 {
                first_key: self.first_key.clone(),
                rest: rest.to_vec(),
            },
            _ => self.clone(),
        }
    }

    fn strip_prefix(&self, prefix: &Self) -> Option<Self> {
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
                    return Some(JsonPathV2 {
                        first_key: k.clone(),
                        rest: self_it.skip(1).cloned().collect(),
                    })
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

    fn extend(&self, other: &Self) -> Self {
        let mut rest = Vec::with_capacity(self.rest.len() + 1 + other.rest.len());
        rest.extend_from_slice(&self.rest);
        rest.push(JsonPathItem::Key(other.first_key.clone()));
        rest.extend_from_slice(&other.rest);
        JsonPathV2 {
            first_key: self.first_key.clone(),
            rest,
        }
    }

    fn array_key(&self) -> Self {
        let mut result = JsonPathV2 {
            first_key: self.first_key.clone(),
            rest: Vec::with_capacity(self.rest.len() + 1),
        };
        result.rest.extend_from_slice(&self.rest);
        if result.rest.last() != Some(&JsonPathItem::WildcardIndex) {
            result.rest.push(JsonPathItem::WildcardIndex);
        }
        result
    }

    fn check_include_pattern(&self, pattern: &Self) -> bool {
        self.first_key == pattern.first_key
            && self.rest.iter().zip(&pattern.rest).all(|(a, b)| a == b)
    }

    fn check_exclude_pattern(&self, pattern: &Self) -> bool {
        self.first_key == pattern.first_key && pattern.rest.starts_with(&self.rest)
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
            (JsonPathItem::Index(idx), Value::Array(array)) => {
                if idx < &array.len() {
                    result.push(array.remove(*idx));
                }
            }
            (JsonPathItem::WildcardIndex, Value::Array(array)) => {
                result.push(Value::Array(std::mem::take(array)));
            }
            _ => (),
        }
    }
}

fn run_filter<'a>(
    path: &mut JsonPathV2,
    value: &'a Value,
    filter: &dyn Fn(&JsonPathV2, &Value) -> bool,
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

impl Display for JsonPathV2 {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let write_key = |f: &mut Formatter<'_>, key: &str| {
            if super::parse::key_needs_quoting(key) {
                write!(f, "\"{}\"", key)
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
                JsonPathItem::Index(index) => write!(f, "[{}]", index)?,
                JsonPathItem::WildcardIndex => f.write_str("[]")?,
            }
        }
        Ok(())
    }
}

impl TryFrom<&str> for JsonPathV2 {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl Serialize for JsonPathV2 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for JsonPathV2 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(|_| serde::de::Error::custom("Invalid json path"))
    }
}

impl JsonSchema for JsonPathV2 {
    fn is_referenceable() -> bool {
        false
    }

    fn schema_name() -> String {
        "JsonPath".to_string()
    }

    fn json_schema(gen: &mut SchemaGenerator) -> Schema {
        String::json_schema(gen)
    }
}

impl Anonymize for JsonPathV2 {
    fn anonymize(&self) -> Self {
        self.clone()
    }
}
