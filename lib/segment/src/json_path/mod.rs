use std::fmt::{Display, Formatter};

use schemars::gen::SchemaGenerator;
use schemars::schema::Schema;
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::common::anonymize::Anonymize;

mod filter;
mod get;
mod parse;
mod remove;
mod set;

pub use filter::filter_json_values;
pub use get::get_value_from_json_map;
pub use remove::remove_value_from_json_map;
pub use set::set_value_to_json_map_new;

use self::parse::ParseError;

#[derive(Debug, PartialEq, Clone, Hash, Eq)]
pub struct JsonPath {
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

impl Display for JsonPath {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let write_key = |f: &mut Formatter<'_>, key: &str| {
            if parse::key_needs_quoting(key) {
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

impl JsonPath {
    pub fn strip_suffix(&self, suffix: &[JsonPathItem]) -> Option<JsonPath> {
        Some(JsonPath {
            first_key: self.first_key.clone(),
            rest: self.rest.strip_suffix(suffix)?.to_vec(),
        })
    }

    pub fn strip_prefix(&self, prefix: &JsonPath) -> Option<JsonPath> {
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

    pub fn extend(&self, other: &JsonPath) -> JsonPath {
        let mut rest = Vec::with_capacity(self.rest.len() + 1 + other.rest.len());
        rest.extend_from_slice(&self.rest);
        rest.push(JsonPathItem::Key(other.first_key.clone()));
        rest.extend_from_slice(&other.rest);
        JsonPath {
            first_key: self.first_key.clone(),
            rest,
        }
    }

    pub fn extend_or_new(base: Option<&JsonPath>, other: &JsonPath) -> JsonPath {
        match base {
            Some(base) => base.extend(other),
            None => other.clone(),
        }
    }

    /// Check if a path is included in a list of patterns
    ///
    /// Basically, it checks if either the pattern or path is a prefix of the other.
    pub fn check_include_pattern(&self, other: &JsonPath) -> bool {
        self.first_key == other.first_key && self.rest.iter().zip(&other.rest).all(|(a, b)| a == b)
    }

    /// Check if a path should be excluded by a pattern
    ///
    /// Basically, it checks if pattern is a prefix of path, but not the other way around.
    pub fn check_exclude_pattern(&self, other: &JsonPath) -> bool {
        self.first_key == other.first_key && other.rest.starts_with(&self.rest)
    }
}

impl TryFrom<&str> for JsonPath {
    type Error = ParseError;

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
        String::deserialize(deserializer)?
            .parse()
            .map_err(|_| serde::de::Error::custom("Invalid json path"))
    }
}

impl JsonSchema for JsonPath {
    fn schema_name() -> String {
        "JsonPath".to_string()
    }

    fn json_schema(gen: &mut SchemaGenerator) -> Schema {
        String::json_schema(gen)
    }
}

impl Anonymize for JsonPath {
    fn anonymize(&self) -> JsonPath {
        self.clone()
    }
}

#[cfg(test)]
pub fn path(p: &str) -> JsonPath {
    p.parse().unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_strip_prefix() {
        assert_eq!(
            path("foo.bar.baz").strip_prefix(&path("foo.bar")),
            Some(path("baz"))
        );
    }

    #[test]
    fn test_check_include_pattern() {
        assert!(path("a.b.c").check_include_pattern(&path("a.b.c")));
        assert!(path("a.b.c").check_include_pattern(&path("a.b")));
        assert!(!path("a.b.c").check_include_pattern(&path("a.b.d")));
        assert!(path("a.b.c").check_include_pattern(&path("a")));
        assert!(path("a").check_include_pattern(&path("a.d")));
    }

    #[test]
    fn test_check_exclude_pattern() {
        assert!(path("a.b.c").check_exclude_pattern(&path("a.b.c")));
        assert!(!path("a.b.c").check_exclude_pattern(&path("a.b")));
        assert!(!path("a.b.c").check_exclude_pattern(&path("a.b.d")));
        assert!(!path("a.b.c").check_exclude_pattern(&path("a")));
        assert!(path("a").check_exclude_pattern(&path("a.d")));
    }
}
