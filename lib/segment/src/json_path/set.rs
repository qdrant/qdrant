use serde_json::Value;

use super::{JsonPath, JsonPathItem};
use crate::common::utils::merge_map;

pub fn set_value_to_json_map_new(
    path: Option<&JsonPath>,
    dest: &mut serde_json::Map<String, Value>,
    src: &serde_json::Map<String, Value>,
) {
    if let Some(path) = path {
        json_value_set_map(&path.first_key, &path.rest, dest, src);
    } else {
        merge_map(dest, src);
    }
}

fn json_value_set(path: &[JsonPathItem], dest: &mut Value, src: &serde_json::Map<String, Value>) {
    if let Some((head, rest)) = path.split_first() {
        match head {
            JsonPathItem::Key(key) => {
                if !dest.is_object() {
                    *dest = Value::Object(serde_json::Map::new());
                }
                let map = dest.as_object_mut().unwrap();
                json_value_set_map(key, rest, map, src);
            }
            &JsonPathItem::Index(i) => {
                if !dest.is_array() {
                    *dest = Value::Array(Vec::new());
                }
                let array = dest.as_array_mut().unwrap();
                if i >= array.len() {
                    array.resize_with(i + 1, || Value::Null);
                }
                json_value_set(rest, array.get_mut(i).unwrap(), src);
            }
            JsonPathItem::WildcardIndex => {
                if dest.is_array() {
                    for value in dest.as_array_mut().unwrap() {
                        json_value_set(rest, value, src);
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

fn json_value_set_map(
    key: &str,
    path: &[JsonPathItem],
    dest_map: &mut serde_json::Map<String, Value>,
    src: &serde_json::Map<String, Value>,
) {
    if let Some(value) = dest_map.get_mut(key) {
        json_value_set(path, value, src);
    } else {
        let mut value = Value::Null;
        json_value_set(path, &mut value, src);
        dest_map.insert(key.to_string(), value);
    }
}

#[cfg(test)]
mod tests {
    use super::super::path;
    use super::*;

    #[test]
    fn test_set_value_to_json_with_empty_key() {
        let mut map = serde_json::from_str::<serde_json::Map<String, Value>>(
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
        )
        .unwrap();

        let src = serde_json::from_str::<serde_json::Map<String, Value>>(
            r#"
            { "c": 5 }
            "#,
        )
        .unwrap();

        set_value_to_json_map_new(None, &mut map, &src);

        assert_eq!(
            map,
            serde_json::from_str::<serde_json::Map<String, Value>>(
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
            )
            .unwrap()
        );
    }

    #[test]
    fn test_set_value_to_json_with_one_level_key() {
        let mut map = serde_json::from_str::<serde_json::Map<String, Value>>(
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
        )
        .unwrap();

        let src = serde_json::from_str::<serde_json::Map<String, Value>>(
            r#"
            { "b": 5 }
            "#,
        )
        .unwrap();

        set_value_to_json_map_new(Some(&path("a")), &mut map, &src);

        assert_eq!(
            map,
            serde_json::from_str::<serde_json::Map<String, Value>>(
                r#"
                {
                    "a": {
                        "b": 5
                    },
                    "f": 3,
                    "g": ["g0", "g1", "g2"]
                }
                "#,
            )
            .unwrap()
        );
    }

    #[test]
    fn test_set_value_to_json_with_array_index() {
        let mut map = serde_json::from_str::<serde_json::Map<String, Value>>(
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
        )
        .unwrap();

        let src = serde_json::from_str::<serde_json::Map<String, Value>>(
            r#"
            { "c": 5 }
            "#,
        )
        .unwrap();

        set_value_to_json_map_new(Some(&path("a.b[1]")), &mut map, &src);

        assert_eq!(
            map,
            serde_json::from_str::<serde_json::Map<String, Value>>(
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
            )
            .unwrap()
        );
    }

    #[test]
    fn test_set_value_to_json_with_empty_src() {
        let mut map = serde_json::from_str::<serde_json::Map<String, Value>>(
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
        )
        .unwrap();

        let src = serde_json::from_str::<serde_json::Map<String, Value>>(
            r#"
            {}
            "#,
        )
        .unwrap();

        set_value_to_json_map_new(Some(&path("a.b[1]")), &mut map, &src);

        assert_eq!(
            map,
            serde_json::from_str::<serde_json::Map<String, Value>>(
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
            )
            .unwrap()
        );
    }

    #[test]
    fn test_set_value_to_json_with_empty_dest() {
        let mut map = serde_json::from_str::<serde_json::Map<String, Value>>(
            r#"
            {
            }
            "#,
        )
        .unwrap();

        let src = serde_json::from_str::<serde_json::Map<String, Value>>(
            r#"
            {"c": 1}
            "#,
        )
        .unwrap();

        set_value_to_json_map_new(None, &mut map, &src);

        assert_eq!(
            map,
            serde_json::from_str::<serde_json::Map<String, Value>>(
                r#"
                {
                    "c": 1
                }
                "#,
            )
            .unwrap()
        );
    }

    #[test]
    fn test_set_value_to_json_with_empty_dest_nested_key() {
        let mut map = serde_json::from_str::<serde_json::Map<String, Value>>(
            r#"
            {
            }
            "#,
        )
        .unwrap();

        let src = serde_json::from_str::<serde_json::Map<String, Value>>(
            r#"
            {"c": 1}
            "#,
        )
        .unwrap();

        set_value_to_json_map_new(Some(&path("key1.key2")), &mut map, &src);

        assert_eq!(
            map,
            serde_json::from_str::<serde_json::Map<String, Value>>(
                r#"
                {
                    "key1": {"key2": { "c": 1 } }
                }
                "#,
            )
            .unwrap()
        );
    }

    #[test]
    fn test_set_value_to_json_with_empty_dest_nested_array_index_key() {
        let mut map = serde_json::from_str::<serde_json::Map<String, Value>>(
            r#"
            {
            }
            "#,
        )
        .unwrap();

        let src = serde_json::from_str::<serde_json::Map<String, Value>>(
            r#"
            {"c": 1}
            "#,
        )
        .unwrap();

        set_value_to_json_map_new(Some(&path("key1.key2[3]")), &mut map, &src);

        assert_eq!(
            map,
            serde_json::from_str::<serde_json::Map<String, Value>>(
                r#"
                {
                    "key1": {"key2": [null, null, null, { "c": 1 }] }
                }
                "#,
            )
            .unwrap()
        );
    }

    #[test]
    fn test_expand_payload_with_non_existing_array() {
        let mut map = serde_json::from_str::<serde_json::Map<String, Value>>(
            r#"
            {
            }
            "#,
        )
        .unwrap();

        let src = serde_json::from_str::<serde_json::Map<String, Value>>(
            r#"
            {"c": 1}
            "#,
        )
        .unwrap();

        set_value_to_json_map_new(Some(&path("key1.key2[].key3")), &mut map, &src);

        assert_eq!(
            map,
            serde_json::from_str::<serde_json::Map<String, Value>>(
                r#"
                {
                    "key1": { "key2": [] }
                }
                "#,
            )
            .unwrap()
        );
    }

    #[test]
    fn test_replace_scalar_key_with_object() {
        let mut map = serde_json::from_str::<serde_json::Map<String, Value>>(
            r#"
            {"a": 10}
            "#,
        )
        .unwrap();

        let src = serde_json::from_str::<serde_json::Map<String, Value>>(
            r#"
            {"x": 1}
            "#,
        )
        .unwrap();

        set_value_to_json_map_new(Some(&path("a.b.c")), &mut map, &src);

        assert_eq!(
            map,
            serde_json::from_str::<serde_json::Map<String, Value>>(
                r#"
                {
                    "a": {"b": {"c": {"x": 1}}}
                }
                "#,
            )
            .unwrap()
        );
    }
}
