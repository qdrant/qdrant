use serde_json::Value;

use super::{JsonPath, JsonPathItem};

/// Remove value at a given JSON path from JSON map
///
/// performance: the function could be improved by using the Entry API instead of BTreeMap.get_mut
pub fn remove_value_from_json_map(
    path: &JsonPath,
    json_map: &mut serde_json::Map<String, Value>,
) -> smallvec::SmallVec<[Value; 1]> {
    let mut result = smallvec::SmallVec::new();
    if let Some((rest1, restn)) = path.rest.split_first() {
        if let Some(value) = json_map.get_mut(&path.first_key) {
            json_value_remove(rest1, restn, value, &mut result);
        }
    } else if let Some(value) = json_map.remove(&path.first_key) {
        result.push(value);
    }
    result
}

fn json_value_remove(
    head: &JsonPathItem,
    rest: &[JsonPathItem],
    value: &mut Value,
    result: &mut smallvec::SmallVec<[Value; 1]>,
) {
    if let Some((rest1, restn)) = rest.split_first() {
        match (head, value) {
            (JsonPathItem::Key(k), Value::Object(map)) => {
                if let Some(value) = map.get_mut(k) {
                    json_value_remove(rest1, restn, value, result);
                }
            }
            (JsonPathItem::Index(i), Value::Array(array)) => {
                if let Some(value) = array.get_mut(*i) {
                    json_value_remove(rest1, restn, value, result);
                }
            }
            (JsonPathItem::WildcardIndex, Value::Array(array)) => {
                for value in array {
                    json_value_remove(rest1, restn, value, result);
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

#[cfg(test)]
mod tests {
    use super::super::path;
    use super::*;
    use crate::common::utils::check_is_empty;

    #[test]
    fn test_remove_key() {
        let mut map = serde_json::from_str::<serde_json::Map<String, Value>>(
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
        )
        .unwrap();
        let removed = remove_value_from_json_map(&path("b.c"), &mut map).to_vec();
        assert_eq!(removed, vec![Value::Number(123.into())]);
        assert_ne!(map, Default::default());

        let removed = remove_value_from_json_map(&path("b.e.f[1]"), &mut map).to_vec();
        assert_eq!(removed, vec![Value::Number(2.into())]);
        assert_ne!(map, Default::default());

        let removed = remove_value_from_json_map(&path("b.e.i[0].j"), &mut map).to_vec();
        assert_eq!(removed, vec![Value::Number(1.into())]);
        assert_ne!(map, Default::default());

        let removed = remove_value_from_json_map(&path("b.e.i[].k"), &mut map).to_vec();
        assert_eq!(
            removed,
            vec![Value::Number(2.into()), Value::Number(4.into())]
        );
        assert_ne!(map, Default::default());

        let removed = remove_value_from_json_map(&path("b.e.i[]"), &mut map).to_vec();
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
        assert_ne!(map, Default::default());

        let removed = remove_value_from_json_map(&path("b.e.i"), &mut map).to_vec();
        assert_eq!(removed, vec![Value::Array(vec![])]);
        assert_ne!(map, Default::default());

        let removed = remove_value_from_json_map(&path("b.e.f"), &mut map).to_vec();
        assert_eq!(removed, vec![Value::Array(vec![1.into(), 3.into()])]);
        assert_ne!(map, Default::default());

        let removed = remove_value_from_json_map(&path("k"), &mut map);
        assert!(check_is_empty(&removed));
        assert_ne!(map, Default::default());

        // XXX: we are not allowing empty keys
        // let removed = remove_value_from_json_map_new(&path(""), &mut map);
        // assert!(check_is_empty(&removed));
        // assert_ne!(map, Default::default());

        let removed = remove_value_from_json_map(&path("b.e.l"), &mut map);
        assert!(check_is_empty(&removed));
        assert_ne!(map, Default::default());

        let removed = remove_value_from_json_map(&path("a"), &mut map).to_vec();
        assert_eq!(removed, vec![Value::Number(1.into())]);
        assert_ne!(map, Default::default());

        let removed = remove_value_from_json_map(&path("b.e"), &mut map).to_vec();
        assert_eq!(
            removed,
            vec![Value::Object(serde_json::Map::from_iter(vec![
                // ("f".to_string(), Value::Array(vec![1.into(), 2.into(), 3.into()])), has been removed
                ("g".to_string(), Value::Number(7.into())),
                ("h".to_string(), Value::String("text".to_owned())),
            ]))]
        );
        assert_ne!(map, Default::default());

        let removed = remove_value_from_json_map(&path("b"), &mut map).to_vec();
        assert_eq!(
            removed,
            vec![Value::Object(serde_json::Map::from_iter(vec![]))]
        ); // empty object left
        assert_eq!(map, Default::default());
    }
}
