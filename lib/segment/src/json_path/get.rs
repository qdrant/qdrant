use serde_json::Value;

use super::{JsonPath, JsonPathItem};

/// Focus on value references according to path
/// Flatten intermediate arrays but keep leaf array values on demand.
/// E.g
/// {
///   "arr": [
///       { "a": [1, 2, 3] },
///       { "a": 4 },
///       { "b": 5 }
///   ]
/// }
///
/// path: "arr[].a"   => Vec![Value::Array[ 1, 2, 3], 4]
/// path: "arr[].a[]" => Vec![ 1, 2, 3, 4]
///
/// performance: the function could be improved by using the Entry API instead of BTreeMap.get
pub fn get_value_from_json_map<'a>(
    path: &JsonPath,
    json_map: &'a serde_json::Map<String, Value>,
) -> smallvec::SmallVec<[&'a Value; 1]> {
    let mut result = smallvec::SmallVec::new();
    if let Some(value) = json_map.get(&path.first_key) {
        json_value_get(&path.rest, Some(value), &mut result);
    }
    result
}

fn json_value_get<'a>(
    path: &[JsonPathItem],
    value: Option<&'a Value>,
    result: &mut smallvec::SmallVec<[&'a Value; 1]>,
) {
    if let Some((head, tail)) = path.split_first() {
        match (head, value) {
            (JsonPathItem::Key(key), Some(Value::Object(map))) => {
                json_value_get(tail, map.get(key), result)
            }
            (JsonPathItem::Index(index), Some(Value::Array(array))) => {
                if let Some(value) = array.get(*index) {
                    json_value_get(tail, Some(value), result);
                }
            }
            (JsonPathItem::WildcardIndex, Some(Value::Array(array))) => array
                .iter()
                .for_each(|value| json_value_get(tail, Some(value), result)),
            _ => (),
        }
    } else if let Some(value) = value {
        result.push(value);
    }
}

#[cfg(test)]
mod tests {
    use super::super::path;
    use super::*;
    use crate::common::utils::check_is_empty;

    #[test]
    fn test_get_nested_value_from_json_map() {
        let map = serde_json::from_str::<serde_json::Map<String, Value>>(
            r#"
            {
                "a": {
                    "b": {
                        "c": 1
                    }
                },
                "d": 2
            }
            "#,
        )
        .unwrap();

        assert_eq!(
            get_value_from_json_map(&path("a.b"), &map).into_vec(),
            vec![&Value::Object(serde_json::Map::from_iter(vec![(
                "c".to_string(),
                Value::Number(1.into())
            )]))]
        );

        // going deeper
        assert_eq!(
            get_value_from_json_map(&path("a.b.c"), &map).into_vec(),
            vec![&Value::Number(1.into())]
        );

        // missing path
        assert!(check_is_empty(
            get_value_from_json_map(&path("a.b.c.d"), &map)
                .iter()
                .copied()
        ));
    }

    #[test]
    fn test_get_nested_array_value_from_json_map() {
        let map = serde_json::from_str::<serde_json::Map<String, Value>>(
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

        // get JSON array
        assert_eq!(
            get_value_from_json_map(&path("a.b"), &map).into_vec(),
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
            get_value_from_json_map(&path("a.b[]"), &map).into_vec(),
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
            get_value_from_json_map(&path("a.b[].c"), &map).into_vec(),
            vec![&Value::Number(1.into()), &Value::Number(2.into())]
        );

        // project object field through array
        assert_eq!(
            get_value_from_json_map(&path("a.b[].d"), &map).into_vec(),
            vec![&Value::Object(serde_json::Map::from_iter(vec![(
                "e".to_string(),
                Value::Number(3.into())
            )]))]
        );

        // select scalar element from array
        assert_eq!(
            get_value_from_json_map(&path("a.b[0]"), &map).into_vec(),
            vec![&Value::Object(serde_json::Map::from_iter(vec![(
                "c".to_string(),
                Value::Number(1.into())
            )]))]
        );

        // select scalar object from array different index
        assert_eq!(
            get_value_from_json_map(&path("a.b[1]"), &map).into_vec(),
            vec![&Value::Object(serde_json::Map::from_iter(vec![(
                "c".to_string(),
                Value::Number(2.into())
            )]))]
        );

        // select field element from array different index
        assert_eq!(
            get_value_from_json_map(&path("a.b[1].c"), &map).into_vec(),
            vec![&Value::Number(2.into())]
        );

        // select scalar element from array different index
        assert_eq!(
            get_value_from_json_map(&path("g[2]"), &map).into_vec(),
            vec![&Value::String("g2".to_string())]
        );

        // select object element from array
        assert_eq!(
            get_value_from_json_map(&path("a.b[2]"), &map).into_vec(),
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
            get_value_from_json_map(&path("a.b[3]"), &map)
                .iter()
                .copied()
        ));

        // select bad index from array
        // XXX: we are not allowing invalid json paths
        // assert!(check_is_empty(
        //     get_value_from_json_map(&path("a.b[z]"), &map)
        //         .iter()
        //         .copied()
        // ));
    }

    #[test]
    fn test_get_deeply_nested_array_value_from_json_map() {
        let map = serde_json::from_str::<serde_json::Map<String, Value>>(
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
        )
        .unwrap();

        // extract and flatten all elements from arrays
        assert_eq!(
            get_value_from_json_map(&path("arr1[].arr2[].a"), &map).into_vec(),
            vec![
                &Value::Number(1.into()),
                &Value::Number(3.into()),
                &Value::Number(5.into()),
            ]
        );
    }

    #[test]
    fn test_no_flatten_array_value_from_json_map() {
        let map = serde_json::from_str::<serde_json::Map<String, Value>>(
            r#"
            {
                "arr": [
                    { "a": [1, 2, 3] },
                    { "a": 4 },
                    { "b": 5 }
                ]
            }
            "#,
        )
        .unwrap();

        // extract and retain structure for arrays arrays
        assert_eq!(
            get_value_from_json_map(&path("arr[].a"), &map).into_vec(),
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
            get_value_from_json_map(&path("arr[].a[]"), &map).into_vec(),
            vec![
                &Value::Number(1.into()),
                &Value::Number(2.into()),
                &Value::Number(3.into()),
            ]
        );
    }
}
