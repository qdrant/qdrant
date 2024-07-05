use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use smallvec::SmallVec;

use crate::data_types::named_vectors::NamedVectors;
use crate::data_types::vectors::Vector;
use crate::index::field_index::FieldIndex;
use crate::types::PayloadKeyType;

pub type IndexesMap = HashMap<PayloadKeyType, Vec<FieldIndex>>;

/// A container for JSON values, optimized for the common case of a single value.
pub type MultiValue<T> = SmallVec<[T; 1]>;

pub fn check_is_empty<'a>(values: impl IntoIterator<Item = &'a Value>) -> bool {
    values.into_iter().all(|x| match x {
        serde_json::Value::Null => true,
        serde_json::Value::Array(arr) => arr.is_empty(),
        _ => false,
    })
}

pub fn check_is_null<'a>(values: impl IntoIterator<Item = &'a Value>) -> bool {
    values.into_iter().any(|x| x.is_null())
    // { "a": [ { "b": null }, { "b": 1 } ] } => true
    // { "a": [ { "b": 1 }, { "b": null } ] } => true
    // { "a": [ { "b": 1 }, { "b": 2 } ] } => false
}

pub fn rev_range(a: usize, b: usize) -> impl Iterator<Item = usize> {
    (b + 1..=a).rev()
}

// Merge source map into destination map
pub fn merge_map(
    dest: &mut serde_json::Map<String, Value>,
    source: &serde_json::Map<String, Value>,
) {
    for (key, value) in source {
        match value {
            Value::Null => dest.remove(key),
            _ => dest.insert(key.to_owned(), value.to_owned()),
        };
    }
}

pub fn transpose_map_into_named_vector<TVector: Into<Vector>>(
    map: HashMap<String, Vec<TVector>>,
) -> Vec<NamedVectors<'static>> {
    let mut result = Vec::new();
    for (key, values) in map {
        result.resize_with(values.len(), NamedVectors::default);
        for (i, value) in values.into_iter().enumerate() {
            result[i].insert(key.clone(), value.into());
        }
    }
    result
}

/// Deserializer helper for `Option<Vec<T>>` that allows deserializing both single and an array of values.
///
/// Use via `#[serde(with = "MaybeOneOrMany")]` and `#[schemars(with="MaybeOneOrMany<T>")]` field attributes
pub struct MaybeOneOrMany<T>(pub Option<Vec<T>>);

impl<T: Serialize> MaybeOneOrMany<T> {
    pub fn serialize<S>(value: &Option<Vec<T>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        value.serialize(serializer)
    }
}

impl<'de, T: Deserialize<'de>> MaybeOneOrMany<T> {
    pub fn deserialize<D>(deserializer: D) -> Result<Option<Vec<T>>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde_untagged::UntaggedEnumVisitor;

        UntaggedEnumVisitor::new()
            .unit(|| Ok(None))
            .seq(|x| x.deserialize().map(Some))
            .map(|x| x.deserialize().map(|x| vec![x]).map(Some))
            .deserialize(deserializer)
    }
}

impl<T: JsonSchema> JsonSchema for MaybeOneOrMany<T> {
    fn json_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        use schemars::schema::SchemaObject;

        #[derive(JsonSchema)]
        #[serde(untagged)]
        enum OneOrMany<T> {
            _One(T),
            _Many(Vec<T>),
            _None(()),
        }

        let schema: SchemaObject = <OneOrMany<T>>::json_schema(gen).into();
        schema.into()
    }

    fn schema_name() -> String {
        <Vec<T>>::schema_name()
    }

    fn is_referenceable() -> bool {
        false
    }
}

#[cfg(test)]
#[generic_tests::define]
mod jsonpath_tests {
    use std::any::TypeId;
    use std::str::FromStr;

    use super::*;
    use crate::json_path::{path, JsonPathInterface, JsonPathV2};

    fn json(str: &str) -> serde_json::Map<String, Value> {
        serde_json::from_str(str).unwrap()
    }

    #[test]
    fn test_get_nested_value_from_json_map<P: JsonPathInterface>() {
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
            path::<P>("a.b").value_get(&map).into_vec(),
            vec![&Value::Object(serde_json::Map::from_iter(vec![(
                "c".to_string(),
                Value::Number(1.into())
            )]))]
        );

        // going deeper
        assert_eq!(
            path::<P>("a.b.c").value_get(&map).into_vec(),
            vec![&Value::Number(1.into())]
        );

        // missing path
        assert!(check_is_empty(
            path::<P>("a.b.c.d").value_get(&map).iter().copied()
        ));
    }

    #[test]
    fn test_is_empty<P: JsonPathInterface>() {
        let map = serde_json::from_str::<serde_json::Map<String, Value>>(
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
        )
        .unwrap();
        let multivalue = path::<P>("a[].b").value_get(&map);
        let is_empty = check_is_empty(multivalue.iter().copied());

        assert!(!is_empty, "a[].b is not empty");

        let multivalue = path::<P>("a[].c").value_get(&map);
        let is_empty = check_is_empty(multivalue.iter().copied());

        assert!(is_empty, "a[].c is empty");

        let multivalue = path::<P>("a[].d").value_get(&map);
        let is_empty = check_is_empty(multivalue.iter().copied());
        assert!(is_empty, "a[].d is empty");

        let multivalue = path::<P>("a[].f").value_get(&map);
        let is_empty = check_is_empty(multivalue.iter().copied());
        assert!(is_empty, "a[].f is empty");
    }

    #[test]
    fn test_get_nested_array_value_from_json_map<P: JsonPathInterface + 'static>() {
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
            path::<P>("a.b").value_get(&map).into_vec(),
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
            path::<P>("a.b[]").value_get(&map).into_vec(),
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
            path::<P>("a.b[].c").value_get(&map).into_vec(),
            vec![&Value::Number(1.into()), &Value::Number(2.into())]
        );

        // project object field through array
        assert_eq!(
            path::<P>("a.b[].d").value_get(&map).into_vec(),
            vec![&Value::Object(serde_json::Map::from_iter(vec![(
                "e".to_string(),
                Value::Number(3.into())
            )]))]
        );

        // select scalar element from array
        assert_eq!(
            path::<P>("a.b[0]").value_get(&map).into_vec(),
            vec![&Value::Object(serde_json::Map::from_iter(vec![(
                "c".to_string(),
                Value::Number(1.into())
            )]))]
        );

        // select scalar object from array different index
        assert_eq!(
            path::<P>("a.b[1]").value_get(&map).into_vec(),
            vec![&Value::Object(serde_json::Map::from_iter(vec![(
                "c".to_string(),
                Value::Number(2.into())
            )]))]
        );

        // select field element from array different index
        assert_eq!(
            path::<P>("a.b[1].c").value_get(&map).into_vec(),
            vec![&Value::Number(2.into())]
        );

        // select scalar element from array different index
        assert_eq!(
            path::<P>("g[2]").value_get(&map).into_vec(),
            vec![&Value::String("g2".to_string())]
        );

        // select object element from array
        assert_eq!(
            path::<P>("a.b[2]").value_get(&map).into_vec(),
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
            path::<P>("a.b[3]").value_get(&map).iter().copied()
        ));

        // select bad index from array
        let path = <P as FromStr>::from_str("a.b[z]");
        if TypeId::of::<P>() != TypeId::of::<JsonPathV2>() {
            assert!(check_is_empty(
                path.unwrap().value_get(&map).iter().copied()
            ));
        } else {
            assert!(path.is_err());
        }
    }

    #[test]
    fn test_get_deeply_nested_array_value_from_json_map<P: JsonPathInterface>() {
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
            path::<P>("arr1[].arr2[].a").value_get(&map).into_vec(),
            vec![
                &Value::Number(1.into()),
                &Value::Number(3.into()),
                &Value::Number(5.into()),
            ]
        );
    }

    #[test]
    fn test_no_flatten_array_value_from_json_map<P: JsonPathInterface>() {
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
            path::<P>("arr[].a").value_get(&map).into_vec(),
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
            path::<P>("arr[].a[]").value_get(&map).into_vec(),
            vec![
                &Value::Number(1.into()),
                &Value::Number(2.into()),
                &Value::Number(3.into()),
            ]
        );
    }

    #[test]
    fn test_get_null_and_absent_values<P: JsonPathInterface>() {
        let map = serde_json::from_str::<serde_json::Map<String, Value>>(
            r#"
            {
                "a": null,
                "b": [null, null],
                "c": []
            }
            "#,
        )
        .unwrap();

        assert_eq!(path::<P>("a").value_get(&map).as_slice(), &[&Value::Null],);

        assert!(path::<P>("a[]").value_get(&map).is_empty());

        assert_eq!(
            path::<P>("b").value_get(&map).as_slice(),
            &[&Value::Array(vec![Value::Null, Value::Null])],
        );

        assert_eq!(
            path::<P>("b[]").value_get(&map).as_slice(),
            &[&Value::Null, &Value::Null],
        );

        assert_eq!(
            path::<P>("c").value_get(&map).as_slice(),
            &[&Value::Array(vec![])],
        );

        assert!(path::<P>("c[]").value_get(&map).is_empty());

        assert!(path::<P>("d").value_get(&map).is_empty());

        assert!(path::<P>("d[]").value_get(&map).is_empty());
    }

    #[test]
    fn test_filter_json<P: JsonPathInterface>() {
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

        let res = P::value_filter(&map, |path, _value| {
            let path = path.to_string();
            path.starts_with("a.b[].c") || "a.b[].c".starts_with(&path)
        });

        assert_eq!(
            res,
            serde_json::from_str::<serde_json::Map<String, Value>>(
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
            )
            .unwrap()
        );
    }

    #[test]
    fn test_check_include_pattern<P: JsonPathInterface>() {
        assert!(path::<P>("a.b.c").check_include_pattern(&path::<P>("a.b.c")));
        assert!(path::<P>("a.b.c").check_include_pattern(&path::<P>("a.b")));
        assert!(!path::<P>("a.b.c").check_include_pattern(&path::<P>("a.b.d")));
        assert!(path::<P>("a.b.c").check_include_pattern(&path::<P>("a")));
        assert!(path::<P>("a").check_include_pattern(&path::<P>("a.d")));
    }

    #[test]
    fn test_check_exclude_pattern<P: JsonPathInterface>() {
        assert!(path::<P>("a.b.c").check_exclude_pattern(&path::<P>("a.b.c")));
        assert!(!path::<P>("a.b.c").check_exclude_pattern(&path::<P>("a.b")));
        assert!(!path::<P>("a.b.c").check_exclude_pattern(&path::<P>("a.b.d")));
        assert!(!path::<P>("a.b.c").check_exclude_pattern(&path::<P>("a")));
        assert!(path::<P>("a").check_exclude_pattern(&path::<P>("a.d")));
    }

    #[test]
    fn test_set_value_to_json_with_empty_key<P: JsonPathInterface>() {
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

        P::value_set(None, &mut map, &src);

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
    fn test_set_value_to_json_with_one_level_key<P: JsonPathInterface>() {
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

        JsonPathInterface::value_set(Some(&path::<P>("a")), &mut map, &src);

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
    fn test_set_value_to_json_with_array_index<P: JsonPathInterface>() {
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

        JsonPathInterface::value_set(Some(&path::<P>("a.b[1]")), &mut map, &src);

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
    fn test_set_value_to_json_with_empty_src<P: JsonPathInterface>() {
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

        JsonPathInterface::value_set(Some(&path::<P>("a.b[1]")), &mut map, &src);

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
    fn test_set_value_to_json_with_empty_dest<P: JsonPathInterface>() {
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

        P::value_set(None, &mut map, &src);

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
    fn test_set_value_to_json_with_empty_dest_nested_key<P: JsonPathInterface>() {
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

        JsonPathInterface::value_set(Some(&path::<P>("key1.key2")), &mut map, &src);

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
    fn test_set_value_to_json_with_empty_dest_nested_array_index_key<P: JsonPathInterface>() {
        let mut map = json("{}");
        let src = json(r#" {"c": 1} "#);
        JsonPathInterface::value_set(Some(&path::<P>("key1.key2[3]")), &mut map, &src);
        assert_eq!(map, json(r#" {"key1": {"key2": []}} "#));

        let mut map = json("{}");
        let src = json(r#" {"c": 1} "#);
        JsonPathInterface::value_set(Some(&path::<P>("key1.key2[0]")), &mut map, &src);
        assert_eq!(map, json(r#" {"key1": {"key2": []}} "#));
    }

    #[test]
    fn test_expand_payload_with_non_existing_array<P: JsonPathInterface>() {
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

        JsonPathInterface::value_set(Some(&path::<P>("key1.key2[].key3")), &mut map, &src);

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
    fn test_replace_scalar_key_with_object<P: JsonPathInterface>() {
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

        JsonPathInterface::value_set(Some(&path::<P>("a.b.c")), &mut map, &src);

        assert_eq!(
            map,
            serde_json::from_str::<serde_json::Map<String, Value>>(
                r#"
                {"a": {"b": {"c": {"x": 1}}}}
                "#,
            )
            .unwrap()
        );
    }

    #[test]
    fn test_get_path_head<P: JsonPathInterface + 'static>() {
        assert_eq!(path::<P>("a.b.c").head(), "a");
        assert_eq!(path::<P>("a[0].b").head(), "a");
        assert_eq!(path::<P>("a").head(), "a");
        if TypeId::of::<P>() != TypeId::of::<JsonPathV2>() {
            // JsonPathV2 does not support empty path
            assert_eq!(path::<P>("").head(), "");
        }
    }

    #[instantiate_tests(<JsonPathV2>)]
    mod v2 {}
}

#[cfg(test)]
mod tests {
    use schemars::{schema_for, JsonSchema};
    use serde::{Deserialize, Serialize};

    use crate::common::utils::MaybeOneOrMany;

    #[test]
    fn test_deserialize_one_or_many() {
        #[derive(Serialize, Deserialize)]
        struct Test {
            #[serde(with = "MaybeOneOrMany")]
            data: Option<Vec<Inner>>,
        }

        #[derive(Serialize, Deserialize)]
        struct Inner {
            key: String,
        }

        let res = serde_json::from_str::<Test>(
            r#"
            {
                "data": null
            }
            "#,
        )
        .unwrap();

        assert!(res.data.is_none());

        let res = serde_json::from_str::<Test>(
            r#"
            {
                "data": {
                    "key": "value"
                }
            }
            "#,
        )
        .unwrap();

        assert_eq!(res.data.as_ref().unwrap().len(), 1);
        assert_eq!(res.data.as_ref().unwrap()[0].key, "value".to_string());

        let res = serde_json::from_str::<Test>(
            r#"
            {
                "data": [
                    {
                        "key": "value"
                    }
                ]
            }
            "#,
        )
        .unwrap();

        assert_eq!(res.data.as_ref().unwrap().len(), 1);
        assert_eq!(res.data.as_ref().unwrap()[0].key, "value".to_string());
    }

    #[test]
    fn test_schema_one_or_many() {
        #[derive(JsonSchema)]
        struct Test {
            #[schemars(with = "MaybeOneOrMany<String>")]
            _field: Option<Vec<String>>,
        }

        let mut field_schema = dbg!(schemars::schema_for!(Test)
            .schema
            .object
            .unwrap()
            .properties
            .remove("_field")
            .unwrap()
            .into_object());

        assert!(field_schema.subschemas.is_some());

        let any_of = field_schema.subschemas().any_of.clone().unwrap();

        assert_eq!(any_of.len(), 3);
        assert_eq!(
            any_of[0].clone().into_object().instance_type,
            schema_for!(String).schema.instance_type
        );
        assert_eq!(
            any_of[1].clone().into_object().array,
            schema_for!(Vec<String>).schema.array
        );
        assert_eq!(
            any_of[2].clone().into_object().instance_type,
            schema_for!(()).schema.instance_type
        );
    }
}
