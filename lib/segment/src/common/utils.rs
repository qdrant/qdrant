use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use smallvec::SmallVec;

use crate::data_types::named_vectors::NamedVectors;
use crate::data_types::vectors::VectorInternal;
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

pub fn transpose_map_into_named_vector<TVector: Into<VectorInternal>>(
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
