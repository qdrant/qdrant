//! Random point generation for `upsert`, shaped to match a collection's live
//! schema: every dense/sparse vector the shard's config advertises, and one
//! payload value per indexed payload field.

use std::collections::HashMap;

use edge::{
    JsonPath, Payload, PayloadIndexInfo, PayloadSchemaType, PointId, PointStructPersisted,
    SparseVector, VectorPersisted, VectorStructPersisted,
};
use rand::RngExt as _;
use rand::rngs::StdRng;
use segment::json_path::JsonPathItem;

/// The shard's write-facing schema: every named vector a point must carry,
/// and every payload field worth generating a value for.
pub struct Schema {
    pub dense: Vec<(String, usize)>,
    pub sparse: Vec<String>,
    pub payload: Vec<(JsonPath, PayloadSchemaType)>,
}

impl Schema {
    pub fn new(
        dense: HashMap<String, usize>,
        sparse: impl IntoIterator<Item = String>,
        payload_schema: &HashMap<JsonPath, PayloadIndexInfo>,
    ) -> Self {
        let mut dense: Vec<(String, usize)> = dense.into_iter().collect();
        dense.sort_by(|a, b| a.0.cmp(&b.0));

        let mut sparse: Vec<String> = sparse.into_iter().collect();
        sparse.sort();

        let mut payload: Vec<(JsonPath, PayloadSchemaType)> = payload_schema
            .iter()
            .map(|(key, info)| (key.clone(), info.data_type))
            .collect();
        payload.sort_by(|a, b| a.0.cmp(&b.0));

        Self {
            dense,
            sparse,
            payload,
        }
    }
}

const WORDS: &[&str] = &[
    "amber", "basalt", "cobalt", "dune", "ember", "fjord", "garnet", "harbor",
];

/// One random point in the shape `schema` prescribes.
pub fn random_point(id: PointId, schema: &Schema, rng: &mut StdRng) -> PointStructPersisted {
    let mut vectors = HashMap::new();

    for (name, size) in &schema.dense {
        let dense: Vec<f32> = (0..*size).map(|_| rng.random_range(-1.0..1.0)).collect();
        vectors.insert(name.clone(), VectorPersisted::Dense(dense));
    }

    for name in &schema.sparse {
        // Cumulative random gaps: sorted, unique indices without a sampler.
        let mut index = 0u32;
        let mut indices = Vec::new();
        let mut values = Vec::new();
        for _ in 0..8 {
            index += rng.random_range(1..1000);
            indices.push(index);
            values.push(rng.random_range(0.0..1.0));
        }
        vectors.insert(
            name.clone(),
            VectorPersisted::Sparse(SparseVector { indices, values }),
        );
    }

    let mut payload = serde_json::Map::new();
    for (field, schema_type) in &schema.payload {
        set_at_path(&mut payload, field, random_payload_value(*schema_type, rng));
    }

    PointStructPersisted {
        id,
        vector: VectorStructPersisted::Named(vectors),
        payload: Some(Payload::from(payload)),
    }
}

/// Wrap `value` in the objects and arrays `path` walks through, so an index on
/// a nested or array field actually sees it — a flat `"a.b"` key leaves that
/// index empty. Paths sharing a prefix merge rather than overwrite.
fn set_at_path(
    payload: &mut serde_json::Map<String, serde_json::Value>,
    path: &JsonPath,
    value: serde_json::Value,
) {
    let nested = path
        .rest
        .iter()
        .rev()
        .fold(value, |value, item| match item {
            JsonPathItem::Key(key) => serde_json::json!({ key.clone(): value }),
            JsonPathItem::WildcardIndex => serde_json::json!([value]),
            JsonPathItem::Index(index) => {
                let mut array = vec![serde_json::Value::Null; *index];
                array.push(value);
                array.into()
            }
        });

    match payload.get_mut(&path.first_key) {
        Some(existing) => merge_value(existing, nested),
        None => {
            payload.insert(path.first_key.clone(), nested);
        }
    }
}

/// Deep-merge objects and arrays so sibling paths under one prefix coexist
/// (`tags[].name` and `tags[].score` land on the same element); anything else
/// is replaced.
fn merge_value(dest: &mut serde_json::Value, source: serde_json::Value) {
    match (dest, source) {
        (serde_json::Value::Object(dest), serde_json::Value::Object(source)) => {
            for (key, value) in source {
                match dest.get_mut(&key) {
                    Some(existing) => merge_value(existing, value),
                    None => {
                        dest.insert(key, value);
                    }
                }
            }
        }
        (serde_json::Value::Array(dest), serde_json::Value::Array(source)) => {
            for (index, value) in source.into_iter().enumerate() {
                // Null is only ever the padding `set_at_path` puts ahead of an
                // explicit index — no generated value is null — so it must not
                // overwrite an element a sibling path already placed.
                if value.is_null() {
                    if dest.len() <= index {
                        dest.resize(index + 1, serde_json::Value::Null);
                    }
                    continue;
                }
                match dest.get_mut(index) {
                    Some(existing) => merge_value(existing, value),
                    None => {
                        dest.resize(index, serde_json::Value::Null);
                        dest.push(value);
                    }
                }
            }
        }
        (dest, source) => *dest = source,
    }
}

fn random_payload_value(schema_type: PayloadSchemaType, rng: &mut StdRng) -> serde_json::Value {
    let word = |rng: &mut StdRng| WORDS[rng.random_range(0..WORDS.len())].to_string();
    match schema_type {
        PayloadSchemaType::Keyword => word(rng).into(),
        PayloadSchemaType::Integer => rng.random_range(0..1000).into(),
        PayloadSchemaType::Float => rng.random_range(0.0..100.0).into(),
        PayloadSchemaType::Bool => rng.random::<bool>().into(),
        PayloadSchemaType::Geo => serde_json::json!({
            "lon": rng.random_range(-180.0..180.0),
            "lat": rng.random_range(-85.0..85.0),
        }),
        PayloadSchemaType::Text => format!("{} {} {}", word(rng), word(rng), word(rng)).into(),
        PayloadSchemaType::Datetime => format!(
            "2026-{:02}-{:02}T{:02}:{:02}:{:02}Z",
            rng.random_range(1..=12),
            rng.random_range(1..=28),
            rng.random_range(0..24),
            rng.random_range(0..60),
            rng.random_range(0..60),
        )
        .into(),
        PayloadSchemaType::Uuid => edge::external::uuid::Uuid::from_u128(rng.random())
            .to_string()
            .into(),
    }
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng as _;

    use super::*;

    fn schema(paths: &[(&str, PayloadSchemaType)]) -> Schema {
        Schema {
            dense: Vec::new(),
            sparse: Vec::new(),
            payload: paths
                .iter()
                .map(|(path, kind)| (path.parse().unwrap(), *kind))
                .collect(),
        }
    }

    fn payload_of(schema: &Schema) -> serde_json::Map<String, serde_json::Value> {
        let mut rng = StdRng::seed_from_u64(1);
        let point = random_point(PointId::NumId(0), schema, &mut rng);
        point.payload.unwrap().0
    }

    #[test]
    fn nested_paths_are_reachable_by_the_index_they_were_generated_for() {
        let schema = schema(&[
            ("meta.city", PayloadSchemaType::Keyword),
            ("meta.age", PayloadSchemaType::Integer),
            ("flat", PayloadSchemaType::Float),
        ]);
        let payload = payload_of(&schema);

        for (path, _) in &schema.payload {
            assert!(
                !path.value_get(&payload).is_empty(),
                "index on {path} sees no value in {payload:?}",
            );
        }
        assert!(
            payload["meta"].is_object(),
            "a nested path must not become a flat key: {payload:?}",
        );
    }

    #[test]
    fn sibling_paths_at_different_explicit_indexes_both_survive() {
        let schema = schema(&[
            ("tags[0].name", PayloadSchemaType::Keyword),
            ("tags[1].score", PayloadSchemaType::Float),
        ]);
        let payload = payload_of(&schema);
        for (path, _) in &schema.payload {
            assert!(
                !path.value_get(&payload).is_empty(),
                "index on {path} sees no value in {payload:?}",
            );
        }
    }

    #[test]
    fn array_paths_are_reachable_too() {
        let schema = schema(&[
            ("tags[].name", PayloadSchemaType::Keyword),
            ("tags[].score", PayloadSchemaType::Float),
        ]);
        let payload = payload_of(&schema);

        for (path, _) in &schema.payload {
            assert!(
                !path.value_get(&payload).is_empty(),
                "index on {path} sees no value in {payload:?}",
            );
        }
    }
}
