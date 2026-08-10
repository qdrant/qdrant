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

/// The shard's write-facing schema: every named vector a point must carry,
/// and every payload field worth generating a value for.
pub struct Schema {
    pub dense: Vec<(String, usize)>,
    pub sparse: Vec<String>,
    pub payload: Vec<(String, PayloadSchemaType)>,
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

        let mut payload: Vec<(String, PayloadSchemaType)> = payload_schema
            .iter()
            .map(|(key, info)| (key.to_string(), info.data_type))
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
        payload.insert(field.clone(), random_payload_value(*schema_type, rng));
    }

    PointStructPersisted {
        id,
        vector: VectorStructPersisted::Named(vectors),
        payload: Some(Payload::from(payload)),
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
