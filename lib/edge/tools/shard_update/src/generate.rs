//! Random point generation in the shape the shard schema prescribes.

use std::collections::HashMap;

use edge::external::uuid::Uuid;
use edge::{
    Payload, PayloadSchemaType, PointId, PointOperations, PointStructPersisted, SparseVector,
    UpdateOperation, VectorPersisted, VectorStructPersisted,
};
use rand::rngs::StdRng;
use rand::{RngExt as _, SeedableRng as _};

use crate::schema::ShardSchema;

const WORDS: &[&str] = &[
    "amber", "basalt", "cobalt", "dune", "ember", "fjord", "garnet", "harbor",
];

/// One random point in the shape the schema prescribes: every named vector at
/// its configured dimensionality, one payload value per indexed field.
fn random_point(id: PointId, schema: &ShardSchema, rng: &mut StdRng) -> PointStructPersisted {
    let mut vectors = HashMap::new();

    for (name, vector_config) in &schema.config.vector_data {
        let dense = |rng: &mut StdRng| {
            (0..vector_config.size)
                .map(|_| rng.random_range(-1.0..1.0))
                .collect::<Vec<f32>>()
        };
        let vector = if vector_config.multivector_config.is_some() {
            VectorPersisted::MultiDense(vec![dense(rng), dense(rng)])
        } else {
            VectorPersisted::Dense(dense(rng))
        };
        vectors.insert(name.clone(), vector);
    }

    for name in schema.config.sparse_vector_data.keys() {
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
    for (field, schema_type) in &schema.payload_fields {
        payload.insert(field.clone(), random_payload_value(*schema_type, rng));
    }

    PointStructPersisted {
        id,
        vector: VectorStructPersisted::Named(vectors),
        payload: Some(
            serde_json::from_value::<Payload>(serde_json::Value::Object(payload))
                .expect("a JSON object is always a valid payload"),
        ),
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
        PayloadSchemaType::Uuid => Uuid::from_u128(rng.random()).to_string().into(),
    }
}

/// Generate the random upsert batch off the shard's schema, logging each
/// point's shape.
pub fn generate_batch(schema: &ShardSchema, ids: &[PointId], seed: u64) -> UpdateOperation {
    let mut rng = StdRng::seed_from_u64(seed);
    let points: Vec<PointStructPersisted> = ids
        .iter()
        .map(|&id| {
            let point = random_point(id, schema, &mut rng);
            log::info!(
                "generated point {id}: vectors {:?}, payload {}",
                match &point.vector {
                    VectorStructPersisted::Single(v) => vec![format!("(default, {} dim)", v.len())],
                    VectorStructPersisted::MultiDense(m) =>
                        vec![format!("(default, {} multivector(s))", m.len())],
                    VectorStructPersisted::Named(named) => named
                        .iter()
                        .map(|(name, vector)| match vector {
                            VectorPersisted::Dense(v) => format!("{name}({} dim)", v.len()),
                            VectorPersisted::Sparse(s) =>
                                format!("{name}({} nnz)", s.indices.len()),
                            VectorPersisted::MultiDense(m) =>
                                format!("{name}({} multivector(s))", m.len()),
                        })
                        .collect(),
                },
                point
                    .payload
                    .as_ref()
                    .map(|payload| serde_json::to_string(payload)
                        .unwrap_or_else(|err| err.to_string()))
                    .unwrap_or_else(|| "<none>".to_string()),
            );
            point
        })
        .collect();

    UpdateOperation::PointOperation(PointOperations::UpsertPoints(points.into()))
}
