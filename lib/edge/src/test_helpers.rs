use std::collections::HashMap;

use segment::data_types::vectors::{VectorInternal, VectorStructInternal};
use segment::types::{Distance, ExtendedPointId, Payload};
use shard::operations::CollectionUpdateOperations::PointOperation;
use shard::operations::point_ops::PointInsertOperationsInternal::PointsList;
use shard::operations::point_ops::PointOperations::UpsertPoints;
use shard::operations::point_ops::{PointStructPersisted, VectorStructPersisted};

use crate::config::vectors::EdgeVectorParams;
use crate::{EdgeConfig, EdgeShard};

pub(crate) const VECTOR_NAME: &str = "edge-test-vector";

pub(crate) fn test_config() -> EdgeConfig {
    EdgeConfig {
        on_disk_payload: Some(false),
        vectors: HashMap::from([(
            VECTOR_NAME.to_string(),
            EdgeVectorParams {
                size: 1,
                distance: Distance::Dot,
                quantization_config: None,
                multivector_config: None,
                datatype: None,
                on_disk: None,
                hnsw_config: None,
            },
        )]),
        sparse_vectors: HashMap::new(),
        hnsw_config: Default::default(),
        quantization_config: None,
        optimizers: Default::default(),
        wal_options: None,
        max_search_threads: None,
    }
}

fn named_vector(id: u64) -> VectorStructPersisted {
    VectorStructPersisted::from(VectorStructInternal::Named(HashMap::from([(
        VECTOR_NAME.to_string(),
        VectorInternal::from(vec![id as f32]),
    )])))
}

pub(crate) fn point(id: u64) -> PointStructPersisted {
    PointStructPersisted {
        id: ExtendedPointId::NumId(id),
        vector: named_vector(id),
        payload: None,
    }
}

pub(crate) fn point_with_group(id: u64, group: &str) -> PointStructPersisted {
    point_with_group_values(id, serde_json::json!(group))
}

pub(crate) fn point_with_group_values(id: u64, group: serde_json::Value) -> PointStructPersisted {
    let payload = serde_json::json!({ "group": group });
    PointStructPersisted {
        id: ExtendedPointId::NumId(id),
        vector: named_vector(id),
        payload: Some(Payload::from(payload.as_object().unwrap().clone())),
    }
}

pub(crate) fn upsert(shard: &EdgeShard, points: Vec<PointStructPersisted>) {
    shard
        .update(PointOperation(UpsertPoints(PointsList(points))))
        .unwrap();
}
