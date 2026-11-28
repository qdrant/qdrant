#![allow(deprecated)]
//! Regression test for <https://github.com/qdrant/qdrant/issues/11723>: a huge
//! `limit` plus an `offset` must not wrap around to a tiny fetch count.

use std::collections::HashMap;

use edge::{Distance, EdgeConfig, EdgeShard, EdgeVectorParams, SearchRequest};
use segment::data_types::vectors::{NamedQuery, VectorInternal, VectorStructInternal};
use segment::types::ExtendedPointId;
use shard::operations::CollectionUpdateOperations::PointOperation;
use shard::operations::point_ops::PointInsertOperationsInternal::PointsList;
use shard::operations::point_ops::PointOperations::UpsertPoints;
use shard::operations::point_ops::{PointStructPersisted, VectorStructPersisted};
use shard::query::query_enum::QueryEnum;

const VECTOR_NAME: &str = "edge-huge-limit-test-vector";

fn config() -> EdgeConfig {
    EdgeConfig::builder()
        .on_disk_payload(false)
        .vector(
            VECTOR_NAME,
            EdgeVectorParams {
                size: 1,
                distance: Distance::Dot,
                quantization_config: None,
                multivector_config: None,
                datatype: None,
                on_disk: None,
                memory: None,
                hnsw_config: None,
            },
        )
        .build()
}

fn point(id: u64) -> PointStructPersisted {
    PointStructPersisted {
        id: ExtendedPointId::NumId(id),
        vector: VectorStructPersisted::from(VectorStructInternal::Named(HashMap::from([(
            VECTOR_NAME.to_string(),
            VectorInternal::from(vec![id as f32]),
        )]))),
        payload: None,
    }
}

#[test]
fn search_with_huge_limit_and_offset_returns_remaining_points() {
    let dir = tempfile::Builder::new()
        .prefix("edge-huge-limit-search")
        .tempdir()
        .unwrap();
    let shard = EdgeShard::new(dir.path(), config()).unwrap();
    shard
        .update(PointOperation(UpsertPoints(PointsList(
            (0..100).map(point).collect(),
        ))))
        .unwrap();

    let mut request = SearchRequest::new(
        QueryEnum::Nearest(NamedQuery::new(vec![1.0].into(), VECTOR_NAME)),
        usize::MAX,
    );
    request.offset = 2;

    let result = shard.search(request).unwrap();

    // Everything after the offset: an oversized limit means "all remaining points".
    assert_eq!(result.len(), 98);
    // Dot product with [1.0] ranks by id, highest first; the offset skips 99 and 98.
    assert_eq!(result[0].id, ExtendedPointId::NumId(97));
}
