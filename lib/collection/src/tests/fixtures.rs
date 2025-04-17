use ahash::AHashSet;
use segment::data_types::vectors::VectorStructInternal;
use segment::types::{
    Condition, Distance, Filter, PayloadFieldSchema, PayloadSchemaType, PointIdType,
};

use crate::config::{CollectionConfigInternal, CollectionParams, WalConfig};
use crate::operations::point_ops::{
    PointInsertOperationsInternal, PointOperations, PointStructPersisted,
};
use crate::operations::types::VectorsConfig;
use crate::operations::vector_params_builder::VectorParamsBuilder;
use crate::operations::{CollectionUpdateOperations, CreateIndex, FieldIndexOperations};
use crate::optimizers_builder::OptimizersConfig;

pub const TEST_OPTIMIZERS_CONFIG: OptimizersConfig = OptimizersConfig {
    deleted_threshold: 0.9,
    vacuum_min_vector_number: 1000,
    default_segment_number: 2,
    max_segment_size: None,
    memmap_threshold: None,
    indexing_threshold: Some(50_000),
    flush_interval_sec: 30,
    max_optimization_threads: Some(2),
};

pub fn create_collection_config_with_dim(dim: usize) -> CollectionConfigInternal {
    let wal_config = WalConfig {
        wal_capacity_mb: 1,
        wal_segments_ahead: 0,
    };

    let collection_params = CollectionParams {
        vectors: VectorsConfig::Single(VectorParamsBuilder::new(dim as u64, Distance::Dot).build()),
        ..CollectionParams::empty()
    };

    let mut optimizer_config = TEST_OPTIMIZERS_CONFIG.clone();

    optimizer_config.default_segment_number = 1;
    optimizer_config.flush_interval_sec = 0;

    CollectionConfigInternal {
        params: collection_params,
        optimizer_config,
        wal_config,
        hnsw_config: Default::default(),
        quantization_config: Default::default(),
        strict_mode_config: Default::default(),
        uuid: None,
    }
}

pub fn create_collection_config() -> CollectionConfigInternal {
    create_collection_config_with_dim(4)
}

pub fn upsert_operation() -> CollectionUpdateOperations {
    let points = vec![
        PointStructPersisted {
            id: 1.into(),
            vector: VectorStructInternal::from(vec![1.0, 2.0, 3.0, 4.0]).into(),
            payload: Some(
                serde_json::from_str(r#"{ "location": { "lat": 10.12, "lon": 32.12  } }"#).unwrap(),
            ),
        },
        PointStructPersisted {
            id: 2.into(),
            vector: VectorStructInternal::from(vec![2.0, 1.0, 3.0, 4.0]).into(),
            payload: Some(
                serde_json::from_str(r#"{ "location": { "lat": 11.12, "lon": 34.82  } }"#).unwrap(),
            ),
        },
        PointStructPersisted {
            id: 3.into(),
            vector: VectorStructInternal::from(vec![3.0, 2.0, 1.0, 4.0]).into(),
            payload: Some(
                serde_json::from_str(r#"{ "location": [ { "lat": 12.12, "lon": 34.82  }, { "lat": 12.2, "lon": 12.82  }] }"#).unwrap(),
            ),
        },
        PointStructPersisted {
            id: 4.into(),
            vector: VectorStructInternal::from(vec![4.0, 2.0, 3.0, 1.0]).into(),
            payload: Some(
                serde_json::from_str(r#"{ "location": { "lat": 13.12, "lon": 34.82  } }"#).unwrap(),
            ),
        },
        PointStructPersisted {
            id: 5.into(),
            vector: VectorStructInternal::from(vec![5.0, 2.0, 3.0, 4.0]).into(),
            payload: Some(
                serde_json::from_str(r#"{ "location": { "lat": 14.12, "lon": 32.12  } }"#).unwrap(),
            ),
        },
    ];

    let op = PointInsertOperationsInternal::from(points);

    CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(op))
}

pub fn create_payload_index_operation() -> CollectionUpdateOperations {
    CollectionUpdateOperations::FieldIndexOperation(FieldIndexOperations::CreateIndex(
        CreateIndex {
            field_name: "location".parse().unwrap(),
            field_schema: Some(PayloadFieldSchema::FieldType(PayloadSchemaType::Geo)),
        },
    ))
}

pub fn delete_point_operation(idx: u64) -> CollectionUpdateOperations {
    CollectionUpdateOperations::PointOperation(PointOperations::DeletePoints {
        ids: vec![idx.into()],
    })
}

pub fn filter_single_id(id: impl Into<PointIdType>) -> Filter {
    Filter::new_must(Condition::HasId(AHashSet::from([id.into()]).into()))
}
