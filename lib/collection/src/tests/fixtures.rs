use segment::data_types::vectors::VectorStruct;
use segment::types::{Distance, PayloadFieldSchema, PayloadSchemaType};

use crate::config::{CollectionConfig, CollectionParams, WalConfig};
use crate::operations::point_ops::{PointOperations, PointStruct};
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

pub fn create_collection_config() -> CollectionConfig {
    let wal_config = WalConfig {
        wal_capacity_mb: 1,
        wal_segments_ahead: 0,
    };

    let collection_params = CollectionParams {
        vectors: VectorsConfig::Single(VectorParamsBuilder::new(4, Distance::Dot).build()),
        ..CollectionParams::empty()
    };

    let mut optimizer_config = TEST_OPTIMIZERS_CONFIG.clone();

    optimizer_config.default_segment_number = 1;
    optimizer_config.flush_interval_sec = 0;

    CollectionConfig {
        params: collection_params,
        optimizer_config,
        wal_config,
        hnsw_config: Default::default(),
        quantization_config: Default::default(),
    }
}

pub fn upsert_operation() -> CollectionUpdateOperations {
    CollectionUpdateOperations::PointOperation(
        vec![
            PointStruct {
                id: 1.into(),
                vector: VectorStruct::from(vec![1.0, 2.0, 3.0, 4.0]).into(),
                payload: Some(
                    serde_json::from_str(r#"{ "location": { "lat": 10.12, "lon": 32.12  } }"#).unwrap(),
                ),
            },
            PointStruct {
                id: 2.into(),
                vector: VectorStruct::from(vec![2.0, 1.0, 3.0, 4.0]).into(),
                payload: Some(
                    serde_json::from_str(r#"{ "location": { "lat": 11.12, "lon": 34.82  } }"#).unwrap(),
                ),
            },
            PointStruct {
                id: 3.into(),
                vector: VectorStruct::from(vec![3.0, 2.0, 1.0, 4.0]).into(),
                payload: Some(
                    serde_json::from_str(r#"{ "location": [ { "lat": 12.12, "lon": 34.82  }, { "lat": 12.2, "lon": 12.82  }] }"#).unwrap(),
                ),
            },
            PointStruct {
                id: 4.into(),
                vector: VectorStruct::from(vec![4.0, 2.0, 3.0, 1.0]).into(),
                payload: Some(
                    serde_json::from_str(r#"{ "location": { "lat": 13.12, "lon": 34.82  } }"#).unwrap(),
                ),
            },
            PointStruct {
                id: 5.into(),
                vector: VectorStruct::from(vec![5.0, 2.0, 3.0, 4.0]).into(),
                payload: Some(
                    serde_json::from_str(r#"{ "location": { "lat": 14.12, "lon": 32.12  } }"#).unwrap(),
                ),
            },

        ]
            .into(),
    )
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
