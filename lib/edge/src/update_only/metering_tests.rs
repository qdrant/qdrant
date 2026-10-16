use common::universal_io::MmapFs;
use segment::data_types::fully_qualified_point::FullyQualifiedPoint;
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::vectors::{
    MultiDenseVectorInternal, VectorInternal, VectorStructInternal,
};
use segment::payload_json;
use segment::types::{HnswConfig, MultiVectorComparator, MultiVectorConfig, VectorStorageDatatype};
use shard::operations::CollectionUpdateOperations::{PayloadOperation, PointOperation};
use shard::operations::payload_ops::{PayloadOps, SetPayloadOp};
use shard::operations::point_ops::PointInsertOperationsInternal::PointsList;
use shard::operations::point_ops::PointOperations::{
    DeletePoints, UpsertPoints, UpsertPointsConditional,
};
use shard::operations::point_ops::{
    ConditionalInsertOperationInternal, UpdateMode, VectorStructPersisted,
};
use sparse::common::sparse_vector::SparseVector;

use crate::config::vectors::EdgeSparseVectorParams;
use crate::read_only::tests::{VECTOR_NAME, point, test_config};
use crate::{
    EdgeConfig, EdgeShard, JsonPath, PointId, UpdateBytes, UpdateMetering, UpdateOnlyEdgeShard,
};

fn writer(config: EdgeConfig) -> (tempfile::TempDir, UpdateOnlyEdgeShard<MmapFs>) {
    let dir = tempfile::tempdir().unwrap();
    let leader = EdgeShard::new(dir.path(), config).unwrap();
    leader.flush().unwrap();
    drop(leader);
    let writer = UpdateOnlyEdgeShard::open_mmap(dir.path()).unwrap();
    (dir, writer)
}

fn resolved(vector: VectorInternal) -> FullyQualifiedPoint {
    let mut updated_vectors = NamedVectors::default();
    updated_vectors.insert(VECTOR_NAME.into(), vector);
    FullyQualifiedPoint {
        id: PointId::NumId(1),
        version: 1,
        stored_vectors: Default::default(),
        updated_vectors,
        payload: Default::default(),
    }
}

#[test]
fn metered_apply_counts_stored_data_once_and_excludes_replay_and_deletes() {
    let config = test_config();
    let metering = UpdateMetering::new(&config, []);
    let (_dir, writer) = writer(config);

    let operations = [(1, PointOperation(UpsertPoints(PointsList(vec![point(1)]))))];
    let (writer, outcome) = writer
        .apply_batch_metered(operations.clone(), &metering)
        .unwrap();
    assert_eq!(
        outcome.usage,
        UpdateBytes {
            updated_bytes: 6,
            indexed_bytes: 4
        }
    );

    let (writer, replayed) = writer.apply_batch_metered(operations, &metering).unwrap();
    assert_eq!(replayed.skipped, 1);
    assert_eq!(replayed.usage, UpdateBytes::default());

    let (_, deleted) = writer
        .apply_batch_metered(
            [(
                2,
                PointOperation(DeletePoints {
                    ids: vec![PointId::NumId(1)],
                }),
            )],
            &metering,
        )
        .unwrap();
    assert_eq!(deleted.deleted, 1);
    assert_eq!(deleted.usage, UpdateBytes::default());
}

#[test]
fn payload_update_meters_full_merged_payload_and_retained_vectors() {
    let config = test_config();
    let metering = UpdateMetering::new(
        &config,
        ["name", "nested[].tag", "absent", "name"].map(|path| path.parse::<JsonPath>().unwrap()),
    );
    let (_dir, writer) = writer(config);
    let mut initial = point(1);
    initial.payload = Some(payload_json! { "keep": true, "name": "old" });
    let (writer, _) = writer
        .apply_batch_metered(
            [(1, PointOperation(UpsertPoints(PointsList(vec![initial]))))],
            &metering,
        )
        .unwrap();
    let change = PayloadOperation(PayloadOps::SetPayload(SetPayloadOp {
        payload: payload_json! { "name": "ár", "nested": [{ "tag": "a" }, { "tag": "bc" }] },
        points: Some(vec![PointId::NumId(1)]),
        filter: None,
        key: None,
    }));
    let (_, outcome) = writer
        .apply_batch_metered([(2, change)], &metering)
        .unwrap();
    assert_eq!(outcome.stored, 1);
    assert_eq!(
        outcome.usage,
        UpdateBytes {
            updated_bytes: r#"{"keep":true,"name":"ár","nested":[{"tag":"a"},{"tag":"bc"}]}"#.len()
                as u64
                + 4,
            indexed_bytes: 4 + 5 + 3 + 4,
        },
        "the unchanged vector and payload fields are rewritten; duplicate paths count once"
    );
}

#[test]
fn folds_repeated_updates_and_does_not_meter_rejected_or_missing_points() {
    let config = test_config();
    let metering = UpdateMetering::new(&config, []);
    let (_dir, writer) = writer(config);
    let mut last = point(1);
    last.payload = Some(payload_json! { "v": 2 });
    let (writer, folded) = writer
        .apply_batch_metered(
            [
                (1, PointOperation(UpsertPoints(PointsList(vec![point(1)])))),
                (2, PointOperation(UpsertPoints(PointsList(vec![last])))),
            ],
            &metering,
        )
        .unwrap();
    assert_eq!(folded.stored, 1);
    assert_eq!(
        folded.usage,
        UpdateBytes {
            updated_bytes: 11,
            indexed_bytes: 4
        }
    );

    let conditional = |id, mode| {
        PointOperation(UpsertPointsConditional(
            ConditionalInsertOperationInternal {
                points_op: PointsList(vec![point(id)]),
                condition: Default::default(),
                update_mode: Some(mode),
            },
        ))
    };
    let (_, outcome) = writer
        .apply_batch_metered(
            [
                (3, conditional(1, UpdateMode::InsertOnly)),
                (4, conditional(2, UpdateMode::UpdateOnly)),
            ],
            &metering,
        )
        .unwrap();
    assert_eq!((outcome.rejected, outcome.missing), (1, 1));
    assert_eq!(outcome.usage, UpdateBytes::default());
}

#[test]
fn dense_datatypes_and_quantization_have_separate_byte_sizes() {
    let mut config = test_config();
    config.vectors.get_mut(VECTOR_NAME).unwrap().size = 129;
    let point = resolved(VectorInternal::from(vec![1.0; 129]));
    for (datatype, width) in [
        (VectorStorageDatatype::Float32, 4),
        (VectorStorageDatatype::Float16, 2),
        (VectorStorageDatatype::Uint8, 1),
    ] {
        config.vectors.get_mut(VECTOR_NAME).unwrap().datatype = Some(datatype);
        assert_eq!(
            UpdateMetering::new(&config, []).measure(&point).unwrap(),
            UpdateBytes {
                updated_bytes: 129 * width + 2,
                indexed_bytes: 129 * width,
            }
        );
    }
    config.vectors.get_mut(VECTOR_NAME).unwrap().datatype = Some(VectorStorageDatatype::Float16);
    for (quantization, expected) in [
        (r#"{"scalar":{"type":"int8"}}"#, 148),
        (r#"{"product":{"compression":"x16"}}"#, 33),
        (r#"{"binary":{}}"#, 32),
        (r#"{"turbo":{"bits":"bits8"}}"#, 133),
    ] {
        config.quantization_config = Some(serde_json::from_str(quantization).unwrap());
        assert_eq!(
            UpdateMetering::new(&config, []).measure(&point).unwrap(),
            UpdateBytes {
                updated_bytes: 260,
                indexed_bytes: expected,
            },
            "{quantization}"
        );
    }
}

#[test]
fn multivectors_without_hnsw_have_no_indexing_charge_even_with_quantization() {
    let mut config = test_config();
    let params = config.vectors.get_mut(VECTOR_NAME).unwrap();
    params.size = 3;
    params.datatype = Some(VectorStorageDatatype::Float16);
    params.multivector_config = Some(MultiVectorConfig {
        comparator: MultiVectorComparator::MaxSim,
    });
    params.hnsw_config = Some(HnswConfig {
        m: 0,
        payload_m: Some(0),
        ..Default::default()
    });
    config.quantization_config = Some(serde_json::from_str(r#"{"binary":{}}"#).unwrap());
    let point = resolved(VectorInternal::MultiDense(
        MultiDenseVectorInternal::new_unchecked(vec![vec![1.0; 3], vec![2.0; 3]]),
    ));
    let metering = UpdateMetering::new(&config, []);
    assert_eq!(
        metering.measure(&point).unwrap(),
        UpdateBytes {
            updated_bytes: 14,
            indexed_bytes: 0
        }
    );

    let mut retained = point;
    retained.updated_vectors = NamedVectors::default();
    retained
        .stored_vectors
        .push((VECTOR_NAME.into(), vec![0; 12]));
    assert_eq!(
        metering.measure(&retained).unwrap(),
        UpdateBytes {
            updated_bytes: 14,
            indexed_bytes: 0
        }
    );
}

#[test]
fn retained_vectors_are_counted_once_and_turbo_storage_uses_its_encoded_width() {
    let mut config = test_config();
    config.vectors.get_mut(VECTOR_NAME).unwrap().size = 129;
    for (datatype, vector_bytes) in [
        (VectorStorageDatatype::Turbo4, 69),
        (VectorStorageDatatype::Turbo8, 133),
        (VectorStorageDatatype::Turbo16, 262),
    ] {
        config.vectors.get_mut(VECTOR_NAME).unwrap().datatype = Some(datatype);
        let meter = UpdateMetering::new(&config, []);
        let mut point = resolved(VectorInternal::from(vec![1.0; 129]));
        point
            .stored_vectors
            .push((VECTOR_NAME.into(), vec![0; vector_bytes]));
        let expected = UpdateBytes {
            updated_bytes: vector_bytes as u64 + 2,
            indexed_bytes: vector_bytes as u64,
        };
        assert_eq!(
            meter.measure(&point).unwrap(),
            expected,
            "new vectors override retained ones"
        );
        point.updated_vectors = NamedVectors::default();
        assert_eq!(
            meter.measure(&point).unwrap(),
            expected,
            "retained native bytes use the same width"
        );
        point.stored_vectors[0].1.pop();
        assert!(
            meter.measure(&point).is_err(),
            "truncated rows must not silently undercount"
        );
    }
}

#[test]
fn sparse_vectors_keep_logical_bytes_when_the_index_uses_smaller_values() {
    for (datatype, expected_indexed) in [
        (VectorStorageDatatype::Float32, 16),
        (VectorStorageDatatype::Float16, 12),
        (VectorStorageDatatype::Uint8, 10),
    ] {
        let mut config = test_config();
        config.vectors.clear();
        config.sparse_vectors.insert(
            VECTOR_NAME.into(),
            EdgeSparseVectorParams {
                datatype: Some(datatype),
                ..Default::default()
            },
        );
        let metering = UpdateMetering::new(&config, []);
        let (_dir, writer) = writer(config);
        let mut initial = point(1);
        initial.vector = VectorStructPersisted::from(VectorStructInternal::Named(
            [(
                VECTOR_NAME.into(),
                VectorInternal::Sparse(SparseVector::new(vec![3, 9], vec![1.0, 2.0]).unwrap()),
            )]
            .into(),
        ));
        let (writer, inserted) = writer
            .apply_batch_metered(
                [(1, PointOperation(UpsertPoints(PointsList(vec![initial]))))],
                &metering,
            )
            .unwrap();
        assert_eq!(
            inserted.usage,
            UpdateBytes {
                updated_bytes: 18,
                indexed_bytes: expected_indexed
            }
        );
        let change = PayloadOperation(PayloadOps::SetPayload(SetPayloadOp {
            payload: payload_json! { "a": 1 },
            points: Some(vec![PointId::NumId(1)]),
            filter: None,
            key: None,
        }));
        let (_, updated) = writer
            .apply_batch_metered([(2, change)], &metering)
            .unwrap();
        assert_eq!(
            updated.usage,
            UpdateBytes {
                updated_bytes: 23,
                indexed_bytes: expected_indexed
            }
        );
    }
}
