use segment::payload_json;
use segment::types::{Payload, PointIdType};
use shard::operations::CollectionUpdateOperations;
use shard::operations::payload_ops::{PayloadOps, SetPayloadOp};
use shard::operations::point_ops::{PointOperations, PointStructPersisted, VectorStructPersisted};

use super::UpdateBatchPlan;

fn point_id(id: u64) -> PointIdType {
    PointIdType::NumId(id)
}

fn upsert(id: u64, payload: Payload) -> CollectionUpdateOperations {
    CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        vec![PointStructPersisted {
            id: point_id(id),
            vector: VectorStructPersisted::Single(vec![1.0, 0.0]),
            payload: Some(payload),
        }]
        .into(),
    ))
}

fn set_payload(id: u64, payload: Payload) -> CollectionUpdateOperations {
    CollectionUpdateOperations::PayloadOperation(PayloadOps::SetPayload(SetPayloadOp {
        payload,
        points: Some(vec![point_id(id)]),
        filter: None,
        key: None,
    }))
}

fn delete(id: u64) -> CollectionUpdateOperations {
    CollectionUpdateOperations::PointOperation(PointOperations::DeletePoints {
        ids: vec![point_id(id)],
    })
}

/// Operations on the same point collapse into one entry, and the merged
/// payload is the fold of all of them.
#[test]
fn folds_operations_on_the_same_point() {
    let plan = UpdateBatchPlan::build([
        (1, upsert(7, payload_json! { "a": 1 })),
        (2, set_payload(7, payload_json! { "b": 2 })),
    ])
    .unwrap();

    assert_eq!(plan.len(), 1);
    // The upsert supplies the whole point, so nothing has to be read.
    assert_eq!(plan.point_ids_needing_stored_point().count(), 0);

    let (id, updates) = plan.into_point_updates().next().unwrap();
    assert_eq!(id, point_id(7));
    assert_eq!(updates.version(), 2);

    let point = updates.materialize(id, None).unwrap().unwrap();
    assert_eq!(point.version, 2);
    assert_eq!(point.payload, payload_json! { "a": 1, "b": 2 });
}

/// A batch that only modifies a point has to read it first.
#[test]
fn modification_only_batch_needs_the_stored_point() {
    let plan = UpdateBatchPlan::build([(1, set_payload(7, payload_json! { "b": 2 }))]).unwrap();

    assert_eq!(
        plan.point_ids_needing_stored_point().collect::<Vec<_>>(),
        vec![point_id(7)],
    );
}

/// A delete discards everything before it: the point is neither read nor
/// written.
#[test]
fn delete_discards_preceding_operations() {
    let plan = UpdateBatchPlan::build([
        (1, set_payload(7, payload_json! { "b": 2 })),
        (2, delete(7)),
    ])
    .unwrap();

    assert_eq!(plan.point_ids_needing_stored_point().count(), 0);

    let (id, updates) = plan.into_point_updates().next().unwrap();
    assert!(updates.materialize(id, None).unwrap().is_none());
}

/// ... and an upsert after a delete brings the point back.
#[test]
fn upsert_after_delete_recreates_the_point() {
    let plan =
        UpdateBatchPlan::build([(1, delete(7)), (2, upsert(7, payload_json! { "a": 1 }))]).unwrap();

    let (id, updates) = plan.into_point_updates().next().unwrap();
    let point = updates.materialize(id, None).unwrap().unwrap();
    assert_eq!(point.payload, payload_json! { "a": 1 });
}

/// Filter-selected operations are rejected up front, not silently applied to
/// nothing.
#[test]
fn rejects_filter_selected_operations() {
    let operation =
        CollectionUpdateOperations::PayloadOperation(PayloadOps::SetPayload(SetPayloadOp {
            payload: payload_json! { "a": 1 },
            points: None,
            filter: Some(Default::default()),
            key: None,
        }));

    assert!(UpdateBatchPlan::build([(1, operation)]).is_err());
}
