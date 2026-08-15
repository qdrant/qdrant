use segment::payload_json;
use segment::types::{Condition, Filter, HasIdCondition, Payload, PointIdType};
use shard::operations::CollectionUpdateOperations;
use shard::operations::payload_ops::{PayloadOps, SetPayloadOp};
use shard::operations::point_ops::{
    ConditionalInsertOperationInternal, PointOperations, PointStructPersisted, UpdateMode,
    VectorStructPersisted,
};

use super::UpdateBatchPlan;

fn point_id(id: u64) -> PointIdType {
    PointIdType::NumId(id)
}

fn point(id: u64, payload: Payload) -> PointStructPersisted {
    PointStructPersisted {
        id: point_id(id),
        vector: VectorStructPersisted::Single(vec![1.0, 0.0]),
        payload: Some(payload),
    }
}

fn upsert(id: u64, payload: Payload) -> CollectionUpdateOperations {
    CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        vec![point(id, payload)].into(),
    ))
}

fn conditional_upsert(
    id: u64,
    payload: Payload,
    mode: UpdateMode,
    condition: Filter,
) -> CollectionUpdateOperations {
    CollectionUpdateOperations::PointOperation(PointOperations::UpsertPointsConditional(
        ConditionalInsertOperationInternal {
            points_op: vec![point(id, payload)].into(),
            condition,
            update_mode: Some(mode),
        },
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

    let point = updates.materialize(id, false, None).unwrap().unwrap();
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
    assert!(updates.materialize(id, false, None).unwrap().is_none());
}

/// ... and an upsert after a delete brings the point back.
#[test]
fn upsert_after_delete_recreates_the_point() {
    let plan =
        UpdateBatchPlan::build([(1, delete(7)), (2, upsert(7, payload_json! { "a": 1 }))]).unwrap();

    let (id, updates) = plan.into_point_updates().next().unwrap();
    let point = updates.materialize(id, false, None).unwrap().unwrap();
    assert_eq!(point.payload, payload_json! { "a": 1 });
}

/// An `insert_only` upsert of a point that is already there applies nothing —
/// and the point it would have overwritten is never read.
#[test]
fn insert_only_upsert_leaves_an_existing_point_alone() {
    let plan = UpdateBatchPlan::build([(
        1,
        conditional_upsert(
            7,
            payload_json! { "a": 1 },
            UpdateMode::InsertOnly,
            Filter::default(),
        ),
    )])
    .unwrap();

    assert_eq!(plan.point_ids_needing_stored_point().count(), 0);

    let (id, updates) = plan.into_point_updates().next().unwrap();
    assert!(!updates.applies_any(true));
    // ...and the same operation does create the point when it is not there.
    assert!(updates.applies_any(false));
    assert_eq!(
        updates
            .materialize(id, false, None)
            .unwrap()
            .unwrap()
            .payload,
        payload_json! { "a": 1 },
    );
}

/// An `update_only` upsert of a point no segment holds applies nothing.
#[test]
fn update_only_upsert_does_not_create_a_missing_point() {
    let plan = UpdateBatchPlan::build([(
        1,
        conditional_upsert(
            7,
            payload_json! { "a": 1 },
            UpdateMode::UpdateOnly,
            Filter::default(),
        ),
    )])
    .unwrap();

    let (_id, updates) = plan.into_point_updates().next().unwrap();
    assert!(!updates.applies_any(false));
    assert!(updates.applies_any(true));
}

/// The condition is judged where the upsert sits in the fold, not against the
/// state the batch started from: a point an earlier operation of the same
/// batch created counts as existing, so an `insert_only` upsert after it does
/// not overwrite it. That mirrors the leader, which resolves each operation
/// only after the ones before it were applied.
#[test]
fn insert_only_upsert_does_not_overwrite_what_the_batch_just_created() {
    let plan = UpdateBatchPlan::build([
        (1, upsert(7, payload_json! { "a": 1 })),
        (
            2,
            conditional_upsert(
                7,
                payload_json! { "a": 2 },
                UpdateMode::InsertOnly,
                Filter::default(),
            ),
        ),
    ])
    .unwrap();

    let (id, updates) = plan.into_point_updates().next().unwrap();
    // The conditional upsert may not apply, so it may not discard the plain
    // upsert it follows.
    assert!(updates.applies_any(false));

    let point = updates.materialize(id, false, None).unwrap().unwrap();
    assert_eq!(point.payload, payload_json! { "a": 1 });
    assert_eq!(point.version, 2);
}

/// A conditional upsert carrying a real condition needs payload indexes the
/// writer never fetches, so it is rejected rather than silently applied as if
/// the condition held.
#[test]
fn rejects_conditional_upsert_with_a_condition() {
    let condition = Filter::new_must(Condition::HasId(HasIdCondition::from(
        [point_id(7)].into_iter().collect::<ahash::AHashSet<_>>(),
    )));

    for mode in [
        UpdateMode::Upsert,
        UpdateMode::InsertOnly,
        UpdateMode::UpdateOnly,
    ] {
        let operation = conditional_upsert(7, payload_json! { "a": 1 }, mode, condition.clone());
        assert!(UpdateBatchPlan::build([(1, operation)]).is_err());
    }
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
