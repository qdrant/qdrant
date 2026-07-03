//! Resolve filter/condition-based update operations into concrete point ids.
//!
//! Filter-carrying operations decide which points they touch by reading live
//! segment state. Persisting them as-is makes WAL replay nondeterministic:
//! replay-time state can differ from the original apply-time state, so a
//! replayed filter can select a different point set (see issue #9575).
//! These helpers rewrite such
//! operations into their id-based equivalents *before* they are written to
//! the WAL, so the WAL only ever contains operations with a fixed target set.
//!
//! Correctness contract for [`resolve_operation`]: every operation that will
//! precede the rewritten one in WAL order must be fully applied to
//! `segments`, and no new operation may be appended between resolution and
//! the append of the rewritten operation. Callers provide this via the shard
//! update fence.

use common::counter::hardware_counter::HardwareCounterCell;
use segment::common::operation_error::OperationResult;
use segment::types::PointIdType;

use crate::operations::payload_ops::{DeletePayloadOp, PayloadOps, SetPayloadOp};
use crate::operations::point_ops::{ConditionalInsertOperationInternal, PointOperations};
use crate::operations::vector_ops::{UpdateVectorsOp, VectorOperations};
use crate::operations::{CollectionUpdateOperations, FieldIndexOperations, VectorNameOperations};
use crate::segment_holder::SegmentHolder;
use crate::update::{points_by_filter, retain_conditional_upsert_points, select_excluded_by_filter_ids};

/// Does this operation decide its target point set by reading current segment
/// data (a filter or an existence condition)?
///
/// Operations for which this returns `true` must be rewritten with
/// [`resolve_operation`] before being persisted to the WAL.
pub fn is_filter_resolving(operation: &CollectionUpdateOperations) -> bool {
    match operation {
        CollectionUpdateOperations::PointOperation(op) => match op {
            PointOperations::UpsertPointsConditional(_) => true,
            PointOperations::DeletePointsByFilter(_) => true,
            PointOperations::UpsertPoints(_)
            | PointOperations::DeletePoints { .. }
            // `SyncPoints` reads current state too, but every point it touches
            // is alive and version-guarded, so its replay is protected by the
            // per-point version checks.
            | PointOperations::SyncPoints(_) => false,
        },
        CollectionUpdateOperations::VectorOperation(op) => match op {
            VectorOperations::UpdateVectors(update) => update.update_filter.is_some(),
            VectorOperations::DeleteVectorsByFilter(_, _) => true,
            VectorOperations::DeleteVectors(_, _) => false,
        },
        CollectionUpdateOperations::PayloadOperation(op) => match op {
            // An explicit id list takes precedence over the filter on apply,
            // so the op is state-reading only when it has no id list.
            PayloadOps::SetPayload(sp) | PayloadOps::OverwritePayload(sp) => {
                sp.points.is_none() && sp.filter.is_some()
            }
            PayloadOps::DeletePayload(dp) => dp.points.is_none() && dp.filter.is_some(),
            PayloadOps::ClearPayloadByFilter(_) => true,
            PayloadOps::ClearPayload { .. } => false,
        },
        CollectionUpdateOperations::FieldIndexOperation(op) => match op {
            FieldIndexOperations::CreateIndex(_) | FieldIndexOperations::DeleteIndex(_) => false,
        },
        CollectionUpdateOperations::VectorNameOperation(op) => match op {
            VectorNameOperations::CreateVectorName(_)
            | VectorNameOperations::DeleteVectorName(_) => false,
        },
        #[cfg(feature = "staging")]
        CollectionUpdateOperations::StagingOperation(_) => false,
    }
}

/// Rewrite a filter/condition-resolving operation into its id-based
/// equivalent by resolving the filter against current segment state.
///
/// Operations for which [`is_filter_resolving`] is `false` are returned
/// unchanged. The rewritten form only uses pre-existing operation variants,
/// so the WAL format is unaffected.
///
/// See the module docs for the ordering contract callers must uphold.
pub fn resolve_operation(
    segments: &SegmentHolder,
    operation: CollectionUpdateOperations,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<CollectionUpdateOperations> {
    let resolved = match operation {
        CollectionUpdateOperations::PointOperation(op) => {
            CollectionUpdateOperations::PointOperation(match op {
                PointOperations::DeletePointsByFilter(filter) => {
                    let ids = matched_ids(segments, &filter, hw_counter)?;
                    PointOperations::DeletePoints { ids }
                }
                PointOperations::UpsertPointsConditional(op) => {
                    resolve_conditional_upsert(segments, op, hw_counter)?
                }
                op @ (PointOperations::UpsertPoints(_)
                | PointOperations::DeletePoints { .. }
                | PointOperations::SyncPoints(_)) => op,
            })
        }
        CollectionUpdateOperations::VectorOperation(op) => {
            CollectionUpdateOperations::VectorOperation(match op {
                VectorOperations::DeleteVectorsByFilter(filter, vector_names) => {
                    let ids = matched_ids(segments, &filter, hw_counter)?;
                    VectorOperations::DeleteVectors(ids.into(), vector_names)
                }
                VectorOperations::UpdateVectors(update) => {
                    let UpdateVectorsOp {
                        mut points,
                        update_filter,
                    } = update;
                    if let Some(filter) = update_filter {
                        // Mirrors `update_vectors_conditional`: drop points that
                        // exist but do not match the filter.
                        let point_ids = points.iter().map(|point| point.id).collect::<Vec<_>>();
                        let points_to_exclude =
                            select_excluded_by_filter_ids(segments, point_ids, filter, hw_counter)?;
                        points.retain(|point| !points_to_exclude.contains(&point.id));
                    }
                    VectorOperations::UpdateVectors(UpdateVectorsOp {
                        points,
                        update_filter: None,
                    })
                }
                op @ VectorOperations::DeleteVectors(_, _) => op,
            })
        }
        CollectionUpdateOperations::PayloadOperation(op) => {
            CollectionUpdateOperations::PayloadOperation(match op {
                PayloadOps::SetPayload(sp) => {
                    PayloadOps::SetPayload(resolve_set_payload(segments, sp, hw_counter)?)
                }
                PayloadOps::OverwritePayload(sp) => {
                    PayloadOps::OverwritePayload(resolve_set_payload(segments, sp, hw_counter)?)
                }
                PayloadOps::DeletePayload(dp) => {
                    let DeletePayloadOp {
                        keys,
                        points,
                        filter,
                    } = dp;
                    // An explicit id list takes precedence over the filter on apply.
                    let points = match (points, filter) {
                        (None, Some(filter)) => Some(matched_ids(segments, &filter, hw_counter)?),
                        (points, _) => points,
                    };
                    PayloadOps::DeletePayload(DeletePayloadOp {
                        keys,
                        points,
                        filter: None,
                    })
                }
                PayloadOps::ClearPayloadByFilter(filter) => {
                    let points = matched_ids(segments, &filter, hw_counter)?;
                    PayloadOps::ClearPayload { points }
                }
                op @ PayloadOps::ClearPayload { .. } => op,
            })
        }
        op @ (CollectionUpdateOperations::FieldIndexOperation(_)
        | CollectionUpdateOperations::VectorNameOperation(_)) => op,
        #[cfg(feature = "staging")]
        op @ CollectionUpdateOperations::StagingOperation(_) => op,
    };

    Ok(resolved)
}

/// Resolve the point set matched by `filter`, deduplicated and in a
/// deterministic order.
fn matched_ids(
    segments: &SegmentHolder,
    filter: &segment::types::Filter,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<Vec<PointIdType>> {
    // `points_by_filter` flattens per-segment matches, so a point with copies
    // in several segments can appear more than once.
    let mut ids = points_by_filter(segments, filter, hw_counter)?;
    ids.sort_unstable();
    ids.dedup();
    Ok(ids)
}

/// Applies the same point-retention as `update::conditional_upsert`, but
/// instead of upserting the surviving subset it returns it as a plain upsert.
fn resolve_conditional_upsert(
    segments: &SegmentHolder,
    operation: ConditionalInsertOperationInternal,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<PointOperations> {
    let ConditionalInsertOperationInternal {
        mut points_op,
        condition,
        update_mode,
    } = operation;

    retain_conditional_upsert_points(segments, &mut points_op, condition, update_mode, hw_counter)?;

    Ok(PointOperations::UpsertPoints(points_op))
}

fn resolve_set_payload(
    segments: &SegmentHolder,
    operation: SetPayloadOp,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<SetPayloadOp> {
    let SetPayloadOp {
        payload,
        points,
        filter,
        key,
    } = operation;
    // An explicit id list takes precedence over the filter on apply.
    let points = match (points, filter) {
        (None, Some(filter)) => Some(matched_ids(segments, &filter, hw_counter)?),
        (points, _) => points,
    };
    Ok(SetPayloadOp {
        payload,
        points,
        filter: None,
        key,
    })
}

#[cfg(test)]
mod tests {
    use common::counter::hardware_counter::HardwareCounterCell;
    use segment::payload_json;
    use segment::types::{
        Condition, FieldCondition, Filter, Match, MatchValue, Payload, ValueVariants,
    };
    use tempfile::Builder;

    use super::*;
    use crate::fixtures::{build_segment_1, build_segment_2};
    use crate::operations::point_ops::{PointInsertOperationsInternal, UpdateMode};
    use crate::operations::point_ops::{PointStructPersisted, VectorStructPersisted};
    use crate::update::{delete_points_by_filter, points_by_filter, process_point_operation};

    fn color_filter(color: &str) -> Filter {
        Filter::new_must(Condition::Field(FieldCondition::new_match(
            "color".parse().unwrap(),
            Match::Value(MatchValue {
                value: ValueVariants::String(color.to_string()),
            }),
        )))
    }

    fn build_holder(path: &std::path::Path) -> SegmentHolder {
        let mut holder = SegmentHolder::default();
        holder.add_new(build_segment_1(path));
        holder.add_new(build_segment_2(path));
        holder
    }

    fn point(id: u64, payload: Payload) -> PointStructPersisted {
        PointStructPersisted {
            id: id.into(),
            vector: VectorStructPersisted::Single(vec![1.0, 0.0, 0.5, 0.25]),
            payload: Some(payload),
        }
    }

    #[test]
    fn resolve_delete_by_filter_matches_apply() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let hw_counter = HardwareCounterCell::new();

        let holder = build_holder(dir.path());
        let twin_holder = build_holder(dir.path());

        let filter = color_filter("blue");

        let resolved = resolve_operation(
            &holder,
            CollectionUpdateOperations::PointOperation(PointOperations::DeletePointsByFilter(
                filter.clone(),
            )),
            &hw_counter,
        )
        .unwrap();

        let CollectionUpdateOperations::PointOperation(PointOperations::DeletePoints { ids }) =
            &resolved
        else {
            panic!("expected DeletePoints, got {resolved:?}");
        };

        // Deterministic: sorted, no duplicates (points 4 and 5 exist in both segments).
        assert!(!ids.is_empty());
        assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));

        let mut expected = points_by_filter(&holder, &filter, &hw_counter).unwrap();
        expected.sort_unstable();
        expected.dedup();
        assert_eq!(*ids, expected);

        // Applying the resolved op removes the same matches as the by-filter apply.
        let CollectionUpdateOperations::PointOperation(op) = resolved else {
            unreachable!()
        };
        process_point_operation(&holder, 100, op, &hw_counter).unwrap();
        delete_points_by_filter(&twin_holder, 100, &filter, &hw_counter).unwrap();

        let remaining = points_by_filter(&holder, &filter, &hw_counter).unwrap();
        let twin_remaining = points_by_filter(&twin_holder, &filter, &hw_counter).unwrap();
        assert!(remaining.is_empty(), "resolved delete left {remaining:?}");
        assert!(twin_remaining.is_empty());
    }

    #[test]
    fn resolve_conditional_insert_only_drops_existing_points() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let hw_counter = HardwareCounterCell::new();
        let holder = build_holder(dir.path());

        // Point 1 exists, point 100 does not.
        let operation = CollectionUpdateOperations::PointOperation(
            PointOperations::UpsertPointsConditional(ConditionalInsertOperationInternal {
                points_op: PointInsertOperationsInternal::PointsList(vec![
                    point(1, payload_json! {"color": "white"}),
                    point(100, payload_json! {"color": "white"}),
                ]),
                condition: color_filter("white"),
                update_mode: Some(UpdateMode::InsertOnly),
            }),
        );

        let resolved = resolve_operation(&holder, operation, &hw_counter).unwrap();

        let CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(points_op)) =
            resolved
        else {
            panic!("expected plain UpsertPoints");
        };
        assert_eq!(points_op.point_ids(), vec![100.into()]);
    }

    #[test]
    fn resolve_set_payload_filter_to_points() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let hw_counter = HardwareCounterCell::new();
        let holder = build_holder(dir.path());

        let operation =
            CollectionUpdateOperations::PayloadOperation(PayloadOps::SetPayload(SetPayloadOp {
                payload: payload_json! {"processed": true},
                points: None,
                filter: Some(color_filter("red")),
                key: None,
            }));

        let resolved = resolve_operation(&holder, operation, &hw_counter).unwrap();

        let CollectionUpdateOperations::PayloadOperation(PayloadOps::SetPayload(sp)) = resolved
        else {
            panic!("expected SetPayload");
        };
        assert!(sp.filter.is_none());
        let points = sp.points.expect("points must be resolved");
        assert!(!points.is_empty());

        let mut expected = points_by_filter(&holder, &color_filter("red"), &hw_counter).unwrap();
        expected.sort_unstable();
        expected.dedup();
        assert_eq!(points, expected);
    }

    #[test]
    fn resolve_leaves_id_based_operations_unchanged() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let hw_counter = HardwareCounterCell::new();
        let holder = build_holder(dir.path());

        let operation = CollectionUpdateOperations::PointOperation(PointOperations::DeletePoints {
            ids: vec![1.into(), 2.into()],
        });
        assert!(!is_filter_resolving(&operation));

        let resolved = resolve_operation(&holder, operation.clone(), &hw_counter).unwrap();
        assert_eq!(resolved, operation);
    }

    #[test]
    fn is_filter_resolving_covers_filter_variants() {
        let filter = color_filter("red");

        assert!(is_filter_resolving(
            &CollectionUpdateOperations::PointOperation(PointOperations::DeletePointsByFilter(
                filter.clone()
            ))
        ));
        assert!(is_filter_resolving(
            &CollectionUpdateOperations::PayloadOperation(PayloadOps::ClearPayloadByFilter(
                filter.clone()
            ))
        ));
        assert!(is_filter_resolving(
            &CollectionUpdateOperations::VectorOperation(VectorOperations::DeleteVectorsByFilter(
                filter.clone(),
                vec![]
            ))
        ));
        assert!(is_filter_resolving(
            &CollectionUpdateOperations::VectorOperation(VectorOperations::UpdateVectors(
                UpdateVectorsOp {
                    points: vec![],
                    update_filter: Some(filter.clone()),
                }
            ))
        ));

        // An explicit id list wins over the filter: not state-reading.
        assert!(!is_filter_resolving(
            &CollectionUpdateOperations::PayloadOperation(PayloadOps::SetPayload(SetPayloadOp {
                payload: payload_json! {"a": 1},
                points: Some(vec![1.into()]),
                filter: Some(filter),
                key: None,
            }))
        ));
        assert!(!is_filter_resolving(
            &CollectionUpdateOperations::VectorOperation(VectorOperations::UpdateVectors(
                UpdateVectorsOp {
                    points: vec![],
                    update_filter: None,
                }
            ))
        ));
    }

}
