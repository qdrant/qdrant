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
//!
//! Resolution also reports how many points a filter *scan* selected, so the
//! caller can gate oversized updates on the point count this shard is about to
//! touch (`max_update_by_filter_limit` in strict mode). Callers that pass that
//! limit get a scan whose cost is bounded by it: the resolver stops reading
//! once it knows the match set is bigger, and the operation it returns is then
//! truncated and must be rejected rather than applied.

use common::counter::hardware_counter::HardwareCounterCell;
use segment::common::operation_error::OperationResult;
use segment::types::PointIdType;

use crate::operations::payload_ops::{DeletePayloadOp, PayloadOps, SetPayloadOp};
use crate::operations::point_ops::{ConditionalInsertOperationInternal, PointOperations};
use crate::operations::vector_ops::{UpdateVectorsOp, VectorOperations};
use crate::operations::{CollectionUpdateOperations, FieldIndexOperations, VectorNameOperations};
use crate::segment_holder::SegmentHolder;
use crate::update::{
    FilteredPoints, points_by_filter_limited, retain_conditional_upsert_points,
    select_excluded_by_filter_ids,
};

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
            | PointOperations::UpsertPointsRaw(_)
            | PointOperations::DeletePoints { .. }
            // `SyncPoints` reads current state too, but every point it touches
            // is alive and version-guarded, so its replay is protected by the
            // per-point version checks.
            | PointOperations::SyncPoints(_)
            | PointOperations::SyncPointsRaw(_) => false,
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

/// Outcome of [`resolve_operation`].
#[derive(Debug)]
pub struct ResolvedOperation {
    /// The operation, rewritten to its id-based form.
    pub operation: CollectionUpdateOperations,

    /// How many points a filter scan selected, for operations that pick their
    /// targets by scanning this shard (delete by filter, payload by filter,
    /// clear payload by filter, delete vectors by filter).
    ///
    /// `None` when the target set came from the client instead: an explicit id
    /// list, a conditional upsert or an `update_filter`, which only trim a
    /// point list the client already sent.
    ///
    /// A count above the `limit` passed to [`resolve_operation`] is only a
    /// lower bound, and [`ResolvedOperation::operation`] holds a truncated
    /// point set: over the limit the caller must reject the operation.
    pub scanned_points: Option<usize>,
}

/// Rewrite a filter/condition-resolving operation into its id-based
/// equivalent by resolving the filter against current segment state.
///
/// Operations for which [`is_filter_resolving`] is `false` are returned
/// unchanged. The rewritten form only uses pre-existing operation variants,
/// so the WAL format is unaffected.
///
/// `limit` caps what a filter scan is allowed to select. It buys a bounded
/// scan, not a different result: under the limit the resolved operation is
/// exactly the one an unbounded scan produces, and above it the scan stops
/// early and reports a count over the limit for the caller to reject.
///
/// See the module docs for the ordering contract callers must uphold.
pub fn resolve_operation(
    segments: &SegmentHolder,
    operation: CollectionUpdateOperations,
    limit: Option<usize>,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<ResolvedOperation> {
    let mut scanned_points = None;

    let resolved = match operation {
        CollectionUpdateOperations::PointOperation(op) => {
            CollectionUpdateOperations::PointOperation(match op {
                PointOperations::DeletePointsByFilter(filter) => {
                    let ids = matched_ids(segments, &filter, limit, hw_counter)?;
                    scanned_points = Some(ids.len());
                    PointOperations::DeletePoints { ids }
                }
                PointOperations::UpsertPointsConditional(op) => {
                    resolve_conditional_upsert(segments, op, hw_counter)?
                }
                op @ (PointOperations::UpsertPoints(_)
                | PointOperations::UpsertPointsRaw(_)
                | PointOperations::DeletePoints { .. }
                | PointOperations::SyncPoints(_)
                | PointOperations::SyncPointsRaw(_)) => op,
            })
        }
        CollectionUpdateOperations::VectorOperation(op) => {
            CollectionUpdateOperations::VectorOperation(match op {
                VectorOperations::DeleteVectorsByFilter(filter, vector_names) => {
                    let ids = matched_ids(segments, &filter, limit, hw_counter)?;
                    scanned_points = Some(ids.len());
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
                PayloadOps::SetPayload(sp) => PayloadOps::SetPayload(resolve_set_payload(
                    segments,
                    sp,
                    limit,
                    &mut scanned_points,
                    hw_counter,
                )?),
                PayloadOps::OverwritePayload(sp) => PayloadOps::OverwritePayload(
                    resolve_set_payload(segments, sp, limit, &mut scanned_points, hw_counter)?,
                ),
                PayloadOps::DeletePayload(dp) => {
                    let DeletePayloadOp {
                        keys,
                        points,
                        filter,
                    } = dp;
                    let points = resolve_points_or_filter(
                        segments,
                        points,
                        filter,
                        limit,
                        &mut scanned_points,
                        hw_counter,
                    )?;
                    PayloadOps::DeletePayload(DeletePayloadOp {
                        keys,
                        points,
                        filter: None,
                    })
                }
                PayloadOps::ClearPayloadByFilter(filter) => {
                    let points = matched_ids(segments, &filter, limit, hw_counter)?;
                    scanned_points = Some(points.len());
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

    Ok(ResolvedOperation {
        operation: resolved,
        scanned_points,
    })
}

/// Resolve the point set matched by `filter`, deduplicated and in a
/// deterministic order.
///
/// With a `limit`, the read stops one point past it in every segment. That is
/// enough to tell an over-the-limit scan from an exact match set: a result
/// above the limit is a lower bound the caller rejects on, and a result at or
/// below it is the complete set, unread points included.
fn matched_ids(
    segments: &SegmentHolder,
    filter: &segment::types::Filter,
    limit: Option<usize>,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<Vec<PointIdType>> {
    let budget = limit.map(|limit| limit + 1);
    let FilteredPoints { points, truncated } =
        points_by_filter_limited(segments, filter, budget, hw_counter)?;
    let ids = sorted_unique(points);

    // Cross-segment duplicates and the deferred-points correction both shrink
    // the read, so a truncated one can land back under the limit. The count is
    // then neither complete nor decisive, and only the full scan can say which
    // side of the limit the filter falls on. It takes a single segment holding
    // more matches than the limit, so it is the rare case, not the scan we set
    // out to avoid.
    if truncated && limit.is_some_and(|limit| ids.len() <= limit) {
        let FilteredPoints { points, .. } =
            points_by_filter_limited(segments, filter, None, hw_counter)?;
        return Ok(sorted_unique(points));
    }

    Ok(ids)
}

/// `points_by_filter_limited` flattens per-segment matches, so a point with
/// copies in several segments can appear more than once.
fn sorted_unique(mut ids: Vec<PointIdType>) -> Vec<PointIdType> {
    ids.sort_unstable();
    ids.dedup();
    ids
}

/// Resolve the `(points, filter)` pair of a payload operation. An explicit id
/// list takes precedence over the filter on apply, so a filter only resolves
/// when there is no id list.
fn resolve_points_or_filter(
    segments: &SegmentHolder,
    points: Option<Vec<PointIdType>>,
    filter: Option<segment::types::Filter>,
    limit: Option<usize>,
    scanned_points: &mut Option<usize>,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<Option<Vec<PointIdType>>> {
    match (points, filter) {
        (None, Some(filter)) => {
            let ids = matched_ids(segments, &filter, limit, hw_counter)?;
            *scanned_points = Some(ids.len());
            Ok(Some(ids))
        }
        (points, _) => Ok(points),
    }
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
    limit: Option<usize>,
    scanned_points: &mut Option<usize>,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<SetPayloadOp> {
    let SetPayloadOp {
        payload,
        points,
        filter,
        key,
    } = operation;
    let points =
        resolve_points_or_filter(segments, points, filter, limit, scanned_points, hw_counter)?;
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
    use crate::operations::point_ops::{
        PointInsertOperationsInternal, PointStructPersisted, UpdateMode, VectorStructPersisted,
    };
    use crate::update::{delete_points_by_filter, process_point_operation};

    /// Every point matching `filter`, read without a budget.
    fn all_points_by_filter(
        holder: &SegmentHolder,
        filter: &Filter,
        hw_counter: &HardwareCounterCell,
    ) -> Vec<PointIdType> {
        points_by_filter_limited(holder, filter, None, hw_counter)
            .unwrap()
            .points
    }

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

        let ResolvedOperation {
            operation: resolved,
            scanned_points,
        } = resolve_operation(
            &holder,
            CollectionUpdateOperations::PointOperation(PointOperations::DeletePointsByFilter(
                filter.clone(),
            )),
            None,
            &hw_counter,
        )
        .unwrap();

        let CollectionUpdateOperations::PointOperation(PointOperations::DeletePoints { ids }) =
            &resolved
        else {
            panic!("expected DeletePoints, got {resolved:?}");
        };

        // A filter scan reports its exact match count for the strict mode gate.
        assert_eq!(scanned_points, Some(ids.len()));

        // Deterministic: sorted, no duplicates (points 4 and 5 exist in both segments).
        assert!(!ids.is_empty());
        assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));

        let mut expected = all_points_by_filter(&holder, &filter, &hw_counter);
        expected.sort_unstable();
        expected.dedup();
        assert_eq!(*ids, expected);

        // Applying the resolved op removes the same matches as the by-filter apply.
        let CollectionUpdateOperations::PointOperation(op) = resolved else {
            unreachable!()
        };
        process_point_operation(&holder, 100, op, None, &hw_counter).unwrap();
        delete_points_by_filter(&twin_holder, 100, &filter, &hw_counter).unwrap();

        let remaining = all_points_by_filter(&holder, &filter, &hw_counter);
        let twin_remaining = all_points_by_filter(&twin_holder, &filter, &hw_counter);
        assert!(remaining.is_empty(), "resolved delete left {remaining:?}");
        assert!(twin_remaining.is_empty());
    }

    /// The `limit` only buys a bounded scan: whatever it is, resolution either
    /// returns the complete match set or a count the caller can reject on.
    #[test]
    fn resolve_with_a_limit_is_complete_or_decisive() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let hw_counter = HardwareCounterCell::new();
        let holder = build_holder(dir.path());
        let filter = color_filter("blue");

        let mut full = all_points_by_filter(&holder, &filter, &hw_counter);
        full.sort_unstable();
        full.dedup();
        // Points 4 and 5 live in both segments, so the per-segment budget and
        // the deduplicated result disagree, which is what the fallback is for.
        assert!(full.len() >= 2, "fixture must match several points");

        for limit in 0..=full.len() + 1 {
            let ResolvedOperation {
                operation: resolved,
                scanned_points,
            } = resolve_operation(
                &holder,
                CollectionUpdateOperations::PointOperation(PointOperations::DeletePointsByFilter(
                    filter.clone(),
                )),
                Some(limit),
                &hw_counter,
            )
            .unwrap();

            let CollectionUpdateOperations::PointOperation(PointOperations::DeletePoints { ids }) =
                &resolved
            else {
                panic!("expected DeletePoints, got {resolved:?}");
            };
            assert_eq!(scanned_points, Some(ids.len()));

            if full.len() > limit {
                assert!(
                    ids.len() > limit,
                    "limit {limit}: a scan over the limit must report over it, got {}",
                    ids.len(),
                );
            } else {
                assert_eq!(
                    *ids, full,
                    "limit {limit}: a scan within the limit must be the complete set",
                );
            }
        }
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

        let ResolvedOperation {
            operation: resolved,
            scanned_points,
        } = resolve_operation(&holder, operation, None, &hw_counter).unwrap();

        let CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(points_op)) =
            resolved
        else {
            panic!("expected plain UpsertPoints");
        };
        assert_eq!(points_op.point_ids(), vec![100.into()]);

        // A conditional upsert only trims the client's own point list, so it
        // is not a scan and must not be gated by the update-by-filter limit.
        assert_eq!(scanned_points, None);
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

        let ResolvedOperation {
            operation: resolved,
            scanned_points,
        } = resolve_operation(&holder, operation, None, &hw_counter).unwrap();

        let CollectionUpdateOperations::PayloadOperation(PayloadOps::SetPayload(sp)) = resolved
        else {
            panic!("expected SetPayload");
        };
        assert!(sp.filter.is_none());
        let points = sp.points.expect("points must be resolved");
        assert!(!points.is_empty());
        assert_eq!(scanned_points, Some(points.len()));

        let mut expected = all_points_by_filter(&holder, &color_filter("red"), &hw_counter);
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

        let resolved = resolve_operation(&holder, operation.clone(), None, &hw_counter).unwrap();
        assert_eq!(resolved.operation, operation);
        assert_eq!(resolved.scanned_points, None);
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
