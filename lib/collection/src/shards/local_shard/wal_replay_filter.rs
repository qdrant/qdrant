//! Filtering helpers used by `LocalShard::load_from_wal` to make WAL replay
//! tolerant of references to vector names that no longer exist in the current
//! `CollectionParams`.
//!
//! ## Why this exists
//!
//! Schema-mutating ops (`delete_named_vector`, in the future possibly others)
//! mutate `CollectionParams` and synchronously persist each segment's mutated
//! `SegmentConfig` while older operations referencing the just-deleted vector
//! name may still be in the un-truncated WAL. Without filtering, a reopen
//! re-applies those historical operations against the post-mutation segment
//! config and the strict `check_named_vectors` check rejects them with
//! `VectorNameNotExists`. The error is logged and swallowed by the replay
//! loop, so the entire operation (e.g. an `Upsert` for a point with two
//! vectors) is silently dropped — including the still-valid sub-operations
//! against vectors that *do* still exist.
//!
//! This module provides [`filter_replay_operation`], which mutates an
//! incoming WAL operation in-place, dropping every reference to a vector
//! name that is no longer in the live `CollectionParams`. It is purely
//! conservative: it only ever drops references that *would* cause a
//! validation error; user-visible data for live names is preserved.
//!
//! It is invoked exclusively from the WAL replay path. Normal write paths
//! (which validate against the live config first) are unchanged.

use std::collections::HashSet;

use segment::types::VectorNameBuf;
use shard::operations::CollectionUpdateOperations;
use shard::operations::point_ops::{
    BatchVectorStructPersisted, PointInsertOperationsInternal, PointOperations,
    PointStructPersisted, VectorStructPersisted,
};
use shard::operations::vector_ops::{PointVectorsPersisted, VectorOperations};

use crate::config::CollectionParams;

/// Result of filtering a WAL operation.
#[derive(Debug)]
pub(super) enum FilteredReplayOp {
    /// The operation was unchanged.
    Unchanged,
    /// The operation was edited in-place; some vector-name references were dropped.
    /// `dropped_names` is the set of names that were filtered out (for logging).
    Filtered {
        dropped_names: HashSet<VectorNameBuf>,
    },
    /// The operation became a no-op after filtering (no live names left and
    /// nothing else to do); the caller should skip dispatching it.
    Empty {
        dropped_names: HashSet<VectorNameBuf>,
    },
}

/// Drop references to vector names that are not present in `params` from `op`.
///
/// Returns [`FilteredReplayOp`] describing what happened so the caller can
/// log dropped names at debug level and skip empty operations.
pub(super) fn filter_replay_operation(
    op: &mut CollectionUpdateOperations,
    params: &CollectionParams,
) -> FilteredReplayOp {
    let mut dropped = HashSet::new();
    let known = KnownVectorNames::from_params(params);

    match op {
        CollectionUpdateOperations::PointOperation(point_op) => match point_op {
            PointOperations::UpsertPoints(insert) => {
                filter_insert_op(insert, &known, &mut dropped);
            }
            PointOperations::UpsertPointsConditional(cond) => {
                filter_insert_op(&mut cond.points_op, &known, &mut dropped);
            }
            PointOperations::SyncPoints(sync) => {
                filter_points_list(&mut sync.points, &known, &mut dropped);
            }
            PointOperations::DeletePoints { .. } | PointOperations::DeletePointsByFilter(_) => {}
        },
        CollectionUpdateOperations::VectorOperation(vector_op) => match vector_op {
            VectorOperations::UpdateVectors(update) => {
                filter_update_vectors(&mut update.points, &known, &mut dropped);
            }
            VectorOperations::DeleteVectors(_, names)
            | VectorOperations::DeleteVectorsByFilter(_, names) => {
                names.retain(|name| {
                    let keep = known.contains(name);
                    if !keep {
                        dropped.insert(name.clone());
                    }
                    keep
                });
            }
        },
        // Schema and field-index ops are idempotent against the current state:
        // - `DeleteVectorName` for an already-removed name is a no-op in
        //   `Segment::delete_vector_name_impl`.
        // - `CreateVectorName` for an already-present matching name is a no-op
        //   in `add_vector_to_config`.
        CollectionUpdateOperations::VectorNameOperation(_)
        | CollectionUpdateOperations::PayloadOperation(_)
        | CollectionUpdateOperations::FieldIndexOperation(_) => {}
        #[cfg(feature = "staging")]
        CollectionUpdateOperations::StagingOperation(_) => {}
    }

    if dropped.is_empty() {
        return FilteredReplayOp::Unchanged;
    }

    if is_now_empty(op) {
        FilteredReplayOp::Empty {
            dropped_names: dropped,
        }
    } else {
        FilteredReplayOp::Filtered {
            dropped_names: dropped,
        }
    }
}

/// Snapshot of known dense and sparse vector names from `CollectionParams`.
///
/// Lookups use `&str` to avoid allocating during filtering.
struct KnownVectorNames<'a> {
    dense: HashSet<&'a str>,
    sparse: HashSet<&'a str>,
}

impl<'a> KnownVectorNames<'a> {
    fn from_params(params: &'a CollectionParams) -> Self {
        // `VectorsConfig::params_iter` already emits `DEFAULT_VECTOR_NAME` for
        // the `Single` shape, so this covers both default-unnamed and
        // multi-named collections uniformly.
        let dense: HashSet<&'a str> = params.vectors.params_iter().map(|(name, _)| name).collect();

        let sparse: HashSet<&'a str> = params
            .sparse_vectors
            .as_ref()
            .map(|m| m.keys().map(|k| k.as_str()).collect())
            .unwrap_or_default();

        Self { dense, sparse }
    }

    fn contains(&self, name: &str) -> bool {
        self.dense.contains(name) || self.sparse.contains(name)
    }
}

fn filter_insert_op(
    insert: &mut PointInsertOperationsInternal,
    known: &KnownVectorNames<'_>,
    dropped: &mut HashSet<VectorNameBuf>,
) {
    match insert {
        PointInsertOperationsInternal::PointsList(points) => {
            filter_points_list(points, known, dropped);
        }
        PointInsertOperationsInternal::PointsBatch(batch) => {
            if let BatchVectorStructPersisted::Named(map) = &mut batch.vectors {
                map.retain(|name, _| {
                    let keep = known.contains(name);
                    if !keep {
                        dropped.insert(name.clone());
                    }
                    keep
                });
            }
            // For `Single`/`MultiDense` shapes the name is `DEFAULT_VECTOR_NAME`,
            // which we leave to the segment-level check (it's only ever absent
            // if someone deletes the default vector, in which case the upsert
            // legitimately cannot apply and the strict path's existing error
            // logging is fine).
        }
    }
}

fn filter_points_list(
    points: &mut Vec<PointStructPersisted>,
    known: &KnownVectorNames<'_>,
    dropped: &mut HashSet<VectorNameBuf>,
) {
    points.retain_mut(|point| {
        if let VectorStructPersisted::Named(map) = &mut point.vector {
            map.retain(|name, _| {
                let keep = known.contains(name);
                if !keep {
                    dropped.insert(name.clone());
                }
                keep
            });
            // Drop the whole point if every named vector was dropped — there is
            // nothing for the segment to upsert. (We could keep an "id-only"
            // upsert with payload, but that would change semantics; better to
            // drop than to silently invent.)
            !map.is_empty()
        } else {
            true
        }
    });
}

fn filter_update_vectors(
    points: &mut Vec<PointVectorsPersisted>,
    known: &KnownVectorNames<'_>,
    dropped: &mut HashSet<VectorNameBuf>,
) {
    points.retain_mut(|point| {
        if let VectorStructPersisted::Named(map) = &mut point.vector {
            map.retain(|name, _| {
                let keep = known.contains(name);
                if !keep {
                    dropped.insert(name.clone());
                }
                keep
            });
            !map.is_empty()
        } else {
            true
        }
    });
}

/// Whether the operation has nothing left to do after filtering.
fn is_now_empty(op: &CollectionUpdateOperations) -> bool {
    match op {
        CollectionUpdateOperations::PointOperation(point_op) => match point_op {
            PointOperations::UpsertPoints(insert) => insert_op_is_empty(insert),
            PointOperations::UpsertPointsConditional(cond) => insert_op_is_empty(&cond.points_op),
            PointOperations::SyncPoints(sync) => sync.points.is_empty(),
            PointOperations::DeletePoints { .. } | PointOperations::DeletePointsByFilter(_) => {
                false
            }
        },
        CollectionUpdateOperations::VectorOperation(vector_op) => match vector_op {
            VectorOperations::UpdateVectors(update) => update.points.is_empty(),
            VectorOperations::DeleteVectors(_, names)
            | VectorOperations::DeleteVectorsByFilter(_, names) => names.is_empty(),
        },
        CollectionUpdateOperations::PayloadOperation(_)
        | CollectionUpdateOperations::FieldIndexOperation(_)
        | CollectionUpdateOperations::VectorNameOperation(_) => false,
        #[cfg(feature = "staging")]
        CollectionUpdateOperations::StagingOperation(_) => false,
    }
}

fn insert_op_is_empty(insert: &PointInsertOperationsInternal) -> bool {
    match insert {
        PointInsertOperationsInternal::PointsList(points) => points.is_empty(),
        PointInsertOperationsInternal::PointsBatch(batch) => batch.ids.is_empty(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::num::NonZeroU64;

    use segment::types::{Distance, VectorNameBuf};
    use shard::operations::point_ops::{
        PointInsertOperationsInternal, PointOperations, PointStructPersisted, VectorPersisted,
        VectorStructPersisted,
    };

    use super::*;
    use crate::operations::types::{VectorParams, VectorsConfig};

    fn make_params(dense: &[&str]) -> CollectionParams {
        use crate::config::{
            default_on_disk_payload, default_replication_factor, default_shard_number,
            default_write_consistency_factor,
        };
        let mut map: BTreeMap<VectorNameBuf, VectorParams> = BTreeMap::new();
        for name in dense {
            map.insert(
                (*name).to_owned(),
                VectorParams {
                    size: NonZeroU64::new(4).unwrap(),
                    distance: Distance::Cosine,
                    hnsw_config: None,
                    quantization_config: None,
                    on_disk: None,
                    datatype: None,
                    multivector_config: None,
                },
            );
        }
        CollectionParams {
            vectors: VectorsConfig::Multi(map),
            shard_number: default_shard_number(),
            sharding_method: None,
            replication_factor: default_replication_factor(),
            write_consistency_factor: default_write_consistency_factor(),
            read_fan_out_factor: None,
            read_fan_out_delay_ms: None,
            on_disk_payload: default_on_disk_payload(),
            sparse_vectors: None,
        }
    }

    fn point_with_named(id: u64, names: &[(&str, Vec<f32>)]) -> PointStructPersisted {
        let mut map: HashMap<VectorNameBuf, VectorPersisted> = HashMap::new();
        for (name, vec) in names {
            map.insert((*name).to_owned(), VectorPersisted::Dense(vec.clone()));
        }
        PointStructPersisted {
            id: id.into(),
            vector: VectorStructPersisted::Named(map),
            payload: None,
        }
    }

    #[test]
    fn drops_unknown_named_vector_keeps_known() {
        let params = make_params(&["a"]);
        let mut op = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
            PointInsertOperationsInternal::PointsList(vec![point_with_named(
                1,
                &[("a", vec![1.0; 4]), ("b", vec![2.0; 4])],
            )]),
        ));
        let result = filter_replay_operation(&mut op, &params);
        match &result {
            FilteredReplayOp::Filtered { dropped_names } => {
                assert!(dropped_names.contains("b"));
                assert!(!dropped_names.contains("a"));
            }
            FilteredReplayOp::Unchanged | FilteredReplayOp::Empty { .. } => {
                panic!("expected Filtered, got {result:?}")
            }
        }
        // Verify `a` survived.
        let CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
            PointInsertOperationsInternal::PointsList(points),
        )) = &op
        else {
            panic!("op shape changed unexpectedly");
        };
        assert_eq!(points.len(), 1);
        let VectorStructPersisted::Named(map) = &points[0].vector else {
            panic!("expected Named");
        };
        assert!(map.contains_key("a"));
        assert!(!map.contains_key("b"));
    }

    #[test]
    fn point_with_only_unknown_vectors_is_dropped() {
        let params = make_params(&["a"]);
        let mut op = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
            PointInsertOperationsInternal::PointsList(vec![point_with_named(
                1,
                &[("b", vec![2.0; 4])],
            )]),
        ));
        let result = filter_replay_operation(&mut op, &params);
        match &result {
            FilteredReplayOp::Empty { dropped_names } => {
                assert!(dropped_names.contains("b"));
            }
            FilteredReplayOp::Unchanged | FilteredReplayOp::Filtered { .. } => {
                panic!("expected Empty, got {result:?}")
            }
        }
    }

    #[test]
    fn unchanged_when_all_names_known() {
        let params = make_params(&["a", "b"]);
        let mut op = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
            PointInsertOperationsInternal::PointsList(vec![point_with_named(
                1,
                &[("a", vec![1.0; 4]), ("b", vec![2.0; 4])],
            )]),
        ));
        let result = filter_replay_operation(&mut op, &params);
        assert!(matches!(result, FilteredReplayOp::Unchanged));
    }
}
