//! A whole filter read into a [`PointSet`], e.g. to be shared by the searches of a batch.

use std::sync::atomic::AtomicBool;

use common::iterator_ext::IteratorExt;
use common::types::{DeferredBehavior, PointOffsetType};

use super::StructPayloadIndexReadView;
use crate::common::check_stopped;
use crate::common::operation_error::OperationResult;
use crate::id_tracker::IdTrackerRead;
use crate::index::PayloadIndexRead;
use crate::index::field_index::{CardinalityEstimation, FieldIndexRead};
use crate::index::query_optimization::point_set::{PointSet, PointSetContext};
use crate::payload_storage::PayloadStorageRead;
use crate::types::Filter;
use crate::vector_storage::VectorStorageRead;

/// Whether the primary clauses of `cardinality` yield exactly the points matching `filter`,
/// so that no point needs checking against it.
///
/// Primary clauses only come from positive conditions, so they never cover a `must_not`
/// condition, even one equal to a primary clause. E.g. a proxy segment reads `HasId` of its
/// deleted points as `must: HasId(ids), must_not: HasId(ids)`.
pub(super) fn primary_clauses_cover(filter: &Filter, cardinality: &CardinalityEstimation) -> bool {
    !cardinality.primary_clauses.is_empty()
        && filter
            .must_not
            .as_ref()
            .is_none_or(|conditions| conditions.is_empty())
        && filter
            .iter_conditions()
            .all(|condition| cardinality.is_primary(condition))
}

impl<P, I, V, F> StructPayloadIndexReadView<'_, P, I, V, F>
where
    P: PayloadStorageRead,
    I: IdTrackerRead,
    V: VectorStorageRead,
    F: FieldIndexRead,
{
    /// See [`PayloadIndexRead::materialize_filter`].
    pub(super) fn materialize_filter_points(
        &self,
        filter: &Filter,
        query_cardinality: &CardinalityEstimation,
        is_stopped: &AtomicBool,
        deferred_behavior: DeferredBehavior,
        expected_checks: usize,
    ) -> OperationResult<PointSet> {
        let context = PointSetContext {
            total_points: self.id_tracker.total_point_count(),
            expected_checks,
        };
        let expected_len = query_cardinality.exp;

        let points = if primary_clauses_cover(filter, query_cardinality)
            && let Some(primary) = self.primary_points(query_cardinality)?
        {
            // The posting lists of the primary clauses are the result: write them straight
            // into the set, which drops repeated points, without a visited list
            let point_mappings = self.id_tracker.point_mappings();
            let points = point_mappings
                .filter_deferred_and_deleted(primary.into_iter().flatten(), deferred_behavior)
                .stop_if(is_stopped);
            PointSet::from_points(points, expected_len, context)
        } else {
            let points = self.iter_filtered_points(
                filter,
                query_cardinality,
                is_stopped,
                deferred_behavior,
            )?;
            PointSet::from_points(points, expected_len, context)
        };

        // A stopped iteration ends early: never hand out a partial set
        check_stopped(is_stopped)?;
        Ok(points)
    }

    /// An iterator per primary clause, or `None` if one of them has no index to read from.
    fn primary_points<'q>(
        &'q self,
        cardinality: &'q CardinalityEstimation,
    ) -> OperationResult<Option<Vec<Box<dyn Iterator<Item = PointOffsetType> + 'q>>>> {
        cardinality
            .primary_clauses
            .iter()
            .map(|clause| self.query_field(clause))
            .collect()
    }
}
