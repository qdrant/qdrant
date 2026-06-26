use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::MmapFile;
use itertools::Itertools;

use super::bool_index::{BoolIndex, ReadOnlyBoolIndex};
use super::map_index::MapIndex;
use super::map_index::read_only::ReadOnlyMapIndex;
use crate::common::operation_error::OperationResult;
use crate::data_types::facets::{FacetHit, FacetValue, FacetValueRef};
use crate::index::UniversalReadExt;
use crate::types::{IntPayloadType, UuidIntType};

pub trait FacetIndex {
    /// Number of distinct values currently indexed.
    ///
    /// Used by the facet read path to decide between the full-scan and the
    /// sampling strategy: if `unique_values_count` is comparable to the
    /// requested `limit`, scanning the whole index is cheaper than sampling.
    fn unique_values_count(&self) -> usize;

    /// Call a closure on value->point_ids mapping for specified `points`.
    fn for_points_values(
        &self,
        points: impl Iterator<Item = PointOffsetType>,
        hw_counter: &HardwareCounterCell,
        f: impl FnMut(PointOffsetType, &mut dyn Iterator<Item = FacetValueRef<'_>>),
    ) -> OperationResult<()>;

    /// Like [`Self::for_each_value_map`], but visits only the given `values`
    /// (cost ∝ requested values, not index cardinality). Values absent from the
    /// index or of a mismatched variant are skipped.
    fn for_values_map(
        &self,
        values: impl Iterator<Item = FacetValue>,
        hw_counter: &HardwareCounterCell,
        f: impl FnMut(FacetValue, &mut dyn Iterator<Item = PointOffsetType>) -> OperationResult<()>,
    ) -> OperationResult<()>;

    /// Call a closure on each value in the index.
    fn for_each_value(
        &self,
        f: impl FnMut(FacetValueRef<'_>) -> OperationResult<()>,
    ) -> OperationResult<()>;

    /// Call a closure on each value->point_ids mapping.
    fn for_each_value_map(
        &self,
        hw_acc: &HardwareCounterCell,
        f: impl FnMut(
            FacetValueRef<'_>,
            &mut dyn Iterator<Item = PointOffsetType>,
        ) -> OperationResult<()>,
    ) -> OperationResult<()>;

    /// Call a closure on each value->count mapping.
    fn for_each_count_per_value(
        &self,
        deferred_internal_id: Option<PointOffsetType>,
        f: impl FnMut(FacetHit<FacetValueRef<'_>>) -> OperationResult<()>,
    ) -> OperationResult<()>;

    /// Candidate-restricted analog of [`Self::for_each_count_per_value`]: counts
    /// only the given `values` (via [`Self::for_values_map`]). `deferred_internal_id`
    /// behaves identically — `Some(threshold)` counts only points `< threshold`.
    fn for_counts_per_value(
        &self,
        values: impl Iterator<Item = FacetValue>,
        deferred_internal_id: Option<PointOffsetType>,
        hw_counter: &HardwareCounterCell,
        mut f: impl FnMut(FacetHit<FacetValue>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        let max_id = deferred_internal_id.unwrap_or(PointOffsetType::MAX);
        self.for_values_map(values, hw_counter, |value, ids| {
            // Postings are sorted, so `take_while` matches `range_cardinality(..threshold)`.
            let count = ids.dedup().take_while(|&id| id < max_id).count();
            f(FacetHit { value, count })
        })
    }

    /// Like [`for_each_value`] but skips values whose only points are deferred.
    ///
    /// When `deferred_internal_id` is `None`, this is equivalent to
    /// [`for_each_value`]. When `Some(threshold)`, a value is reported only if
    /// it has at least one point with internal id `< threshold`.
    fn for_each_visible_value(
        &self,
        hw_counter: &HardwareCounterCell,
        deferred_internal_id: Option<PointOffsetType>,
        mut f: impl FnMut(FacetValueRef<'_>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        match deferred_internal_id {
            Some(deferred_internal_id) => {
                self.for_each_value_map(hw_counter, |facet_value, id_iter| {
                    let has_visible_point = id_iter
                        .take_while(|&id| id < deferred_internal_id)
                        .next()
                        .is_some();

                    if has_visible_point {
                        f(facet_value)?;
                    }
                    Ok(())
                })
            }
            None => self.for_each_value(f),
        }
    }
}

/// Borrowed view over any concrete index that can produce facet counts.
///
/// The `S: UniversalReadExt` parameter is consumed by the map-based `*ReadOnly`
/// variants (`ReadOnlyMapIndex<N, S>`) and threaded as a phantom by
/// `ReadOnlyBoolIndex<S>` (so it can re-read its flags through `S::Fs` on
/// live-reload); the in-memory variants (`Keyword`, `Int`, `Uuid`, `Bool`)
/// ignore it. The default `S = MmapFile` keeps the common construction path
/// (`FieldIndex::as_facet_index`) free of turbofish.
pub enum FacetIndexEnum<'a, S: UniversalReadExt = MmapFile> {
    Keyword(&'a MapIndex<str>),
    Int(&'a MapIndex<IntPayloadType>),
    Uuid(&'a MapIndex<UuidIntType>),
    Bool(&'a BoolIndex),
    // Constructed only by `ReadOnlyFieldIndex::as_facet_index`, which is
    // itself dead-code-allowed (`ReadOnlyFieldIndex` isn't wired into the
    // read path yet).
    #[allow(dead_code)]
    KeywordReadOnly(&'a ReadOnlyMapIndex<str, S>),
    #[allow(dead_code)]
    IntReadOnly(&'a ReadOnlyMapIndex<IntPayloadType, S>),
    #[allow(dead_code)]
    UuidReadOnly(&'a ReadOnlyMapIndex<UuidIntType, S>),
    #[allow(dead_code)]
    BoolReadOnly(&'a ReadOnlyBoolIndex<S>),
}

impl<'a, S: UniversalReadExt> FacetIndex for FacetIndexEnum<'a, S> {
    fn unique_values_count(&self) -> usize {
        match self {
            FacetIndexEnum::Keyword(index) => FacetIndex::unique_values_count(*index),
            FacetIndexEnum::Int(index) => FacetIndex::unique_values_count(*index),
            FacetIndexEnum::Uuid(index) => FacetIndex::unique_values_count(*index),
            FacetIndexEnum::Bool(index) => FacetIndex::unique_values_count(*index),
            FacetIndexEnum::KeywordReadOnly(index) => FacetIndex::unique_values_count(*index),
            FacetIndexEnum::IntReadOnly(index) => FacetIndex::unique_values_count(*index),
            FacetIndexEnum::UuidReadOnly(index) => FacetIndex::unique_values_count(*index),
            FacetIndexEnum::BoolReadOnly(index) => FacetIndex::unique_values_count(*index),
        }
    }

    fn for_points_values(
        &self,
        points: impl Iterator<Item = PointOffsetType>,
        hw_counter: &HardwareCounterCell,
        f: impl FnMut(PointOffsetType, &mut dyn Iterator<Item = FacetValueRef<'_>>),
    ) -> OperationResult<()> {
        match self {
            FacetIndexEnum::Keyword(index) => {
                FacetIndex::for_points_values(*index, points, hw_counter, f)
            }
            FacetIndexEnum::Int(index) => {
                FacetIndex::for_points_values(*index, points, hw_counter, f)
            }
            FacetIndexEnum::Uuid(index) => {
                FacetIndex::for_points_values(*index, points, hw_counter, f)
            }
            FacetIndexEnum::Bool(index) => {
                FacetIndex::for_points_values(*index, points, hw_counter, f)
            }
            FacetIndexEnum::KeywordReadOnly(index) => {
                FacetIndex::for_points_values(*index, points, hw_counter, f)
            }
            FacetIndexEnum::IntReadOnly(index) => {
                FacetIndex::for_points_values(*index, points, hw_counter, f)
            }
            FacetIndexEnum::UuidReadOnly(index) => {
                FacetIndex::for_points_values(*index, points, hw_counter, f)
            }
            FacetIndexEnum::BoolReadOnly(index) => {
                FacetIndex::for_points_values(*index, points, hw_counter, f)
            }
        }
    }

    fn for_each_value(
        &self,
        f: impl FnMut(FacetValueRef<'_>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        match self {
            FacetIndexEnum::Keyword(index) => FacetIndex::for_each_value(*index, f),
            FacetIndexEnum::Int(index) => FacetIndex::for_each_value(*index, f),
            FacetIndexEnum::Uuid(index) => FacetIndex::for_each_value(*index, f),
            FacetIndexEnum::Bool(index) => FacetIndex::for_each_value(*index, f),
            FacetIndexEnum::KeywordReadOnly(index) => FacetIndex::for_each_value(*index, f),
            FacetIndexEnum::IntReadOnly(index) => FacetIndex::for_each_value(*index, f),
            FacetIndexEnum::UuidReadOnly(index) => FacetIndex::for_each_value(*index, f),
            FacetIndexEnum::BoolReadOnly(index) => FacetIndex::for_each_value(*index, f),
        }
    }

    fn for_each_value_map(
        &self,
        hw_counter: &HardwareCounterCell,
        f: impl FnMut(
            FacetValueRef<'_>,
            &mut dyn Iterator<Item = PointOffsetType>,
        ) -> OperationResult<()>,
    ) -> OperationResult<()> {
        match self {
            FacetIndexEnum::Keyword(index) => FacetIndex::for_each_value_map(*index, hw_counter, f),
            FacetIndexEnum::Int(index) => FacetIndex::for_each_value_map(*index, hw_counter, f),
            FacetIndexEnum::Uuid(index) => FacetIndex::for_each_value_map(*index, hw_counter, f),
            FacetIndexEnum::Bool(index) => FacetIndex::for_each_value_map(*index, hw_counter, f),
            FacetIndexEnum::KeywordReadOnly(index) => {
                FacetIndex::for_each_value_map(*index, hw_counter, f)
            }
            FacetIndexEnum::IntReadOnly(index) => {
                FacetIndex::for_each_value_map(*index, hw_counter, f)
            }
            FacetIndexEnum::UuidReadOnly(index) => {
                FacetIndex::for_each_value_map(*index, hw_counter, f)
            }
            FacetIndexEnum::BoolReadOnly(index) => {
                FacetIndex::for_each_value_map(*index, hw_counter, f)
            }
        }
    }

    fn for_values_map(
        &self,
        values: impl Iterator<Item = FacetValue>,
        hw_counter: &HardwareCounterCell,
        f: impl FnMut(FacetValue, &mut dyn Iterator<Item = PointOffsetType>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        match self {
            FacetIndexEnum::Keyword(index) => {
                FacetIndex::for_values_map(*index, values, hw_counter, f)
            }
            FacetIndexEnum::Int(index) => FacetIndex::for_values_map(*index, values, hw_counter, f),
            FacetIndexEnum::Uuid(index) => {
                FacetIndex::for_values_map(*index, values, hw_counter, f)
            }
            FacetIndexEnum::Bool(index) => {
                FacetIndex::for_values_map(*index, values, hw_counter, f)
            }
            FacetIndexEnum::KeywordReadOnly(index) => {
                FacetIndex::for_values_map(*index, values, hw_counter, f)
            }
            FacetIndexEnum::IntReadOnly(index) => {
                FacetIndex::for_values_map(*index, values, hw_counter, f)
            }
            FacetIndexEnum::UuidReadOnly(index) => {
                FacetIndex::for_values_map(*index, values, hw_counter, f)
            }
            FacetIndexEnum::BoolReadOnly(index) => {
                FacetIndex::for_values_map(*index, values, hw_counter, f)
            }
        }
    }

    fn for_each_count_per_value(
        &self,
        deferred_internal_id: Option<PointOffsetType>,
        f: impl FnMut(FacetHit<FacetValueRef<'_>>) -> OperationResult<()>,
    ) -> OperationResult<()> {
        match self {
            FacetIndexEnum::Keyword(index) => {
                FacetIndex::for_each_count_per_value(*index, deferred_internal_id, f)
            }
            FacetIndexEnum::Int(index) => {
                FacetIndex::for_each_count_per_value(*index, deferred_internal_id, f)
            }
            FacetIndexEnum::Uuid(index) => {
                FacetIndex::for_each_count_per_value(*index, deferred_internal_id, f)
            }
            FacetIndexEnum::Bool(index) => {
                FacetIndex::for_each_count_per_value(*index, deferred_internal_id, f)
            }
            FacetIndexEnum::KeywordReadOnly(index) => {
                FacetIndex::for_each_count_per_value(*index, deferred_internal_id, f)
            }
            FacetIndexEnum::IntReadOnly(index) => {
                FacetIndex::for_each_count_per_value(*index, deferred_internal_id, f)
            }
            FacetIndexEnum::UuidReadOnly(index) => {
                FacetIndex::for_each_count_per_value(*index, deferred_internal_id, f)
            }
            FacetIndexEnum::BoolReadOnly(index) => {
                FacetIndex::for_each_count_per_value(*index, deferred_internal_id, f)
            }
        }
    }
}
