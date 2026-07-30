//! Read operations shared by all boolean index variants.
//!
//! The three concrete boolean index types ([`MutableBoolIndex`],
//! [`ImmutableBoolIndex`], [`ReadOnlyBoolIndex`]) all expose the same read
//! API on top of the same two-flag layout (`trues` + `falses`). The
//! [`BoolIndexRead`] trait captures that layout via four accessors
//! (`trues_flags`, `falses_flags`, `indexed_count`, `telemetry_index_type`);
//! every other read method is a default impl computed from those.
//!
//! Big query methods (filter / cardinality / payload blocks / condition
//! checker) live as free functions over `&impl BoolIndexRead` rather than
//! trait methods to avoid name clashes with [`PayloadFieldIndexRead`].
//!
//! [`MutableBoolIndex`]: super::mutable_bool_index::MutableBoolIndex
//! [`ImmutableBoolIndex`]: super::immutable_bool_index::ImmutableBoolIndex
//! [`ReadOnlyBoolIndex`]: super::read_only_bool_index::ReadOnlyBoolIndex
//! [`PayloadFieldIndexRead`]: crate::index::field_index::PayloadFieldIndexRead

use std::path::PathBuf;

use common::condition_checker::{CheckItem, ConditionChecker, Rest, Select, default_check_batched};
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::counter::iterator_hw_measurement::HwMeasurementIteratorExt;
use common::types::PointOffsetType;
use serde_json::Value;

use crate::common::flags::roaring_flags::RoaringFlagsRead;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::utils::MultiValue;
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition, PrimaryCondition};
use crate::index::payload_config::StorageType;
use crate::index::query_optimization::rescore_formula::value_retriever::VariableRetrieverFn;
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{
    AnyVariants, FieldCondition, Match, MatchAny, MatchExcept, MatchValue, PayloadKeyType,
    ValueVariants,
};

/// Shared read-only surface over the two-flag boolean index layout.
///
/// Implementers expose four required pieces (`trues_flags`, `falses_flags`,
/// `indexed_count`, `telemetry_index_type`); every other read / lifecycle /
/// telemetry method is a default impl derived from those.
pub trait BoolIndexRead {
    type Flags: RoaringFlagsRead;

    fn trues_flags(&self) -> &Self::Flags;
    fn falses_flags(&self) -> &Self::Flags;

    /// Number of distinct points with at least one indexed bool value
    /// (i.e. `|trues ∪ falses|`).
    ///
    /// Fallible: the read-only variant derives it from bitmaps it materializes
    /// on demand. See [`RoaringFlagsRead::get_bitmap`].
    fn indexed_count(&self) -> OperationResult<usize>;

    /// Per-variant telemetry tag (e.g. `"mmap_bool"`).
    fn telemetry_index_type(&self) -> &'static str;

    fn trues_count(&self) -> OperationResult<usize>;

    fn falses_count(&self) -> OperationResult<usize>;

    fn values_count(&self, point_id: PointOffsetType) -> OperationResult<usize> {
        let has_true = self.trues_flags().get(point_id)?;
        let has_false = self.falses_flags().get(point_id)?;
        Ok(usize::from(has_true) + usize::from(has_false))
    }

    fn check_values_any(&self, point_id: PointOffsetType, is_true: bool) -> OperationResult<bool> {
        if is_true {
            self.trues_flags().get(point_id)
        } else {
            self.falses_flags().get(point_id)
        }
    }

    fn values_is_empty(&self, point_id: PointOffsetType) -> OperationResult<bool> {
        Ok(!self.trues_flags().get(point_id)? && !self.falses_flags().get(point_id)?)
    }

    fn get_point_values(&self, point_id: PointOffsetType) -> OperationResult<Vec<bool>> {
        Ok([
            self.trues_flags().get(point_id)?.then_some(true),
            self.falses_flags().get(point_id)?.then_some(false),
        ]
        .into_iter()
        .flatten()
        .collect())
    }

    fn iter_values(&self) -> OperationResult<Box<dyn Iterator<Item = bool> + '_>> {
        let has_false = self.falses_flags().iter_trues()?.next().map(|_| false);
        let has_true = self.trues_flags().iter_trues()?.next().map(|_| true);
        Ok(Box::new([has_false, has_true].into_iter().flatten()))
    }

    fn for_each_value_map<F>(
        &self,
        hw_counter: &HardwareCounterCell,
        mut f: F,
    ) -> OperationResult<()>
    where
        F: FnMut(bool, &mut dyn Iterator<Item = PointOffsetType>) -> OperationResult<()>,
    {
        f(false, &mut self.falses_flags().iter_trues()?)?;
        hw_counter
            .payload_index_io_read_counter()
            .incr_delta(u8::BITS as usize);
        f(true, &mut self.trues_flags().iter_trues()?)?;
        hw_counter
            .payload_index_io_read_counter()
            .incr_delta(u8::BITS as usize);
        Ok(())
    }

    /// Like [`Self::for_each_value_map`] but visits only the requested `values`.
    /// Trivial here (≤2 values); exists only to back `FacetIndex::for_values_map`.
    fn for_values_map<F>(
        &self,
        values: impl Iterator<Item = bool>,
        hw_counter: &HardwareCounterCell,
        mut f: F,
    ) -> OperationResult<()>
    where
        F: FnMut(bool, &mut dyn Iterator<Item = PointOffsetType>) -> OperationResult<()>,
    {
        for is_true in values {
            let mut ids = if is_true {
                self.trues_flags().iter_trues()?
            } else {
                self.falses_flags().iter_trues()?
            };
            hw_counter
                .payload_index_io_read_counter()
                .incr_delta(u8::BITS as usize);
            f(is_true, &mut ids)?;
        }
        Ok(())
    }

    fn for_each_count_per_value<F>(
        &self,
        deferred_internal_id: Option<PointOffsetType>,
        mut f: F,
    ) -> OperationResult<()>
    where
        F: FnMut(bool, usize) -> OperationResult<()>,
    {
        let (false_count, true_count) = match deferred_internal_id {
            Some(deferred_internal_id) => {
                let false_count =
                    self.falses_flags()
                        .get_bitmap()?
                        .range_cardinality(..deferred_internal_id) as usize;
                let true_count =
                    self.trues_flags()
                        .get_bitmap()?
                        .range_cardinality(..deferred_internal_id) as usize;
                (false_count, true_count)
            }
            None => (self.falses_count()?, self.trues_count()?),
        };
        f(false, false_count)?;
        f(true, true_count)
    }

    /// Zero while neither bitmap is materialized — nothing is held in RAM yet.
    fn ram_usage_bytes(&self) -> usize {
        self.trues_flags().ram_usage_bytes() + self.falses_flags().ram_usage_bytes()
    }

    /// Whether the index keeps its primary data on disk. Default `false` —
    /// every current variant serves reads from an in-RAM bitmap (the read-only
    /// one materializes it on first use).
    fn is_on_disk(&self) -> bool {
        false
    }

    fn populate(&self) -> OperationResult<()> {
        self.trues_flags().populate()?;
        self.falses_flags().populate()
    }

    fn clear_cache(&self) -> OperationResult<()> {
        self.trues_flags().clear_cache()?;
        self.falses_flags().clear_cache()
    }

    fn files(&self) -> Vec<PathBuf> {
        let mut files = self.trues_flags().files();
        files.extend(self.falses_flags().files());
        files
    }

    fn get_storage_type(&self) -> StorageType {
        StorageType::Mmap {
            is_on_disk: self.is_on_disk(),
        }
    }

    /// Materializes both bitmaps for its counts, unlike the null index's
    /// length-driven telemetry.
    fn get_telemetry_data(&self) -> OperationResult<PayloadIndexTelemetry> {
        Ok(PayloadIndexTelemetry {
            field_name: None,
            points_count: self.indexed_count()?,
            points_values_count: self.trues_count()? + self.falses_count()?,
            histogram_bucket_size: None,
            index_type: self.telemetry_index_type(),
        })
    }
}

pub(super) fn filter<'a, N: BoolIndexRead>(
    idx: &'a N,
    condition: &'a FieldCondition,
    hw_counter: &'a HardwareCounterCell,
) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
    match &condition.r#match {
        Some(Match::Value(MatchValue {
            value: ValueVariants::Bool(value),
        })) => {
            let bitmap = if *value {
                idx.trues_flags().get_bitmap()?
            } else {
                idx.falses_flags().get_bitmap()?
            };
            let iter = bitmap
                .iter()
                .map(|x| x as PointOffsetType)
                .measure_hw_with_acc_and_fraction(
                    hw_counter.new_accumulator(),
                    u8::BITS as usize,
                    |i| i.payload_index_io_read_counter(),
                );
            Ok(Some(Box::new(iter)))
        }
        _ => Ok(None),
    }
}

pub(super) fn estimate_cardinality<N: BoolIndexRead>(
    idx: &N,
    condition: &FieldCondition,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<Option<CardinalityEstimation>> {
    match &condition.r#match {
        Some(Match::Value(MatchValue {
            value: ValueVariants::Bool(value),
        })) => {
            let count = if *value {
                idx.trues_count()?
            } else {
                idx.falses_count()?
            };

            hw_counter
                .payload_index_io_read_counter()
                .incr_delta(size_of::<usize>());

            Ok(Some(
                CardinalityEstimation::exact(count)
                    .with_primary_clause(PrimaryCondition::Condition(Box::new(condition.clone()))),
            ))
        }
        _ => Ok(None),
    }
}

pub(super) fn for_each_payload_block<N: BoolIndexRead>(
    idx: &N,
    threshold: usize,
    key: PayloadKeyType,
    f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
) -> OperationResult<()> {
    let mut handle_block = |cardinality, value: bool, key: PayloadKeyType| {
        if cardinality > threshold {
            f(PayloadBlockCondition {
                condition: FieldCondition::new_match(key.clone(), Match::from(value)),
                cardinality,
            })?;
        }
        Ok(())
    };

    // just two possible blocks: true and false
    handle_block(idx.trues_count()?, true, key.clone())?;
    handle_block(idx.falses_count()?, false, key)
}

pub(super) fn condition_checker<'a, N: BoolIndexRead>(
    idx: &'a N,
    condition: &FieldCondition,
    _hw_acc: HwMeasurementAcc,
) -> Option<BoolConditionChecker<'a, N>> {
    // Destructure explicitly (no `..`) so a new field added to
    // `FieldCondition` forces this method to be revisited.
    let FieldCondition {
        key: _,
        r#match,
        range: _,
        geo_radius: _,
        geo_bounding_box: _,
        geo_polygon: _,
        values_count: _,
        is_empty: _,
        is_null: _,
    } = condition;

    let cond_match = r#match.as_ref()?;
    match cond_match {
        Match::Value(MatchValue {
            value: ValueVariants::Bool(is_true),
        }) => Some(BoolConditionChecker {
            idx,
            is_true: *is_true,
        }),
        Match::Value(MatchValue {
            value: ValueVariants::String(_) | ValueVariants::Integer(_),
        })
        | Match::Any(MatchAny {
            any: AnyVariants::Strings(_) | AnyVariants::Integers(_),
        })
        | Match::Except(MatchExcept {
            except: AnyVariants::Strings(_) | AnyVariants::Integers(_),
        })
        | Match::Text(_)
        | Match::TextAny(_)
        | Match::Phrase(_)
        | Match::Prefix(_) => None,
    }
}

pub struct BoolConditionChecker<'a, N> {
    idx: &'a N,
    is_true: bool,
}

impl<N: BoolIndexRead> ConditionChecker for BoolConditionChecker<'_, N> {
    type Error = OperationError;

    fn check(&self, point_id: PointOffsetType) -> OperationResult<bool> {
        self.idx.check_values_any(point_id, self.is_true)
    }

    fn check_batched<K>(&self, ids: &mut [K], select: Select, rest: Rest) -> OperationResult<usize>
    where
        K: CheckItem,
    {
        default_check_batched(ids, select, rest, |id| self.check(id))
    }
}

/// Produce a closure that maps a point id to its indexed bool values as JSON
/// `Value`s. Shared by `BoolIndex::value_retriever` and
/// `ReadOnlyBoolIndex::value_retriever`; both expose it via inherent methods
/// so the per-variant dispatch in [`FieldIndex::value_retriever`] /
/// [`ReadOnlyFieldIndex::value_retriever`] can call it without going through
/// a trait object.
///
/// [`FieldIndex::value_retriever`]: crate::index::field_index::FieldIndex::value_retriever
/// [`ReadOnlyFieldIndex::value_retriever`]: crate::index::field_index::field_index_base::read_only::ReadOnlyFieldIndex
pub(super) fn value_retriever<'a, N: BoolIndexRead + ?Sized + 'a>(
    idx: &'a N,
    _hw_counter: &'a HardwareCounterCell,
) -> OperationResult<VariableRetrieverFn<'a>> {
    // Materialize both bitmaps here rather than inside the closure: the
    // retriever is invoked per point and must not fail, so the one scan that
    // could fail is hoisted to construction time.
    let trues = idx.trues_flags().get_bitmap()?;
    let falses = idx.falses_flags().get_bitmap()?;

    Ok(Box::new(
        move |point_id: PointOffsetType| -> MultiValue<Value> {
            [
                trues.contains(point_id).then_some(true),
                falses.contains(point_id).then_some(false),
            ]
            .into_iter()
            .flatten()
            .map(Value::Bool)
            .collect()
        },
    ))
}
