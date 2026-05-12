//! Read operations shared by all null index variants.
//!
//! The three concrete null index types ([`MutableNullIndex`],
//! [`ImmutableNullIndex`], [`ReadOnlyNullIndex`]) all expose the same read
//! API on top of the same two-flag layout (`has_values` + `is_null`). The
//! [`NullIndexRead`] trait captures that layout via three accessors
//! (`has_values_flags`, `is_null_flags`, `total_point_count`); every other
//! read method is a default impl computed from those three.
//!
//! Big query methods (filter / cardinality / condition checker) live as free
//! functions over `&impl NullIndexRead` rather than trait methods to avoid
//! name clashes with [`PayloadFieldIndexRead`].
//!
//! [`MutableNullIndex`]: super::mutable_null_index::MutableNullIndex
//! [`ImmutableNullIndex`]: super::immutable_null_index::ImmutableNullIndex
//! [`ReadOnlyNullIndex`]: super::read_only_null_index::ReadOnlyNullIndex
//! [`PayloadFieldIndexRead`]: crate::index::field_index::PayloadFieldIndexRead

use std::path::PathBuf;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::types::PointOffsetType;

use crate::common::flags::roaring_flags::RoaringFlagsRead;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::{CardinalityEstimation, PrimaryCondition};
use crate::index::payload_config::StorageType;
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::FieldCondition;

/// Shared read-only surface over the two-flag null index layout.
///
/// Implementers expose four required pieces (`has_values_flags`,
/// `is_null_flags`, `total_point_count`, `telemetry_index_type`); every other
/// read / lifecycle / telemetry method is a default impl derived from those.
pub trait NullIndexRead {
    type Flags: RoaringFlagsRead;

    fn has_values_flags(&self) -> &Self::Flags;
    fn is_null_flags(&self) -> &Self::Flags;
    fn total_point_count(&self) -> usize;

    /// Per-variant telemetry tag (e.g. `"mutable_null_index"`).
    fn telemetry_index_type(&self) -> &'static str;

    fn values_count(&self, id: PointOffsetType) -> usize {
        usize::from(self.has_values_flags().get(id))
    }

    fn values_is_empty(&self, id: PointOffsetType) -> bool {
        !self.has_values_flags().get(id)
    }

    fn values_is_null(&self, id: PointOffsetType) -> bool {
        self.is_null_flags().get(id)
    }

    fn indexed_points_count(&self) -> usize {
        self.has_values_flags().len()
    }

    fn ram_usage_bytes(&self) -> usize {
        self.has_values_flags().get_bitmap().serialized_size()
            + self.is_null_flags().get_bitmap().serialized_size()
    }

    /// Whether the index keeps its primary data on disk. Default `false` —
    /// the bitmap is in RAM after open for every current variant.
    fn is_on_disk(&self) -> bool {
        false
    }

    fn populate(&self) -> OperationResult<()> {
        self.has_values_flags().populate()?;
        self.is_null_flags().populate()
    }

    fn clear_cache(&self) -> OperationResult<()> {
        self.has_values_flags().clear_cache()?;
        self.is_null_flags().clear_cache()
    }

    fn files(&self) -> Vec<PathBuf> {
        let mut files = self.has_values_flags().files();
        files.extend(self.is_null_flags().files());
        files
    }

    fn get_storage_type(&self) -> StorageType {
        StorageType::Mmap {
            is_on_disk: self.is_on_disk(),
        }
    }

    fn get_telemetry_data(&self) -> PayloadIndexTelemetry {
        let points_count = self.indexed_points_count();
        PayloadIndexTelemetry {
            field_name: None,
            points_count,
            points_values_count: points_count,
            histogram_bucket_size: None,
            index_type: self.telemetry_index_type(),
        }
    }
}

pub(super) fn filter<'a, N: NullIndexRead>(
    null_index: &'a N,
    condition: &'a FieldCondition,
) -> Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
    let FieldCondition {
        key: _,
        r#match: _,
        range: _,
        geo_bounding_box: _,
        geo_radius: _,
        geo_polygon: _,
        values_count: _,
        is_empty,
        is_null,
    } = condition;

    let has_values_flags = null_index.has_values_flags();
    let is_null_flags = null_index.is_null_flags();
    let total_point_count = null_index.total_point_count();

    if let Some(is_empty) = is_empty {
        if *is_empty {
            // Return points that don't have values
            Some(Box::new(has_values_flags.iter_falses().chain({
                let end = has_values_flags.len() as PointOffsetType;
                end..total_point_count as u32
            })))
        } else {
            // Return points that have values
            Some(Box::new(has_values_flags.iter_trues()))
        }
    } else if let Some(is_null) = is_null {
        if *is_null {
            // Return points that have null values
            Some(Box::new(is_null_flags.iter_trues()))
        } else {
            // Return points that don't have null values
            Some(Box::new(is_null_flags.iter_falses().chain({
                let end = is_null_flags.len() as PointOffsetType;
                end..total_point_count as u32
            })))
        }
    } else {
        None
    }
}

pub(super) fn estimate_cardinality<N: NullIndexRead>(
    null_index: &N,
    condition: &FieldCondition,
) -> Option<CardinalityEstimation> {
    let FieldCondition {
        key,
        r#match: _,
        range: _,
        geo_bounding_box: _,
        geo_radius: _,
        geo_polygon: _,
        values_count: _,
        is_empty,
        is_null,
    } = condition;

    let has_values_flags = null_index.has_values_flags();
    let is_null_flags = null_index.is_null_flags();
    let total_point_count = null_index.total_point_count();

    if let Some(is_empty) = is_empty {
        if *is_empty {
            let has_values_count = has_values_flags.count_trues();
            let estimated = total_point_count.saturating_sub(has_values_count);

            Some(CardinalityEstimation {
                min: 0,
                exp: 2 * estimated / 3, // assuming 1/3 of the points are deleted
                max: estimated,
                primary_clauses: vec![PrimaryCondition::from(FieldCondition::new_is_empty(
                    key.clone(),
                    true,
                ))],
            })
        } else {
            let count = has_values_flags.count_trues();
            Some(
                CardinalityEstimation::exact(count).with_primary_clause(PrimaryCondition::from(
                    FieldCondition::new_is_empty(key.clone(), false),
                )),
            )
        }
    } else if let Some(is_null) = is_null {
        if *is_null {
            let count = is_null_flags.count_trues();
            Some(
                CardinalityEstimation::exact(count).with_primary_clause(PrimaryCondition::from(
                    FieldCondition::new_is_null(key.clone(), true),
                )),
            )
        } else {
            let is_null_count = is_null_flags.count_trues();
            let estimated = total_point_count.saturating_sub(is_null_count);

            Some(CardinalityEstimation {
                min: 0,
                exp: 2 * estimated / 3, // assuming 1/3 of the points are deleted
                max: estimated,
                primary_clauses: vec![PrimaryCondition::from(FieldCondition::new_is_null(
                    key.clone(),
                    false,
                ))],
            })
        }
    } else {
        None
    }
}

pub(super) fn condition_checker<'a, N: NullIndexRead>(
    null_index: &'a N,
    condition: &FieldCondition,
    _hw_acc: HwMeasurementAcc,
) -> Option<ConditionCheckerFn<'a>> {
    // Destructure explicitly (no `..`) so a new field added to
    // `FieldCondition` forces this method to be revisited.
    let FieldCondition {
        key: _,
        r#match: _,
        range: _,
        geo_radius: _,
        geo_bounding_box: _,
        geo_polygon: _,
        values_count: _,
        is_empty,
        is_null,
    } = condition;

    if let Some(is_empty) = *is_empty {
        return Some(Box::new(move |point_id: PointOffsetType| {
            null_index.values_is_empty(point_id) == is_empty
        }));
    }
    if let Some(is_null) = *is_null {
        return Some(Box::new(move |point_id: PointOffsetType| {
            null_index.values_is_null(point_id) == is_null
        }));
    }
    None
}
