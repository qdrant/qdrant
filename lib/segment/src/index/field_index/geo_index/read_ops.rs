//! Read operations shared by all geo index variants.
//!
//! The three concrete geo index types ([`MutableGeoIndex`],
//! [`ImmutableGeoIndex`], [`OnDiskGeoIndex`]) — and the upcoming
//! [`ReadOnlyGeoIndex`] — all expose the same read API on top of the same
//! geohash-bucket layout. The [`GeoIndexRead`] trait captures that layout
//! via per-variant accessors; query / cardinality / payload-block logic lives
//! as free functions over `&impl GeoIndexRead`, matching the
//! [`NullIndexRead`] / [`BoolIndexRead`] pattern.
//!
//! [`MutableGeoIndex`]: super::mutable_geo_index::MutableGeoIndex
//! [`ImmutableGeoIndex`]: super::immutable_geo_index::ImmutableGeoIndex
//! [`OnDiskGeoIndex`]: super::mmap_geo_index::OnDiskGeoIndex
//! [`ReadOnlyGeoIndex`]: super::read_only_geo_index::ReadOnlyGeoIndex
//! [`NullIndexRead`]: super::super::null_index::NullIndexRead
//! [`BoolIndexRead`]: super::super::bool_index::BoolIndexRead

use std::cmp::{max, min};
use std::path::PathBuf;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;

use super::GEO_QUERY_MAX_REGION;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::geo_hash::{
    GeoHash, circle_hashes, common_hash_prefix, geo_hash_to_box, polygon_hashes,
    polygon_hashes_estimation, rectangle_hashes,
};
use crate::index::field_index::stat_tools::estimate_multi_value_selection_cardinality;
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition, PrimaryCondition};
use crate::index::payload_config::StorageType;
use crate::index::query_optimization::optimized_filter::ConditionChecker;
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{FieldCondition, GeoPoint, PayloadKeyType};

/// Shared read-only surface over the geo index variants.
///
/// Implementers provide per-variant accessors (counts, hash lookups, value
/// retrieval, iteration over hash regions); every higher-level read operation
/// (`match_cardinality`, `large_hashes`, telemetry, populate / clear_cache /
/// files) is a default impl derived from those.
pub trait GeoIndexRead {
    fn points_count(&self) -> usize;

    fn points_values_count(&self) -> usize;

    /// Maximum number of values per point. Zero if the index is empty.
    fn max_values_per_point(&self) -> usize;

    fn points_of_hash(
        &self,
        hash: GeoHash,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<usize>;

    fn values_of_hash(
        &self,
        hash: GeoHash,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<usize>;

    fn check_values_any(
        &self,
        idx: PointOffsetType,
        hw_counter: &HardwareCounterCell,
        check_fn: &dyn Fn(&GeoPoint) -> bool,
    ) -> OperationResult<bool>;

    fn values_count(&self, idx: PointOffsetType) -> usize;

    fn get_values(&self, idx: PointOffsetType) -> Option<Box<dyn Iterator<Item = GeoPoint> + '_>>;

    /// Iterator over point IDs covered by any of the given top-level hash
    /// prefixes. Point IDs may repeat across prefixes; callers that need
    /// uniqueness deduplicate themselves.
    fn iterator(
        &self,
        values: Vec<GeoHash>,
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + '_>>;

    /// Materialise (geohash, points_count) pairs filtered by `filter`. The
    /// Storage variant pushes the filter down to its IO layer; the in-memory
    /// variants apply it post-collect.
    fn points_per_hash_filtered(
        &self,
        filter: &dyn Fn(&(GeoHash, usize)) -> bool,
    ) -> OperationResult<Vec<(GeoHash, usize)>>;

    fn get_storage_type(&self) -> StorageType;

    fn ram_usage_bytes(&self) -> usize;

    fn is_on_disk(&self) -> bool;

    fn populate(&self) -> OperationResult<()>;

    fn clear_cache(&self) -> OperationResult<()>;

    fn files(&self) -> Vec<PathBuf>;

    fn immutable_files(&self) -> Vec<PathBuf>;

    /// Per-variant telemetry tag (e.g. `"mutable_geo"`).
    fn telemetry_index_type(&self) -> &'static str;

    fn values_is_empty(&self, idx: PointOffsetType) -> bool {
        self.values_count(idx) == 0
    }

    /// Cardinality estimation for a set of geo-hash regions. Mirrors the
    /// previous inherent method on `GeoIndex`; depends only on the trait's
    /// required accessors so every variant gets the same estimation logic.
    fn match_cardinality(
        &self,
        values: &[GeoHash],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<CardinalityEstimation> {
        let max_values_per_point = self.max_values_per_point();
        if max_values_per_point == 0 {
            return Ok(CardinalityEstimation::exact(0));
        }

        let Some(common_hash) = common_hash_prefix(values) else {
            return Ok(CardinalityEstimation::exact(0));
        };

        let total_points = self.points_of_hash(common_hash, hw_counter)?;
        let total_values = self.values_of_hash(common_hash, hw_counter)?;

        let (sum, maximum_per_hash) = values
            .iter()
            .map(|&region| self.points_of_hash(region, hw_counter))
            .try_fold((0, 0), |(sum, maximum), count| {
                let count = count?;
                OperationResult::Ok((sum + count, max(maximum, count)))
            })?;

        // Assume all selected points have `max_values_per_point` value hits.
        // Therefore number of points can't be less than `total_hits / max_values_per_point`
        // Note: max_values_per_point is never zero here because we check it above
        let min_hits_by_value_groups = sum / max_values_per_point;

        // Assume that we have selected all possible duplications of the points
        let point_duplications = total_values - total_points;
        let possible_non_duplicated = sum.saturating_sub(point_duplications);

        let estimation_min = max(
            max(min_hits_by_value_groups, possible_non_duplicated),
            maximum_per_hash,
        );
        let estimation_max = min(sum, total_points);

        let estimation_exp =
            estimate_multi_value_selection_cardinality(total_points, total_values, sum).round()
                as usize;

        Ok(CardinalityEstimation {
            primary_clauses: vec![],
            min: estimation_min,
            exp: min(estimation_max, max(estimation_min, estimation_exp)),
            max: estimation_max,
        })
    }

    /// Get iterator over smallest geo-hash regions larger than `threshold` points.
    fn large_hashes(
        &self,
        threshold: usize,
    ) -> OperationResult<Box<dyn Iterator<Item = (GeoHash, usize)>>> {
        let filter = |pair: &(GeoHash, usize)| pair.1 > threshold && !pair.0.is_empty();
        let mut large_regions = self.points_per_hash_filtered(&filter)?;

        // smallest regions first
        large_regions.sort_by(|a, b| b.cmp(a));

        let mut edge_region = vec![];
        let mut current_region = GeoHash::default();

        for (region, size) in large_regions {
            if !current_region.starts_with(region) {
                current_region = region;
                edge_region.push((region, size));
            }
        }

        Ok(Box::new(edge_region.into_iter()))
    }

    fn get_telemetry_data(&self) -> PayloadIndexTelemetry {
        PayloadIndexTelemetry {
            field_name: None,
            points_count: self.points_count(),
            points_values_count: self.points_values_count(),
            histogram_bucket_size: None,
            index_type: self.telemetry_index_type(),
        }
    }
}

pub(super) fn filter<'a, G: GeoIndexRead + ?Sized>(
    geo: &'a G,
    condition: &FieldCondition,
    hw_counter: &'a HardwareCounterCell,
) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
    if let Some(geo_bounding_box) = &condition.geo_bounding_box {
        let geo_hashes = rectangle_hashes(geo_bounding_box, GEO_QUERY_MAX_REGION)?;
        let geo_condition_copy = *geo_bounding_box;
        return Ok(Some(Box::new(geo.iterator(geo_hashes)?.filter(
            move |&point| {
                geo.check_values_any(point, hw_counter, &|geo_point| {
                    geo_condition_copy.check_point(geo_point)
                })
                .unwrap_or(false) // TODO(uio): handle errors
            },
        ))));
    }

    if let Some(geo_radius) = &condition.geo_radius {
        let geo_hashes = circle_hashes(geo_radius, GEO_QUERY_MAX_REGION)?;
        let geo_condition_copy = *geo_radius;
        return Ok(Some(Box::new(geo.iterator(geo_hashes)?.filter(
            move |&point| {
                geo.check_values_any(point, hw_counter, &|geo_point| {
                    geo_condition_copy.check_point(geo_point)
                })
                .unwrap_or(false) // TODO(uio): handle errors
            },
        ))));
    }

    if let Some(geo_polygon) = &condition.geo_polygon {
        let geo_hashes = polygon_hashes(geo_polygon, GEO_QUERY_MAX_REGION)?;
        let geo_condition_copy = geo_polygon.convert();
        return Ok(Some(Box::new(geo.iterator(geo_hashes)?.filter(
            move |&point| {
                geo.check_values_any(point, hw_counter, &|geo_point| {
                    geo_condition_copy.check_point(geo_point)
                })
                .unwrap_or(false) // TODO(uio): handle errors
            },
        ))));
    }

    Ok(None)
}

pub(super) fn estimate_cardinality<G: GeoIndexRead + ?Sized>(
    geo: &G,
    condition: &FieldCondition,
    hw_counter: &HardwareCounterCell,
) -> OperationResult<Option<CardinalityEstimation>> {
    if let Some(geo_bounding_box) = &condition.geo_bounding_box {
        let Some(geo_hashes) = rectangle_hashes(geo_bounding_box, GEO_QUERY_MAX_REGION).ok() else {
            return Ok(None);
        };
        let mut estimation = geo.match_cardinality(&geo_hashes, hw_counter)?;
        estimation
            .primary_clauses
            .push(PrimaryCondition::Condition(Box::new(condition.clone())));
        return Ok(Some(estimation));
    }

    if let Some(geo_radius) = &condition.geo_radius {
        let Some(geo_hashes) = circle_hashes(geo_radius, GEO_QUERY_MAX_REGION).ok() else {
            return Ok(None);
        };
        let mut estimation = geo.match_cardinality(&geo_hashes, hw_counter)?;
        estimation
            .primary_clauses
            .push(PrimaryCondition::Condition(Box::new(condition.clone())));
        return Ok(Some(estimation));
    }

    if let Some(geo_polygon) = &condition.geo_polygon {
        let (exterior_hashes, interior_hashes) =
            polygon_hashes_estimation(geo_polygon, GEO_QUERY_MAX_REGION);
        let mut exterior_estimation = geo.match_cardinality(&exterior_hashes, hw_counter)?;

        for interior in &interior_hashes {
            let interior_estimation = geo.match_cardinality(interior, hw_counter)?;
            exterior_estimation.min = exterior_estimation
                .min
                .saturating_sub(interior_estimation.max);
            exterior_estimation.max = max(
                exterior_estimation.min,
                exterior_estimation
                    .max
                    .saturating_sub(interior_estimation.min),
            );
            exterior_estimation.exp = max(
                exterior_estimation
                    .exp
                    .saturating_sub(interior_estimation.exp),
                exterior_estimation.min,
            );
        }

        exterior_estimation
            .primary_clauses
            .push(PrimaryCondition::Condition(Box::new(condition.clone())));
        return Ok(Some(exterior_estimation));
    }

    Ok(None)
}

pub(super) fn for_each_payload_block<G: GeoIndexRead + ?Sized>(
    geo: &G,
    threshold: usize,
    key: PayloadKeyType,
    f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
) -> OperationResult<()> {
    geo.large_hashes(threshold)?
        .try_for_each(|(geo_hash, size)| {
            f(PayloadBlockCondition {
                condition: FieldCondition::new_geo_bounding_box(
                    key.clone(),
                    geo_hash_to_box(geo_hash),
                ),
                cardinality: size,
            })
        })
}

pub(super) fn condition_checker<'a, G: GeoIndexRead + ?Sized>(
    geo: &'a G,
    condition: &FieldCondition,
    hw_acc: HwMeasurementAcc,
) -> Option<Box<dyn ConditionChecker + 'a>> {
    // Destructure explicitly (no `..`) so a new field added to
    // `FieldCondition` forces this method to be revisited.
    let FieldCondition {
        key: _,
        r#match: _,
        range: _,
        geo_radius,
        geo_bounding_box,
        geo_polygon,
        values_count: _,
        is_empty: _,
        is_null: _,
    } = condition;

    let hw_counter = hw_acc.get_counter_cell();

    if let Some(geo_radius) = *geo_radius {
        return Some(Box::new(move |point_id: PointOffsetType| {
            geo.check_values_any(point_id, &hw_counter, &|value| {
                geo_radius.check_point(value)
            })
        }));
    }
    if let Some(geo_bounding_box) = *geo_bounding_box {
        return Some(Box::new(move |point_id: PointOffsetType| {
            geo.check_values_any(point_id, &hw_counter, &|value| {
                geo_bounding_box.check_point(value)
            })
        }));
    }
    if let Some(geo_polygon) = geo_polygon.as_ref() {
        let polygon_wrapper = geo_polygon.convert();
        return Some(Box::new(move |point_id: PointOffsetType| {
            geo.check_values_any(point_id, &hw_counter, &|value| {
                polygon_wrapper.check_point(value)
            })
        }));
    }
    None
}
