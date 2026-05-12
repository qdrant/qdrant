use std::cmp::max;
use std::path::PathBuf;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use serde_json::Value;

use super::{GEO_QUERY_MAX_REGION, GeoMapIndex};
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::utils::MultiValue;
use crate::index::field_index::geo_hash::{
    circle_hashes, geo_hash_to_box, polygon_hashes, polygon_hashes_estimation, rectangle_hashes,
};
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndex, PayloadFieldIndexRead,
    PrimaryCondition, ValueIndexer,
};
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::index::query_optimization::rescore_formula::value_retriever::VariableRetrieverFn;
use crate::types::{FieldCondition, GeoPoint, PayloadKeyType};

impl ValueIndexer for GeoMapIndex {
    type ValueType = GeoPoint;

    fn add_many(
        &mut self,
        id: PointOffsetType,
        values: Vec<GeoPoint>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            GeoMapIndex::Mutable(index) => index.add_many_geo_points(id, &values, hw_counter),
            GeoMapIndex::Immutable(_) => Err(OperationError::service_error(
                "Can't add values to immutable geo index",
            )),
            GeoMapIndex::Storage(_) => Err(OperationError::service_error(
                "Can't add values to mmap geo index",
            )),
        }
    }

    fn get_value(value: &Value) -> Option<GeoPoint> {
        match value {
            Value::Object(obj) => {
                let lon_op = obj.get("lon").and_then(|x| x.as_f64());
                let lat_op = obj.get("lat").and_then(|x| x.as_f64());

                if let (Some(lon), Some(lat)) = (lon_op, lat_op) {
                    return GeoPoint::new(lon, lat).ok();
                }
                None
            }
            _ => None,
        }
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        match self {
            GeoMapIndex::Mutable(index) => index.remove_point(id),
            GeoMapIndex::Immutable(index) => index.remove_point(id),
            GeoMapIndex::Storage(index) => {
                index.remove_point(id);
                Ok(())
            }
        }
    }
}

impl GeoMapIndex {
    pub fn value_retriever<'a>(
        &'a self,
        _hw_counter: &'a HardwareCounterCell,
    ) -> VariableRetrieverFn<'a> {
        Box::new(move |point_id: PointOffsetType| -> MultiValue<Value> {
            self.get_values(point_id)
                .into_iter()
                .flatten()
                .filter_map(|v| serde_json::to_value(v).ok())
                .collect()
        })
    }
}

impl PayloadFieldIndex for GeoMapIndex {
    fn wipe(self) -> OperationResult<()> {
        match self {
            GeoMapIndex::Mutable(index) => index.wipe(),
            GeoMapIndex::Immutable(index) => index.wipe(),
            GeoMapIndex::Storage(index) => index.wipe(),
        }
    }

    fn flusher(&self) -> Flusher {
        match self {
            GeoMapIndex::Mutable(index) => index.flusher(),
            GeoMapIndex::Immutable(index) => index.flusher(),
            GeoMapIndex::Storage(index) => index.flusher(),
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        match &self {
            GeoMapIndex::Mutable(index) => index.files(),
            GeoMapIndex::Immutable(index) => index.files(),
            GeoMapIndex::Storage(index) => index.files(),
        }
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        match &self {
            GeoMapIndex::Mutable(_) => vec![],
            GeoMapIndex::Immutable(index) => index.immutable_files(),
            GeoMapIndex::Storage(index) => index.immutable_files(),
        }
    }
}

impl PayloadFieldIndexRead for GeoMapIndex {
    fn count_indexed_points(&self) -> usize {
        self.points_count()
    }

    fn filter<'a>(
        &'a self,
        condition: &FieldCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>>> {
        if let Some(geo_bounding_box) = &condition.geo_bounding_box {
            let geo_hashes = rectangle_hashes(geo_bounding_box, GEO_QUERY_MAX_REGION)?;
            let geo_condition_copy = *geo_bounding_box;
            return Ok(Some(Box::new(self.iterator(geo_hashes)?.filter(
                move |&point| {
                    self.check_values_any(point, hw_counter, |geo_point| {
                        geo_condition_copy.check_point(geo_point)
                    })
                },
            ))));
        }

        if let Some(geo_radius) = &condition.geo_radius {
            let geo_hashes = circle_hashes(geo_radius, GEO_QUERY_MAX_REGION)?;
            let geo_condition_copy = *geo_radius;
            return Ok(Some(Box::new(self.iterator(geo_hashes)?.filter(
                move |&point| {
                    self.check_values_any(point, hw_counter, |geo_point| {
                        geo_condition_copy.check_point(geo_point)
                    })
                },
            ))));
        }

        if let Some(geo_polygon) = &condition.geo_polygon {
            let geo_hashes = polygon_hashes(geo_polygon, GEO_QUERY_MAX_REGION)?;
            let geo_condition_copy = geo_polygon.convert();
            return Ok(Some(Box::new(self.iterator(geo_hashes)?.filter(
                move |&point| {
                    self.check_values_any(point, hw_counter, |geo_point| {
                        geo_condition_copy.check_point(geo_point)
                    })
                },
            ))));
        }

        Ok(None)
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<CardinalityEstimation>> {
        if let Some(geo_bounding_box) = &condition.geo_bounding_box {
            let Some(geo_hashes) = rectangle_hashes(geo_bounding_box, GEO_QUERY_MAX_REGION).ok()
            else {
                return Ok(None);
            };
            let mut estimation = self.match_cardinality(&geo_hashes, hw_counter)?;
            estimation
                .primary_clauses
                .push(PrimaryCondition::Condition(Box::new(condition.clone())));
            return Ok(Some(estimation));
        }

        if let Some(geo_radius) = &condition.geo_radius {
            let Some(geo_hashes) = circle_hashes(geo_radius, GEO_QUERY_MAX_REGION).ok() else {
                return Ok(None);
            };
            let mut estimation = self.match_cardinality(&geo_hashes, hw_counter)?;
            estimation
                .primary_clauses
                .push(PrimaryCondition::Condition(Box::new(condition.clone())));
            return Ok(Some(estimation));
        }

        if let Some(geo_polygon) = &condition.geo_polygon {
            let (exterior_hashes, interior_hashes) =
                polygon_hashes_estimation(geo_polygon, GEO_QUERY_MAX_REGION);
            let mut exterior_estimation = self.match_cardinality(&exterior_hashes, hw_counter)?;

            for interior in &interior_hashes {
                let interior_estimation = self.match_cardinality(interior, hw_counter)?;
                exterior_estimation.min = max(0, exterior_estimation.min - interior_estimation.max);
                exterior_estimation.max = max(
                    exterior_estimation.min,
                    exterior_estimation.max - interior_estimation.min,
                );
                exterior_estimation.exp = max(
                    exterior_estimation.exp - interior_estimation.exp,
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

    fn for_each_payload_block(
        &self,
        threshold: usize,
        key: PayloadKeyType,
        f: &mut dyn FnMut(PayloadBlockCondition) -> OperationResult<()>,
    ) -> OperationResult<()> {
        self.large_hashes(threshold)?
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

    fn condition_checker<'a>(
        &'a self,
        condition: &FieldCondition,
        hw_acc: HwMeasurementAcc,
    ) -> Option<ConditionCheckerFn<'a>> {
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
                self.check_values_any(point_id, &hw_counter, |value| geo_radius.check_point(value))
            }));
        }
        if let Some(geo_bounding_box) = *geo_bounding_box {
            return Some(Box::new(move |point_id: PointOffsetType| {
                self.check_values_any(point_id, &hw_counter, |value| {
                    geo_bounding_box.check_point(value)
                })
            }));
        }
        if let Some(geo_polygon) = geo_polygon.as_ref() {
            let polygon_wrapper = geo_polygon.convert();
            return Some(Box::new(move |point_id: PointOffsetType| {
                self.check_values_any(point_id, &hw_counter, |value| {
                    polygon_wrapper.check_point(value)
                })
            }));
        }
        None
    }
}
