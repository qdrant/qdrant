#[cfg(test)]
mod tests;

mod mutable_numeric_index;

use std::cmp::{max, min};
use std::collections::BTreeMap;
use std::ops::Bound;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::Arc;

use common::types::PointOffsetType;
use mutable_numeric_index::MutableNumericIndex;
use parking_lot::RwLock;
use rocksdb::DB;
use serde_json::Value;

use super::utils::check_boundaries;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::common::Flusher;
use crate::index::field_index::histogram::{Histogram, Numericable, Point};
use crate::index::field_index::stat_tools::estimate_multi_value_selection_cardinality;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndex, PrimaryCondition, ValueIndexer,
};
use crate::index::key_encoding::{
    decode_f64_key_ascending, decode_i64_key_ascending, encode_f64_key_ascending,
    encode_i64_key_ascending,
};
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{FieldCondition, FloatPayloadType, IntPayloadType, PayloadKeyType, Range};

const HISTOGRAM_MAX_BUCKET_SIZE: usize = 10_000;
const HISTOGRAM_PRECISION: f64 = 0.01;

pub trait Encodable: Copy {
    fn encode_key(&self, id: PointOffsetType) -> Vec<u8>;
    fn decode_key(key: &[u8]) -> (PointOffsetType, Self);
}

impl Encodable for IntPayloadType {
    fn encode_key(&self, id: PointOffsetType) -> Vec<u8> {
        encode_i64_key_ascending(*self, id)
    }
    fn decode_key(key: &[u8]) -> (PointOffsetType, Self) {
        decode_i64_key_ascending(key)
    }
}

impl Encodable for FloatPayloadType {
    fn encode_key(&self, id: PointOffsetType) -> Vec<u8> {
        encode_f64_key_ascending(*self, id)
    }
    fn decode_key(key: &[u8]) -> (PointOffsetType, Self) {
        decode_f64_key_ascending(key)
    }
}

pub enum NumericIndex<T: Encodable + Numericable> {
    Mutable(MutableNumericIndex<T>),
}

impl<T: Encodable + Numericable> NumericIndex<T> {
    pub fn new(db: Arc<RwLock<DB>>, field: &str) -> Self {
        NumericIndex::Mutable(MutableNumericIndex::new(db, field))
    }

    fn get_db_wrapper(&self) -> &DatabaseColumnWrapper {
        match self {
            NumericIndex::Mutable(index) => index.get_db_wrapper(),
        }
    }

    fn get_histogram(&self) -> &Histogram<T> {
        match self {
            NumericIndex::Mutable(index) => &index.histogram,
        }
    }

    fn get_points_count(&self) -> usize {
        match self {
            NumericIndex::Mutable(index) => index.points_count,
        }
    }

    fn get_values_count(&self) -> usize {
        match self {
            NumericIndex::Mutable(index) => index.get_values_count(),
        }
    }

    fn storage_cf_name(field: &str) -> String {
        format!("{field}_numeric")
    }

    pub fn recreate(&self) -> OperationResult<()> {
        self.get_db_wrapper().recreate_column_family()
    }

    pub fn load(&mut self) -> OperationResult<bool> {
        match self {
            NumericIndex::Mutable(index) => index.load(),
        }
    }

    pub fn flusher(&self) -> Flusher {
        self.get_db_wrapper().flusher()
    }

    pub fn remove_point(&mut self, idx: PointOffsetType) -> OperationResult<()> {
        match self {
            NumericIndex::Mutable(index) => index.remove_point(idx),
        }
    }

    pub fn get_values(&self, idx: PointOffsetType) -> Option<&Vec<T>> {
        match self {
            NumericIndex::Mutable(index) => index.get_values(idx),
        }
    }

    pub fn get_max_values_per_point(&self) -> usize {
        match self {
            NumericIndex::Mutable(index) => index.max_values_per_point,
        }
    }

    fn range_cardinality(&self, range: &Range) -> CardinalityEstimation {
        let lbound = if let Some(lte) = range.lte {
            Included(T::from_f64(lte))
        } else if let Some(lt) = range.lt {
            Excluded(T::from_f64(lt))
        } else {
            Unbounded
        };

        let gbound = if let Some(gte) = range.gte {
            Included(T::from_f64(gte))
        } else if let Some(gt) = range.gt {
            Excluded(T::from_f64(gt))
        } else {
            Unbounded
        };

        let histogram_estimation = self.get_histogram().estimate(gbound, lbound);
        let min_estimation = histogram_estimation.0;
        let max_estimation = histogram_estimation.2;

        let total_values = self.get_values_count();
        // Example: points_count = 1000, total values = 2000, values_count = 500
        // min = max(1, 500 - (2000 - 1000)) = 1
        // exp = 500 / (2000 / 1000) = 250
        // max = min(1000, 500) = 500

        // Example: points_count = 1000, total values = 1200, values_count = 500
        // min = max(1, 500 - (1200 - 1000)) = 300
        // exp = 500 / (1200 / 1000) = 416
        // max = min(1000, 500) = 500
        let expected_min = max(
            min_estimation / self.get_max_values_per_point(),
            max(
                min(1, min_estimation),
                min_estimation.saturating_sub(total_values - self.get_points_count()),
            ),
        );
        let expected_max = min(self.get_points_count(), max_estimation);

        let estimation = estimate_multi_value_selection_cardinality(
            self.get_points_count(),
            total_values,
            histogram_estimation.1,
        )
        .round() as usize;

        CardinalityEstimation {
            primary_clauses: vec![],
            min: expected_min,
            exp: min(expected_max, max(estimation, expected_min)),
            max: expected_max,
        }
    }

    fn add_to_map(
        map: &mut BTreeMap<Vec<u8>, PointOffsetType>,
        histogram: &mut Histogram<T>,
        key: Vec<u8>,
        id: PointOffsetType,
    ) {
        let existed_value = map.insert(key.clone(), id);
        // Histogram works with unique values (idx + value) only, so we need to
        // make sure that we don't add the same value twice.
        // key is a combination of value + idx, so we can use it to ensure than the pair is unique
        if existed_value.is_none() {
            histogram.insert(
                NumericIndex::<T>::key_to_histogram_point(&key),
                |x| NumericIndex::<T>::get_histogram_left_neighbor(map, x),
                |x| NumericIndex::<T>::get_histogram_right_neighbor(map, x),
            );
        }
    }

    pub fn remove_from_map(
        map: &mut BTreeMap<Vec<u8>, PointOffsetType>,
        histogram: &mut Histogram<T>,
        key: Vec<u8>,
    ) {
        let existed_val = map.remove(&key);
        if existed_val.is_some() {
            histogram.remove(
                &Self::key_to_histogram_point(&key),
                |x| Self::get_histogram_left_neighbor(map, x),
                |x| Self::get_histogram_right_neighbor(map, x),
            );
        }
    }

    fn key_to_histogram_point(key: &[u8]) -> Point<T> {
        let (decoded_idx, decoded_val) = T::decode_key(key);
        Point {
            val: decoded_val,
            idx: decoded_idx as usize,
        }
    }

    fn get_histogram_left_neighbor(
        map: &BTreeMap<Vec<u8>, PointOffsetType>,
        point: &Point<T>,
    ) -> Option<Point<T>> {
        let key = point.val.encode_key(point.idx as PointOffsetType);
        map.range((Unbounded, Excluded(key)))
            .next_back()
            .map(|(key, _)| Self::key_to_histogram_point(key))
    }

    fn get_histogram_right_neighbor(
        map: &BTreeMap<Vec<u8>, PointOffsetType>,
        point: &Point<T>,
    ) -> Option<Point<T>> {
        let key = point.val.encode_key(point.idx as PointOffsetType);
        map.range((Excluded(key), Unbounded))
            .next()
            .map(|(key, _)| Self::key_to_histogram_point(key))
    }

    pub fn get_telemetry_data(&self) -> PayloadIndexTelemetry {
        PayloadIndexTelemetry {
            field_name: None,
            points_count: self.get_points_count(),
            points_values_count: self.get_histogram().get_total_count(),
            histogram_bucket_size: Some(self.get_histogram().current_bucket_size()),
        }
    }

    pub fn values_count(&self, point_id: PointOffsetType) -> usize {
        self.get_values(point_id).map(|x| x.len()).unwrap_or(0)
    }

    pub fn values_is_empty(&self, point_id: PointOffsetType) -> bool {
        self.get_values(point_id)
            .map(|x| x.is_empty())
            .unwrap_or(true)
    }
}

impl<T: Encodable + Numericable> PayloadFieldIndex for NumericIndex<T> {
    fn count_indexed_points(&self) -> usize {
        self.get_points_count()
    }

    fn load(&mut self) -> OperationResult<bool> {
        NumericIndex::load(self)
    }

    fn clear(self) -> OperationResult<()> {
        self.get_db_wrapper().recreate_column_family()
    }

    fn flusher(&self) -> Flusher {
        NumericIndex::flusher(self)
    }

    fn filter(
        &self,
        condition: &FieldCondition,
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + '_>> {
        let cond_range = condition
            .range
            .as_ref()
            .ok_or_else(|| OperationError::service_error("failed to get condition range"))?;

        let start_bound = match cond_range {
            Range { gt: Some(gt), .. } => {
                let v: T = T::from_f64(*gt);
                Excluded(v.encode_key(PointOffsetType::MAX))
            }
            Range { gte: Some(gte), .. } => {
                let v: T = T::from_f64(*gte);
                Included(v.encode_key(PointOffsetType::MIN))
            }
            _ => Unbounded,
        };

        let end_bound = match cond_range {
            Range { lt: Some(lt), .. } => {
                let v: T = T::from_f64(*lt);
                Excluded(v.encode_key(PointOffsetType::MIN))
            }
            Range { lte: Some(lte), .. } => {
                let v: T = T::from_f64(*lte);
                Included(v.encode_key(PointOffsetType::MAX))
            }
            _ => Unbounded,
        };

        // map.range
        // Panics if range start > end. Panics if range start == end and both bounds are Excluded.
        if !check_boundaries(&start_bound, &end_bound) {
            return Ok(Box::new(vec![].into_iter()));
        }

        Ok(match self {
            NumericIndex::Mutable(index) => Box::new(index.values_range(start_bound, end_bound)),
        })
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
    ) -> OperationResult<CardinalityEstimation> {
        condition
            .range
            .as_ref()
            .map(|range| {
                let mut cardinality = self.range_cardinality(range);
                cardinality
                    .primary_clauses
                    .push(PrimaryCondition::Condition(condition.clone()));
                cardinality
            })
            .ok_or_else(|| OperationError::service_error("failed to estimate cardinality"))
    }

    fn payload_blocks(
        &self,
        threshold: usize,
        key: PayloadKeyType,
    ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_> {
        let mut lower_bound = Unbounded;
        let mut pre_lower_bound: Option<Bound<T>> = None;
        let mut payload_conditions = Vec::new();

        let value_per_point = self.get_values_count() as f64 / self.get_points_count() as f64;
        let effective_threshold = (threshold as f64 * value_per_point) as usize;

        loop {
            let upper_bound = self
                .get_histogram()
                .get_range_by_size(lower_bound, effective_threshold / 2);

            if let Some(pre_lower_bound) = pre_lower_bound {
                let range = Range {
                    lt: match upper_bound {
                        Excluded(val) => Some(val.to_f64()),
                        _ => None,
                    },
                    gt: match pre_lower_bound {
                        Excluded(val) => Some(val.to_f64()),
                        _ => None,
                    },
                    gte: match pre_lower_bound {
                        Included(val) => Some(val.to_f64()),
                        _ => None,
                    },
                    lte: match upper_bound {
                        Included(val) => Some(val.to_f64()),
                        _ => None,
                    },
                };
                let cardinality = self.range_cardinality(&range);
                let condition = PayloadBlockCondition {
                    condition: FieldCondition::new_range(key.clone(), range),
                    cardinality: cardinality.exp,
                };

                payload_conditions.push(condition);
            } else if upper_bound == Unbounded {
                // One block covers all points
                payload_conditions.push(PayloadBlockCondition {
                    condition: FieldCondition::new_range(
                        key.clone(),
                        Range {
                            gte: None,
                            lte: None,
                            lt: None,
                            gt: None,
                        },
                    ),
                    cardinality: self.get_points_count(),
                });
            }

            pre_lower_bound = Some(lower_bound);

            lower_bound = match upper_bound {
                Included(val) => Excluded(val),
                Excluded(val) => Excluded(val),
                Unbounded => break,
            };
        }
        Box::new(payload_conditions.into_iter())
    }
}

impl ValueIndexer<IntPayloadType> for NumericIndex<IntPayloadType> {
    fn add_many(
        &mut self,
        id: PointOffsetType,
        values: Vec<IntPayloadType>,
    ) -> OperationResult<()> {
        match self {
            NumericIndex::Mutable(index) => index.add_many_to_list(id, values),
        }
    }

    fn get_value(&self, value: &Value) -> Option<IntPayloadType> {
        if let Value::Number(num) = value {
            return num.as_i64();
        }
        None
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        NumericIndex::remove_point(self, id)
    }
}

impl ValueIndexer<FloatPayloadType> for NumericIndex<FloatPayloadType> {
    fn add_many(
        &mut self,
        id: PointOffsetType,
        values: Vec<FloatPayloadType>,
    ) -> OperationResult<()> {
        match self {
            NumericIndex::Mutable(index) => index.add_many_to_list(id, values),
        }
    }

    fn get_value(&self, value: &Value) -> Option<FloatPayloadType> {
        if let Value::Number(num) = value {
            return num.as_f64();
        }
        None
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        NumericIndex::remove_point(self, id)
    }
}
