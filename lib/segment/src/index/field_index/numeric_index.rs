use std::cmp::{max, min};
use std::collections::BTreeMap;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::Arc;

use parking_lot::RwLock;
use rocksdb::DB;
use serde_json::Value;

use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::common::Flusher;
use crate::entry::entry_point::{OperationError, OperationResult};
use crate::index::field_index::histogram::{Histogram, Point};
use crate::index::field_index::stat_tools::estimate_multi_value_selection_cardinality;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndex, PrimaryCondition, ValueIndexer,
};
use crate::index::key_encoding::{
    decode_f64_key_ascending, decode_i64_key_ascending, encode_f64_key_ascending,
    encode_i64_key_ascending,
};
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{
    FieldCondition, FloatPayloadType, IntPayloadType, PayloadKeyType, PointOffsetType, Range,
};

const HISTOGRAM_MAX_BUCKET_SIZE: usize = 10_000;
const HISTOGRAM_PRECISION: f64 = 0.01;

pub trait KeyEncoder: Clone {
    fn encode_key(&self, id: PointOffsetType) -> Vec<u8>;
}

impl KeyEncoder for IntPayloadType {
    fn encode_key(&self, id: PointOffsetType) -> Vec<u8> {
        encode_i64_key_ascending(*self, id)
    }
}

impl KeyEncoder for FloatPayloadType {
    fn encode_key(&self, id: PointOffsetType) -> Vec<u8> {
        encode_f64_key_ascending(*self, id)
    }
}

pub trait KeyDecoder {
    fn decode_key(key: &[u8]) -> (PointOffsetType, Self);
}

impl KeyDecoder for IntPayloadType {
    fn decode_key(key: &[u8]) -> (PointOffsetType, Self) {
        decode_i64_key_ascending(key)
    }
}

impl KeyDecoder for FloatPayloadType {
    fn decode_key(key: &[u8]) -> (PointOffsetType, Self) {
        decode_f64_key_ascending(key)
    }
}

pub trait FromRangeValue {
    fn from_range(range_value: f64) -> Self;
}

impl FromRangeValue for FloatPayloadType {
    fn from_range(range_value: f64) -> Self {
        range_value
    }
}

impl FromRangeValue for IntPayloadType {
    fn from_range(range_value: f64) -> Self {
        range_value as Self
    }
}

pub trait ToRangeValue {
    fn to_range(value: Self) -> f64;
}

impl ToRangeValue for FloatPayloadType {
    fn to_range(value: Self) -> f64 {
        value
    }
}

impl ToRangeValue for IntPayloadType {
    fn to_range(value: Self) -> f64 {
        value as f64
    }
}

pub struct NumericIndex<T: KeyEncoder + KeyDecoder + FromRangeValue + Clone> {
    map: BTreeMap<Vec<u8>, u32>,
    db_wrapper: DatabaseColumnWrapper,
    histogram: Histogram,
    points_count: usize,
    max_values_per_point: usize,
    point_to_values: Vec<Vec<T>>,
}

impl<T: KeyEncoder + KeyDecoder + FromRangeValue + ToRangeValue + Clone> NumericIndex<T> {
    pub fn new(db: Arc<RwLock<DB>>, field: &str) -> Self {
        let store_cf_name = Self::storage_cf_name(field);
        let db_wrapper = DatabaseColumnWrapper::new(db, &store_cf_name);
        Self {
            map: BTreeMap::new(),
            db_wrapper,
            histogram: Histogram::new(HISTOGRAM_MAX_BUCKET_SIZE, HISTOGRAM_PRECISION),
            points_count: 0,
            max_values_per_point: 1,
            point_to_values: Default::default(),
        }
    }

    fn storage_cf_name(field: &str) -> String {
        format!("{field}_numeric")
    }

    pub fn recreate(&self) -> OperationResult<()> {
        self.db_wrapper.recreate_column_family()
    }

    fn add_value(&mut self, id: PointOffsetType, value: T) -> OperationResult<()> {
        let key = value.encode_key(id);
        self.db_wrapper.put(&key, id.to_be_bytes())?;
        Self::add_to_map(&mut self.map, &mut self.histogram, key, id);
        Ok(())
    }

    pub fn add_many_to_list(
        &mut self,
        idx: PointOffsetType,
        values: impl IntoIterator<Item = T>,
    ) -> OperationResult<()> {
        if self.point_to_values.len() <= idx as usize {
            self.point_to_values.resize(idx as usize + 1, Vec::new())
        }
        let values: Vec<T> = values.into_iter().collect();
        for value in &values {
            self.add_value(idx, value.clone())?;
        }
        if !values.is_empty() {
            self.points_count += 1;
            self.max_values_per_point = self.max_values_per_point.max(values.len());
        }
        self.point_to_values[idx as usize] = values;
        Ok(())
    }

    pub fn load(&mut self) -> OperationResult<bool> {
        if !self.db_wrapper.has_column_family()? {
            return Ok(false);
        };

        for (key, value) in self.db_wrapper.lock_db().iter()? {
            let value_idx = u32::from_be_bytes(value.as_ref().try_into().unwrap());
            let (idx, value) = T::decode_key(&key);

            if idx != value_idx {
                return Err(OperationError::service_error("incorrect key value"));
            }

            if self.point_to_values.len() <= idx as usize {
                self.point_to_values.resize(idx as usize + 1, Vec::new())
            }

            self.point_to_values[idx as usize].push(value);

            Self::add_to_map(&mut self.map, &mut self.histogram, key.to_vec(), idx);
        }
        for values in &self.point_to_values {
            if !values.is_empty() {
                self.points_count += 1;
                self.max_values_per_point = self.max_values_per_point.max(values.len());
            }
        }
        Ok(true)
    }

    pub fn flusher(&self) -> Flusher {
        self.db_wrapper.flusher()
    }

    pub fn remove_point(&mut self, idx: PointOffsetType) -> OperationResult<()> {
        if self.point_to_values.len() <= idx as usize {
            return Ok(());
        }

        let removed_values = std::mem::take(&mut self.point_to_values[idx as usize]);

        for value in &removed_values {
            let key = value.encode_key(idx);
            self.db_wrapper.remove(&key)?;
            Self::remove_from_map(&mut self.map, &mut self.histogram, key);
        }

        if !removed_values.is_empty() {
            self.points_count -= 1;
        }

        Ok(())
    }

    pub fn get_values(&self, idx: PointOffsetType) -> Option<&Vec<T>> {
        self.point_to_values.get(idx as usize)
    }

    fn range_cardinality(&self, range: &Range) -> CardinalityEstimation {
        let lbound = if let Some(lte) = range.lte {
            Included(lte)
        } else if let Some(lt) = range.lt {
            Excluded(lt)
        } else {
            Unbounded
        };

        let gbound = if let Some(gte) = range.gte {
            Included(gte)
        } else if let Some(gt) = range.gt {
            Excluded(gt)
        } else {
            Unbounded
        };

        let histogram_estimation = self.histogram.estimate(gbound, lbound);
        let min_estimation = histogram_estimation.0;
        let max_estimation = histogram_estimation.2;

        let total_values = self.map.len();
        // Example: points_count = 1000, total values = 2000, values_count = 500
        // min = max(1, 500 - (2000 - 1000)) = 1
        // exp = 500 / (2000 / 1000) = 250
        // max = min(1000, 500) = 500

        // Example: points_count = 1000, total values = 1200, values_count = 500
        // min = max(1, 500 - (1200 - 1000)) = 300
        // exp = 500 / (1200 / 1000) = 416
        // max = min(1000, 500) = 500
        let expected_min = max(
            min_estimation / self.max_values_per_point,
            max(
                min(1, min_estimation),
                min_estimation.saturating_sub(total_values - self.points_count),
            ),
        );
        let expected_max = min(self.points_count, max_estimation);

        let estimation = estimate_multi_value_selection_cardinality(
            self.points_count,
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
        histogram: &mut Histogram,
        key: Vec<u8>,
        id: PointOffsetType,
    ) {
        let existed_value = map.insert(key.clone(), id);
        // Histogram works with unique values (idx + value) only, so we need to
        // make sure that we don't add the same value twice.
        // key is a combination of value + idx, so we can use it to ensure than the pair is unique
        if existed_value.is_none() {
            histogram.insert(
                Self::key_to_histogram_point(&key),
                |x| Self::get_histogram_left_neighbor(map, x),
                |x| Self::get_histogram_right_neighbor(map, x),
            );
        }
    }

    pub fn remove_from_map(
        map: &mut BTreeMap<Vec<u8>, PointOffsetType>,
        histogram: &mut Histogram,
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

    fn key_to_histogram_point(key: &[u8]) -> Point {
        let (decoded_idx, decoded_val) = T::decode_key(key);
        Point {
            val: T::to_range(decoded_val),
            idx: decoded_idx as usize,
        }
    }

    fn get_histogram_left_neighbor(
        map: &BTreeMap<Vec<u8>, PointOffsetType>,
        point: &Point,
    ) -> Option<Point> {
        let key = T::from_range(point.val).encode_key(point.idx as PointOffsetType);
        map.range((Unbounded, Excluded(key)))
            .next_back()
            .map(|(key, _)| Self::key_to_histogram_point(key))
    }

    fn get_histogram_right_neighbor(
        map: &BTreeMap<Vec<u8>, PointOffsetType>,
        point: &Point,
    ) -> Option<Point> {
        let key = T::from_range(point.val).encode_key(point.idx as PointOffsetType);
        map.range((Excluded(key), Unbounded))
            .next()
            .map(|(key, _)| Self::key_to_histogram_point(key))
    }

    pub fn get_telemetry_data(&self) -> PayloadIndexTelemetry {
        PayloadIndexTelemetry {
            field_name: None,
            points_count: self.points_count,
            points_values_count: self.histogram.get_total_count(),
            histogram_bucket_size: Some(self.histogram.current_bucket_size()),
        }
    }
}

impl<T: KeyEncoder + KeyDecoder + FromRangeValue + ToRangeValue + Clone> PayloadFieldIndex
    for NumericIndex<T>
{
    fn indexed_points(&self) -> usize {
        self.points_count
    }

    fn load(&mut self) -> OperationResult<bool> {
        NumericIndex::load(self)
    }

    fn clear(self) -> OperationResult<()> {
        self.db_wrapper.recreate_column_family()
    }

    fn flusher(&self) -> Flusher {
        NumericIndex::flusher(self)
    }

    fn filter(
        &self,
        condition: &FieldCondition,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + '_>> {
        let cond_range = condition.range.as_ref()?;

        let start_bound = match cond_range {
            Range { gt: Some(gt), .. } => {
                let v: T = T::from_range(gt.to_owned());
                Excluded(v.encode_key(PointOffsetType::MAX))
            }
            Range { gte: Some(gte), .. } => {
                let v: T = T::from_range(gte.to_owned());
                Included(v.encode_key(PointOffsetType::MIN))
            }
            _ => Unbounded,
        };

        let end_bound = match cond_range {
            Range { lt: Some(lt), .. } => {
                let v: T = T::from_range(lt.to_owned());
                Excluded(v.encode_key(PointOffsetType::MIN))
            }
            Range { lte: Some(lte), .. } => {
                let v: T = T::from_range(lte.to_owned());
                Included(v.encode_key(PointOffsetType::MAX))
            }
            _ => Unbounded,
        };

        // map.range
        // Panics if range start > end. Panics if range start == end and both bounds are Excluded.
        match (&start_bound, &end_bound) {
            (Excluded(s), Excluded(e)) if s == e => {
                // range start and end are equal and excluded in BTreeMap
                return Some(Box::new(vec![].into_iter()));
            }
            (Included(s) | Excluded(s), Included(e) | Excluded(e)) if s > e => {
                //range start is greater than range end
                return Some(Box::new(vec![].into_iter()));
            }
            _ => {}
        }

        Some(Box::new(
            self.map.range((start_bound, end_bound)).map(|(_, v)| *v),
        ))
    }

    fn estimate_cardinality(&self, condition: &FieldCondition) -> Option<CardinalityEstimation> {
        condition.range.as_ref().map(|range| {
            let mut cardinality = self.range_cardinality(range);
            cardinality
                .primary_clauses
                .push(PrimaryCondition::Condition(condition.clone()));
            cardinality
        })
    }

    fn payload_blocks(
        &self,
        threshold: usize,
        key: PayloadKeyType,
    ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_> {
        let mut lower_bound = Unbounded;
        let mut pre_lower_bound = None;
        let mut payload_conditions = Vec::new();

        let value_per_point = self.map.len() as f64 / self.points_count as f64;
        let effective_threshold = (threshold as f64 * value_per_point) as usize;

        loop {
            let upper_bound = self
                .histogram
                .get_range_by_size(lower_bound, effective_threshold / 2);

            if let Some(pre_lower_bound) = pre_lower_bound {
                let range = Range {
                    lt: match upper_bound {
                        Excluded(val) => Some(val),
                        _ => None,
                    },
                    gt: match pre_lower_bound {
                        Excluded(val) => Some(val),
                        _ => None,
                    },
                    gte: match pre_lower_bound {
                        Included(val) => Some(val),
                        _ => None,
                    },
                    lte: match upper_bound {
                        Included(val) => Some(val),
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
                    cardinality: self.points_count,
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

    fn count_indexed_points(&self) -> usize {
        self.points_count
    }
}

impl ValueIndexer<IntPayloadType> for NumericIndex<IntPayloadType> {
    fn add_many(
        &mut self,
        id: PointOffsetType,
        values: Vec<IntPayloadType>,
    ) -> OperationResult<()> {
        self.add_many_to_list(id, values)
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
        self.add_many_to_list(id, values)
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

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use rand::prelude::StdRng;
    use rand::{Rng, SeedableRng};
    use tempfile::{Builder, TempDir};

    use super::*;
    use crate::common::rocksdb_wrapper::open_db_with_existing_cf;

    const COLUMN_NAME: &str = "test";

    fn get_index() -> (TempDir, NumericIndex<f64>) {
        let tmp_dir = Builder::new()
            .prefix("test_numeric_index")
            .tempdir()
            .unwrap();
        let db = open_db_with_existing_cf(tmp_dir.path()).unwrap();
        let index: NumericIndex<_> = NumericIndex::new(db, COLUMN_NAME);
        index.recreate().unwrap();
        (tmp_dir, index)
    }

    fn random_index(num_points: usize, values_per_point: usize) -> (TempDir, NumericIndex<f64>) {
        let mut rng = StdRng::seed_from_u64(42);
        let (tmp_dir, mut index) = get_index();

        for i in 0..num_points {
            let values = (0..values_per_point).map(|_| rng.gen_range(0.0..100.0));
            index
                .add_many_to_list(i as PointOffsetType, values)
                .unwrap();
        }

        (tmp_dir, index)
    }

    fn cardinality_request(index: &NumericIndex<f64>, query: Range) -> CardinalityEstimation {
        let estimation = index.range_cardinality(&query);

        let result = index
            .filter(&FieldCondition::new_range("".to_string(), query))
            .unwrap()
            .unique()
            .collect_vec();

        eprintln!("estimation = {:#?}", estimation);
        eprintln!("result.len() = {:#?}", result.len());
        assert!(estimation.min <= result.len());
        assert!(estimation.max >= result.len());
        estimation
    }

    #[test]
    fn test_cardinality_exp() {
        let (_tmp_dir, index) = random_index(1000, 1);

        cardinality_request(
            &index,
            Range {
                lt: Some(20.0),
                gt: None,
                gte: Some(10.0),
                lte: None,
            },
        );
        cardinality_request(
            &index,
            Range {
                lt: Some(60.0),
                gt: None,
                gte: Some(10.0),
                lte: None,
            },
        );

        let (_tmp_dir, index) = random_index(1000, 2);
        cardinality_request(
            &index,
            Range {
                lt: Some(20.0),
                gt: None,
                gte: Some(10.0),
                lte: None,
            },
        );
        cardinality_request(
            &index,
            Range {
                lt: Some(60.0),
                gt: None,
                gte: Some(10.0),
                lte: None,
            },
        );

        cardinality_request(
            &index,
            Range {
                lt: None,
                gt: None,
                gte: Some(10.0),
                lte: None,
            },
        );

        cardinality_request(
            &index,
            Range {
                lt: None,
                gt: None,
                gte: Some(110.0),
                lte: None,
            },
        );
    }

    #[test]
    fn test_payload_blocks() {
        let (_tmp_dir, index) = random_index(1000, 2);
        let threshold = 100;
        let blocks = index
            .payload_blocks(threshold, "test".to_owned())
            .collect_vec();
        assert!(!blocks.is_empty());
        eprintln!("threshold {threshold}, blocks.len() = {:#?}", blocks.len());

        let threshold = 500;
        let blocks = index
            .payload_blocks(threshold, "test".to_owned())
            .collect_vec();
        assert!(!blocks.is_empty());
        eprintln!("threshold {threshold}, blocks.len() = {:#?}", blocks.len());

        let threshold = 1000;
        let blocks = index
            .payload_blocks(threshold, "test".to_owned())
            .collect_vec();
        assert!(!blocks.is_empty());
        eprintln!("threshold {threshold}, blocks.len() = {:#?}", blocks.len());

        let threshold = 10000;
        let blocks = index
            .payload_blocks(threshold, "test".to_owned())
            .collect_vec();
        assert!(!blocks.is_empty());
        eprintln!("threshold {threshold}, blocks.len() = {:#?}", blocks.len());
    }

    #[test]
    fn test_payload_blocks_small() {
        let (_tmp_dir, mut index) = get_index();
        let threshold = 4;
        let values = vec![
            vec![1.0],
            vec![1.0],
            vec![1.0],
            vec![1.0],
            vec![1.0],
            vec![2.0],
            vec![2.0],
            vec![2.0],
            vec![2.0],
        ];

        values.into_iter().enumerate().for_each(|(idx, values)| {
            index
                .add_many_to_list(idx as PointOffsetType + 1, values)
                .unwrap()
        });

        let blocks = index
            .payload_blocks(threshold, "test".to_owned())
            .collect_vec();
        assert!(!blocks.is_empty());
    }

    #[test]
    fn test_numeric_index_load_from_disk() {
        let (_tmp_dir, mut index) = get_index();

        let values = vec![
            vec![1.0],
            vec![1.0],
            vec![1.0],
            vec![1.0],
            vec![1.0],
            vec![2.0],
            vec![2.5],
            vec![2.6],
            vec![3.0],
        ];

        values.into_iter().enumerate().for_each(|(idx, values)| {
            index
                .add_many_to_list(idx as PointOffsetType + 1, values)
                .unwrap()
        });

        index.flusher()().unwrap();

        let db_ref = index.db_wrapper.database;
        let mut new_index: NumericIndex<f64> = NumericIndex::new(db_ref, COLUMN_NAME);
        new_index.load().unwrap();

        test_cond(
            &new_index,
            Range {
                gt: None,
                gte: None,
                lt: None,
                lte: Some(2.6),
            },
            vec![1, 2, 3, 4, 5, 6, 7, 8],
        );
    }

    #[test]
    fn test_numeric_index() {
        let (_tmp_dir, mut index) = get_index();

        let values = vec![
            vec![1.0],
            vec![1.0],
            vec![1.0],
            vec![1.0],
            vec![1.0],
            vec![2.0],
            vec![2.5],
            vec![2.6],
            vec![3.0],
        ];

        values.into_iter().enumerate().for_each(|(idx, values)| {
            index
                .add_many_to_list(idx as PointOffsetType + 1, values)
                .unwrap()
        });

        test_cond(
            &index,
            Range {
                gt: Some(1.0),
                gte: None,
                lt: None,
                lte: None,
            },
            vec![6, 7, 8, 9],
        );

        test_cond(
            &index,
            Range {
                gt: None,
                gte: Some(1.0),
                lt: None,
                lte: None,
            },
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9],
        );

        test_cond(
            &index,
            Range {
                gt: None,
                gte: None,
                lt: Some(2.6),
                lte: None,
            },
            vec![1, 2, 3, 4, 5, 6, 7],
        );

        test_cond(
            &index,
            Range {
                gt: None,
                gte: None,
                lt: None,
                lte: Some(2.6),
            },
            vec![1, 2, 3, 4, 5, 6, 7, 8],
        );

        test_cond(
            &index,
            Range {
                gt: None,
                gte: Some(2.0),
                lt: None,
                lte: Some(2.6),
            },
            vec![6, 7, 8],
        );
    }

    fn test_cond<T: KeyEncoder + KeyDecoder + FromRangeValue + ToRangeValue + Clone>(
        index: &NumericIndex<T>,
        rng: Range,
        result: Vec<u32>,
    ) {
        let condition = FieldCondition {
            key: "".to_string(),
            r#match: None,
            range: Some(rng),
            geo_bounding_box: None,
            geo_radius: None,
            values_count: None,
        };

        let offsets = index.filter(&condition).unwrap().collect_vec();

        assert_eq!(offsets, result);
    }
}
