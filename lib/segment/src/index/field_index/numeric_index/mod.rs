mod immutable_numeric_index;
mod mutable_numeric_index;

#[cfg(test)]
mod tests;

use std::cmp::{max, min};
use std::marker::PhantomData;
use std::ops::Bound;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::str::FromStr;
use std::sync::Arc;

use chrono::DateTime;
use common::types::PointOffsetType;
use delegate::delegate;
use mutable_numeric_index::MutableNumericIndex;
use parking_lot::RwLock;
use rocksdb::DB;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

use self::immutable_numeric_index::{ImmutableNumericIndex, NumericIndexKey};
use super::utils::check_boundaries;
use super::FieldIndexBuilderTrait;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::rocksdb_buffered_delete_wrapper::DatabaseColumnScheduledDeleteWrapper;
use crate::common::Flusher;
use crate::index::field_index::histogram::{Histogram, Numericable};
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
    DateTimePayloadType, FieldCondition, FloatPayloadType, IntPayloadType, PayloadKeyType, Range,
    RangeInterface,
};

const HISTOGRAM_MAX_BUCKET_SIZE: usize = 10_000;
const HISTOGRAM_PRECISION: f64 = 0.01;

pub trait StreamRange<T> {
    fn stream_range(
        &self,
        range: &RangeInterface,
    ) -> Box<dyn DoubleEndedIterator<Item = (T, PointOffsetType)> + '_>;
}

pub trait Encodable: Copy + Serialize + DeserializeOwned {
    fn encode_key(&self, id: PointOffsetType) -> Vec<u8>;

    fn decode_key(key: &[u8]) -> (PointOffsetType, Self);

    fn cmp_encoded(&self, other: &Self) -> std::cmp::Ordering;
}

impl Encodable for IntPayloadType {
    fn encode_key(&self, id: PointOffsetType) -> Vec<u8> {
        encode_i64_key_ascending(*self, id)
    }

    fn decode_key(key: &[u8]) -> (PointOffsetType, Self) {
        decode_i64_key_ascending(key)
    }

    fn cmp_encoded(&self, other: &Self) -> std::cmp::Ordering {
        self.cmp(other)
    }
}

impl Encodable for FloatPayloadType {
    fn encode_key(&self, id: PointOffsetType) -> Vec<u8> {
        encode_f64_key_ascending(*self, id)
    }

    fn decode_key(key: &[u8]) -> (PointOffsetType, Self) {
        decode_f64_key_ascending(key)
    }

    fn cmp_encoded(&self, other: &Self) -> std::cmp::Ordering {
        if self.is_nan() && other.is_nan() {
            return std::cmp::Ordering::Equal;
        }
        if self.is_nan() {
            return std::cmp::Ordering::Less;
        }
        if other.is_nan() {
            return std::cmp::Ordering::Greater;
        }
        self.partial_cmp(other).unwrap()
    }
}

/// Encodes timestamps as i64 in microseconds
impl Encodable for DateTimePayloadType {
    fn encode_key(&self, id: PointOffsetType) -> Vec<u8> {
        encode_i64_key_ascending(self.timestamp(), id)
    }

    fn decode_key(key: &[u8]) -> (PointOffsetType, Self) {
        let (id, timestamp) = decode_i64_key_ascending(key);
        let datetime =
            DateTime::from_timestamp(timestamp / 1000, (timestamp % 1000) as u32 * 1_000_000)
                .unwrap_or_else(|| {
                    log::warn!("Failed to decode timestamp {timestamp}, fallback to UNIX_EPOCH");
                    DateTime::UNIX_EPOCH
                });
        (id, datetime.into())
    }

    fn cmp_encoded(&self, other: &Self) -> std::cmp::Ordering {
        self.timestamp().cmp(&other.timestamp())
    }
}

impl<T: Encodable + Numericable> Range<T> {
    pub(in crate::index::field_index::numeric_index) fn as_index_key_bounds(
        &self,
    ) -> (Bound<NumericIndexKey<T>>, Bound<NumericIndexKey<T>>) {
        let start_bound = match self {
            Range { gt: Some(gt), .. } => Excluded(NumericIndexKey::new(*gt, PointOffsetType::MAX)),
            Range { gte: Some(gte), .. } => {
                Included(NumericIndexKey::new(*gte, PointOffsetType::MIN))
            }
            _ => Unbounded,
        };

        let end_bound = match self {
            Range { lt: Some(lt), .. } => Excluded(NumericIndexKey::new(*lt, PointOffsetType::MIN)),
            Range { lte: Some(lte), .. } => {
                Included(NumericIndexKey::new(*lte, PointOffsetType::MAX))
            }
            _ => Unbounded,
        };

        (start_bound, end_bound)
    }
}

pub enum NumericIndexInner<T: Encodable + Numericable + Default> {
    Mutable(MutableNumericIndex<T>),
    Immutable(ImmutableNumericIndex<T>),
}

impl<T: Encodable + Numericable + Default> NumericIndexInner<T> {
    pub fn new(db: Arc<RwLock<DB>>, field: &str, is_appendable: bool) -> Self {
        if is_appendable {
            NumericIndexInner::Mutable(MutableNumericIndex::new(db, field))
        } else {
            NumericIndexInner::Immutable(ImmutableNumericIndex::new(db, field))
        }
    }

    #[cfg(test)]
    pub fn wrap<P>(self) -> NumericIndex<T, P> {
        NumericIndex {
            inner: self,
            _phantom: PhantomData,
        }
    }

    fn get_db_wrapper(&self) -> &DatabaseColumnScheduledDeleteWrapper {
        match self {
            NumericIndexInner::Mutable(index) => index.get_db_wrapper(),
            NumericIndexInner::Immutable(index) => index.get_db_wrapper(),
        }
    }

    fn get_histogram(&self) -> &Histogram<T> {
        match self {
            NumericIndexInner::Mutable(index) => &index.histogram,
            NumericIndexInner::Immutable(index) => &index.histogram,
        }
    }

    fn get_points_count(&self) -> usize {
        match self {
            NumericIndexInner::Mutable(index) => index.points_count,
            NumericIndexInner::Immutable(index) => index.points_count,
        }
    }

    fn total_unique_values_count(&self) -> usize {
        match self {
            NumericIndexInner::Mutable(index) => index.total_unique_values_count(),
            NumericIndexInner::Immutable(index) => index.total_unique_values_count(),
        }
    }

    fn storage_cf_name(field: &str) -> String {
        format!("{field}_numeric")
    }

    pub fn load(&mut self) -> OperationResult<bool> {
        match self {
            NumericIndexInner::Mutable(index) => index.load(),
            NumericIndexInner::Immutable(index) => index.load(),
        }
    }

    pub fn flusher(&self) -> Flusher {
        self.get_db_wrapper().flusher()
    }

    pub fn remove_point(&mut self, idx: PointOffsetType) -> OperationResult<()> {
        match self {
            NumericIndexInner::Mutable(index) => index.remove_point(idx),
            NumericIndexInner::Immutable(index) => index.remove_point(idx),
        }
    }

    pub fn check_values_any(&self, idx: PointOffsetType, check_fn: impl Fn(&T) -> bool) -> bool {
        match self {
            NumericIndexInner::Mutable(index) => index.check_values_any(idx, check_fn),
            NumericIndexInner::Immutable(index) => index.check_values_any(idx, check_fn),
        }
    }

    pub fn get_values(&self, idx: PointOffsetType) -> Option<Box<dyn Iterator<Item = T> + '_>> {
        match self {
            NumericIndexInner::Mutable(index) => index.get_values(idx),
            NumericIndexInner::Immutable(index) => index.get_values(idx),
        }
    }

    pub fn values_count(&self, idx: PointOffsetType) -> usize {
        match self {
            NumericIndexInner::Mutable(index) => index.values_count(idx).unwrap_or_default(),
            NumericIndexInner::Immutable(index) => index.values_count(idx).unwrap_or_default(),
        }
    }

    /// Maximum number of values per point
    ///
    /// # Warning
    ///
    /// Zero if the index is empty.
    pub fn max_values_per_point(&self) -> usize {
        match self {
            NumericIndexInner::Mutable(index) => index.max_values_per_point,
            NumericIndexInner::Immutable(index) => index.max_values_per_point,
        }
    }

    fn range_cardinality(&self, range: &RangeInterface) -> CardinalityEstimation {
        let max_values_per_point = self.max_values_per_point();
        if max_values_per_point == 0 {
            return CardinalityEstimation::exact(0);
        }

        let range = match range {
            RangeInterface::Float(float_range) => float_range.map(T::from_f64),
            RangeInterface::DateTime(datetime_range) => {
                datetime_range.map(|dt| T::from_i64(dt.timestamp()))
            }
        };

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

        let histogram_estimation = self.get_histogram().estimate(gbound, lbound);
        let min_estimation = histogram_estimation.0;
        let max_estimation = histogram_estimation.2;

        let total_values = self.total_unique_values_count();
        // Example: points_count = 1000, total values = 2000, values_count = 500
        // min = max(1, 500 - (2000 - 1000)) = 1
        // exp = 500 / (2000 / 1000) = 250
        // max = min(1000, 500) = 500

        // Example: points_count = 1000, total values = 1200, values_count = 500
        // min = max(1, 500 - (1200 - 1000)) = 300
        // exp = 500 / (1200 / 1000) = 416
        // max = min(1000, 500) = 500
        // Note: max_values_per_point is never zero here because we check it above
        let expected_min = max(
            min_estimation / max_values_per_point,
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

    pub fn get_telemetry_data(&self) -> PayloadIndexTelemetry {
        PayloadIndexTelemetry {
            field_name: None,
            points_count: self.get_points_count(),
            points_values_count: self.get_histogram().get_total_count(),
            histogram_bucket_size: Some(self.get_histogram().current_bucket_size()),
        }
    }

    pub fn values_is_empty(&self, idx: PointOffsetType) -> bool {
        self.values_count(idx) == 0
    }
}

pub struct NumericIndex<T: Encodable + Numericable + Default, P> {
    inner: NumericIndexInner<T>,
    _phantom: PhantomData<P>,
}

impl<T: Encodable + Numericable + Default, P> NumericIndex<T, P> {
    pub fn new(db: Arc<RwLock<DB>>, field: &str, is_appendable: bool) -> Self {
        Self {
            inner: NumericIndexInner::new(db, field, is_appendable),
            _phantom: PhantomData,
        }
    }

    pub fn builder(db: Arc<RwLock<DB>>, field: &str) -> NumericIndexBuilder<T, P>
    where
        Self: ValueIndexer,
    {
        NumericIndexBuilder(Self::new(db, field, true))
    }

    pub fn inner(&self) -> &NumericIndexInner<T> {
        &self.inner
    }

    pub fn mut_inner(&mut self) -> &mut NumericIndexInner<T> {
        &mut self.inner
    }

    delegate! {
        to self.inner {
            pub fn check_values_any(&self, idx: PointOffsetType, check_fn: impl Fn(&T) -> bool) -> bool;
            pub fn clear(self) -> OperationResult<()>;
            pub fn get_telemetry_data(&self) -> PayloadIndexTelemetry;
            pub fn load(&mut self) -> OperationResult<bool>;
            pub fn values_count(&self, idx: PointOffsetType) -> usize;
            pub fn get_values(&self, idx: PointOffsetType) -> Option<Box<dyn Iterator<Item = T> + '_>>;
            pub fn values_is_empty(&self, idx: PointOffsetType) -> bool;
        }
    }
}

pub struct NumericIndexBuilder<T: Encodable + Numericable + Default, P>(NumericIndex<T, P>)
where
    NumericIndex<T, P>: ValueIndexer;

impl<T: Encodable + Numericable + Default, P> FieldIndexBuilderTrait for NumericIndexBuilder<T, P>
where
    NumericIndex<T, P>: ValueIndexer,
{
    type FieldIndexType = NumericIndex<T, P>;

    fn init(&mut self) -> OperationResult<()> {
        self.0.inner.get_db_wrapper().recreate_column_family()
    }

    fn add_point(&mut self, id: PointOffsetType, payload: &[&Value]) -> OperationResult<()> {
        self.0.add_point(id, payload)
    }

    fn finalize(self) -> OperationResult<Self::FieldIndexType> {
        Ok(self.0)
    }
}

impl<T: Encodable + Numericable + Default> PayloadFieldIndex for NumericIndexInner<T> {
    fn count_indexed_points(&self) -> usize {
        self.get_points_count()
    }

    fn load(&mut self) -> OperationResult<bool> {
        NumericIndexInner::load(self)
    }

    fn clear(self) -> OperationResult<()> {
        self.get_db_wrapper().recreate_column_family()
    }

    fn flusher(&self) -> Flusher {
        NumericIndexInner::flusher(self)
    }

    fn filter(
        &self,
        condition: &FieldCondition,
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + '_>> {
        let range_cond = condition
            .range
            .as_ref()
            .ok_or_else(|| OperationError::service_error("failed to get range condition"))?;

        let (start_bound, end_bound) = match range_cond {
            RangeInterface::Float(float_range) => float_range.map(T::from_f64),
            RangeInterface::DateTime(datetime_range) => {
                datetime_range.map(|dt| T::from_i64(dt.timestamp()))
            }
        }
        .as_index_key_bounds();

        // map.range
        // Panics if range start > end. Panics if range start == end and both bounds are Excluded.
        if !check_boundaries(&start_bound, &end_bound) {
            return Ok(Box::new(vec![].into_iter()));
        }

        Ok(match self {
            NumericIndexInner::Mutable(index) => {
                let start_bound = start_bound.map(|k| k.encode());
                let end_bound = end_bound.map(|k| k.encode());
                Box::new(index.values_range(start_bound, end_bound))
            }
            NumericIndexInner::Immutable(index) => {
                Box::new(index.values_range(start_bound, end_bound))
            }
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

        let value_per_point =
            self.total_unique_values_count() as f64 / self.get_points_count() as f64;
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
                let cardinality = self.range_cardinality(&RangeInterface::Float(range.clone()));
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

impl ValueIndexer for NumericIndex<IntPayloadType, IntPayloadType> {
    type ValueType = IntPayloadType;

    fn add_many(
        &mut self,
        id: PointOffsetType,
        values: Vec<IntPayloadType>,
    ) -> OperationResult<()> {
        match &mut self.inner {
            NumericIndexInner::Mutable(index) => index.add_many_to_list(id, values),
            NumericIndexInner::Immutable(_) => Err(OperationError::service_error(
                "Can't add values to immutable numeric index",
            )),
        }
    }

    fn get_value(&self, value: &Value) -> Option<IntPayloadType> {
        value.as_i64()
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        self.inner.remove_point(id)
    }
}

impl ValueIndexer for NumericIndex<IntPayloadType, DateTimePayloadType> {
    type ValueType = DateTimePayloadType;

    fn add_many(
        &mut self,
        id: PointOffsetType,
        values: Vec<DateTimePayloadType>,
    ) -> OperationResult<()> {
        match &mut self.inner {
            NumericIndexInner::Mutable(index) => {
                index.add_many_to_list(id, values.into_iter().map(|x| x.timestamp()))
            }
            NumericIndexInner::Immutable(_) => Err(OperationError::service_error(
                "Can't add values to immutable numeric index",
            )),
        }
    }

    fn get_value(&self, value: &Value) -> Option<DateTimePayloadType> {
        DateTimePayloadType::from_str(value.as_str()?).ok()
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        self.inner.remove_point(id)
    }
}

impl ValueIndexer for NumericIndex<FloatPayloadType, FloatPayloadType> {
    type ValueType = FloatPayloadType;

    fn add_many(
        &mut self,
        id: PointOffsetType,
        values: Vec<FloatPayloadType>,
    ) -> OperationResult<()> {
        match &mut self.inner {
            NumericIndexInner::Mutable(index) => index.add_many_to_list(id, values),
            NumericIndexInner::Immutable(_) => Err(OperationError::service_error(
                "Can't add values to immutable numeric index",
            )),
        }
    }

    fn get_value(&self, value: &Value) -> Option<FloatPayloadType> {
        value.as_f64()
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        self.inner.remove_point(id)
    }
}

impl<T> StreamRange<T> for NumericIndexInner<T>
where
    T: Encodable + Numericable + Default,
{
    fn stream_range(
        &self,
        range: &RangeInterface,
    ) -> Box<dyn DoubleEndedIterator<Item = (T, PointOffsetType)> + '_> {
        let range = match range {
            RangeInterface::Float(float_range) => float_range.map(T::from_f64),
            RangeInterface::DateTime(datetime_range) => {
                datetime_range.map(|dt| T::from_i64(dt.timestamp()))
            }
        };
        let (start_bound, end_bound) = range.as_index_key_bounds();

        // map.range
        // Panics if range start > end. Panics if range start == end and both bounds are Excluded.
        if !check_boundaries(&start_bound, &end_bound) {
            return Box::new(vec![].into_iter());
        }

        match self {
            NumericIndexInner::Mutable(index) => {
                let start_bound = start_bound.map(|k| k.encode());
                let end_bound = end_bound.map(|k| k.encode());
                Box::new(index.orderable_values_range(start_bound, end_bound))
            }
            NumericIndexInner::Immutable(index) => {
                Box::new(index.orderable_values_range(start_bound, end_bound))
            }
        }
    }
}
