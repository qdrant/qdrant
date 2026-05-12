mod builders;
pub mod immutable_numeric_index;
mod index;
pub mod mmap_numeric_index;
pub mod mutable_numeric_index;
mod numeric_field_index;
mod storage;
mod value_indexer;

pub use builders::{NumericIndexBuilder, NumericIndexGridstoreBuilder, NumericIndexMmapBuilder};
pub use index::{NumericIndex, NumericIndexIntoInnerValue};
pub use numeric_field_index::{NumericFieldIndex, NumericFieldIndexRead};
pub use storage::NumericIndexInner;

#[cfg(test)]
mod tests;

use std::ops::Bound;
use std::ops::Bound::{Excluded, Included, Unbounded};

use chrono::DateTime;
use common::types::PointOffsetType;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::common::operation_error::OperationResult;
use crate::index::field_index::numeric_point::{Numericable, Point};
use crate::index::key_encoding::{
    decode_f64_key_ascending, decode_i64_key_ascending, decode_u128_key_ascending,
    encode_f64_key_ascending, encode_i64_key_ascending, encode_u128_key_ascending,
};
use crate::types::{DateTimePayloadType, FloatPayloadType, IntPayloadType, Range, RangeInterface};

const HISTOGRAM_MAX_BUCKET_SIZE: usize = 10_000;
const HISTOGRAM_PRECISION: f64 = 0.01;

pub trait StreamRange<T> {
    fn stream_range(
        &self,
        range: &RangeInterface,
    ) -> OperationResult<impl DoubleEndedIterator<Item = (T, PointOffsetType)> + '_>;
}

pub trait Encodable: Copy + Serialize + DeserializeOwned + 'static {
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

impl Encodable for u128 {
    fn encode_key(&self, id: PointOffsetType) -> Vec<u8> {
        encode_u128_key_ascending(*self, id)
    }

    fn decode_key(key: &[u8]) -> (PointOffsetType, Self) {
        decode_u128_key_ascending(key)
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
    ) -> (Bound<Point<T>>, Bound<Point<T>>) {
        let start_bound = match self {
            Range { gt: Some(gt), .. } => Excluded(Point::new(*gt, PointOffsetType::MAX)),
            Range { gte: Some(gte), .. } => Included(Point::new(*gte, PointOffsetType::MIN)),
            _ => Unbounded,
        };

        let end_bound = match self {
            Range { lt: Some(lt), .. } => Excluded(Point::new(*lt, PointOffsetType::MIN)),
            Range { lte: Some(lte), .. } => Included(Point::new(*lte, PointOffsetType::MAX)),
            _ => Unbounded,
        };

        (start_bound, end_bound)
    }
}
