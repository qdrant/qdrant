//! The [`Encodable`] key-format trait: on-disk key encoding/decoding and
//! encoded-order comparison shared by every numeric-index storage variant.

use chrono::DateTime;
use common::types::PointOffsetType;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::index::key_encoding::{
    decode_f64_key_ascending, decode_i64_key_ascending, decode_u128_key_ascending,
    encode_f64_key_ascending, encode_i64_key_ascending, encode_u128_key_ascending,
};
use crate::types::{DateTimePayloadType, FloatPayloadType, IntPayloadType};

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
