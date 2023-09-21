use std::fmt::Debug;

use crate::index::field_index::histogram::Numericable;
use crate::index::key_encoding::{
    decode_f64_key_ascending, decode_i64_key_ascending, encode_f64_key_ascending,
    encode_i64_key_ascending,
};
use crate::types::{FloatPayloadType, IntPayloadType, PointOffsetType};

pub trait Encodable: Copy + Default {
    fn encode_key(&self, id: PointOffsetType) -> Vec<u8>;

    fn decode_key(key: &[u8]) -> (PointOffsetType, Self);

    fn cmp_key(&self, other: &Self) -> std::cmp::Ordering;
}

impl Encodable for IntPayloadType {
    fn encode_key(&self, id: PointOffsetType) -> Vec<u8> {
        encode_i64_key_ascending(*self, id)
    }

    fn decode_key(key: &[u8]) -> (PointOffsetType, Self) {
        decode_i64_key_ascending(key)
    }

    fn cmp_key(&self, other: &Self) -> std::cmp::Ordering {
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

    fn cmp_key(&self, other: &Self) -> std::cmp::Ordering {
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

#[derive(Clone, PartialEq, Debug)]
pub struct NumericIndexKey<T: PartialEq + PartialOrd> {
    pub key: T,
    pub idx: PointOffsetType,
}

impl<T: PartialEq + PartialOrd + Encodable + Numericable> NumericIndexKey<T> {
    pub fn new(key: T, idx: PointOffsetType) -> Self {
        Self { key, idx }
    }

    pub fn encode(&self) -> Vec<u8> {
        T::encode_key(&self.key, self.idx)
    }

    pub fn decode(bytes: &[u8]) -> Self {
        let (idx, key) = T::decode_key(bytes);
        Self { key, idx }
    }
}

impl<T: PartialEq + PartialOrd + Encodable> PartialOrd for NumericIndexKey<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.key.cmp_key(&other.key) {
            core::cmp::Ordering::Equal => {}
            ord => return Some(ord),
        }
        Some(self.idx.cmp(&other.idx))
    }
}

impl<T: PartialEq + PartialOrd + Encodable> Eq for NumericIndexKey<T> {}

impl<T: PartialEq + PartialOrd + Encodable> Ord for NumericIndexKey<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other)
            .expect("Numeric index key is not comparable")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::types::FloatPayloadType;

    #[test]
    fn test_numeric_index_key_sorting() {
        let pairs = [
            NumericIndexKey::new(0.0, 1),
            NumericIndexKey::new(0.0, 3),
            NumericIndexKey::new(-0.0, 2),
            NumericIndexKey::new(-0.0, 4),
            NumericIndexKey::new(0.4, 2),
            NumericIndexKey::new(-0.4, 3),
            NumericIndexKey::new(5.0, 1),
            NumericIndexKey::new(-5.0, 1),
            NumericIndexKey::new(f64::NAN, 0),
            NumericIndexKey::new(f64::NAN, 2),
            NumericIndexKey::new(f64::NAN, 3),
            NumericIndexKey::new(f64::INFINITY, 0),
            NumericIndexKey::new(f64::NEG_INFINITY, 1),
            NumericIndexKey::new(f64::NEG_INFINITY, 2),
            NumericIndexKey::new(f64::NEG_INFINITY, 3),
        ];

        let encoded: Vec<Vec<u8>> = pairs
            .iter()
            .map(|k| FloatPayloadType::encode_key(&k.key, k.idx))
            .collect();

        let set_byte: BTreeSet<Vec<u8>> = BTreeSet::from_iter(encoded.iter().cloned());
        let set_keys: BTreeSet<NumericIndexKey<FloatPayloadType>> =
            BTreeSet::from_iter(pairs.iter().cloned());

        for (b, k) in set_byte.iter().zip(set_keys.iter()) {
            let (decoded_id, decoded_float) = FloatPayloadType::decode_key(b.as_slice());
            if decoded_float.is_nan() && k.key.is_nan() {
                assert_eq!(decoded_id, k.idx);
            } else {
                assert_eq!(decoded_id, k.idx);
                assert_eq!(decoded_float, k.key);
            }
        }
    }
}
