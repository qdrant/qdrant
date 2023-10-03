use std::fmt::Debug;

use common::types::PointOffsetType;

use super::Encodable;
use crate::index::field_index::histogram::Numericable;

#[derive(Clone, PartialEq, Debug)]
pub struct NumericIndexKey<T> {
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
        match self.key.cmp_encoded(&other.key) {
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
