use super::Encodable;
use crate::index::field_index::histogram::Numericable;
use crate::types::PointOffsetType;

#[derive(Clone, PartialEq)]
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

impl<T: PartialEq + PartialOrd> PartialOrd for NumericIndexKey<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.key.partial_cmp(&other.key) {
            Some(core::cmp::Ordering::Equal) => {}
            None => {}
            ord => return ord,
        }
        self.idx.partial_cmp(&other.idx)
    }
}

impl<T: PartialEq + PartialOrd> Eq for NumericIndexKey<T> {}

impl<T: PartialEq + PartialOrd> Ord for NumericIndexKey<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.key.partial_cmp(&other.key) {
            Some(core::cmp::Ordering::Equal) => {}
            None => {}
            ord => return ord.unwrap(),
        }
        self.idx.cmp(&other.idx)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::super::Encodable;
    use super::*;
    use crate::types::FloatPayloadType;

    #[test]
    fn test_sorting() {
        let pairs = [
            NumericIndexKey::new(0.0, 1),
            NumericIndexKey::new(0.0, 3),
            //NumericIndexKey::new(-0.0, 2),
            //NumericIndexKey::new(-0.0, 4),
            NumericIndexKey::new(0.4, 2),
            NumericIndexKey::new(-0.4, 3),
            NumericIndexKey::new(5.0, 1),
            NumericIndexKey::new(-5.0, 1),
            //NumericIndexKey::new(f64::NAN, 0),
            //NumericIndexKey::new(f64::INFINITY, 0),
            //NumericIndexKey::new(f64::NEG_INFINITY, 1),
            //NumericIndexKey::new(f64::NEG_INFINITY, 2),
            //NumericIndexKey::new(f64::NEG_INFINITY, 3),
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
            //println!("{}:{} \t\t {}:{}", k.key, k.idx, decoded_float, decoded_id);
            assert_eq!(decoded_id, k.idx);
            assert_eq!(decoded_float, k.key);
        }
    }
}
