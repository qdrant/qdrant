use std::collections::HashMap;

use crate::types::SeqNumberType;

/// Compressed representation of Vec<SeqNumberType>
/// Which takes advantage of the fact that the sequence numbers are likely to be < 2**32
///
/// Implements a required subset of the Vec API:
///
/// * get by index
/// * set by index
/// * [] operator
/// * len
/// * push
#[derive(Debug)]
pub struct CompressedVersionsStore {
    lower_bytes: Vec<u32>,
    upper_bytes: HashMap<u32, u32>,
}

impl CompressedVersionsStore {
    fn version_from_parts(lower: u32, upper: u32) -> SeqNumberType {
        u64::from(upper) << u32::BITS | u64::from(lower)
    }

    fn version_to_parts(value: SeqNumberType) -> (u32, u32) {
        let lower = value as u32;
        let upper = (value >> u32::BITS) as u32;
        (lower, upper)
    }

    pub fn get(&self, index: usize) -> Option<SeqNumberType> {
        self.lower_bytes.get(index).map(|&lower| {
            let upper = *self.upper_bytes.get(&(index as u32)).unwrap_or(&0);
            Self::version_from_parts(lower, upper)
        })
    }

    pub fn set(&mut self, index: usize, value: SeqNumberType) {
        let (lower, upper) = Self::version_to_parts(value);

        self.lower_bytes[index] = lower;
        if upper > 0 {
            self.upper_bytes.insert(index as u32, upper);
        } else {
            self.upper_bytes.remove(&(index as u32));
        }
    }

    pub fn len(&self) -> usize {
        self.lower_bytes.len()
    }

    pub fn from_slice(slice: &[SeqNumberType]) -> Self {
        let mut lower_bytes = Vec::with_capacity(slice.len());
        let mut upper_bytes = HashMap::new();

        for (index, &value) in slice.iter().enumerate() {
            let (lower, upper) = Self::version_to_parts(value);

            lower_bytes.push(lower);
            if upper > 0 {
                upper_bytes.insert(index as u32, upper);
            }
        }

        Self {
            lower_bytes,
            upper_bytes,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use proptest::prelude::*;
    use rand::Rng;

    use super::*;
    use crate::types::SeqNumberType;

    fn model_test_range() -> Range<SeqNumberType> {
        0..SeqNumberType::max_value()
    }

    proptest! {
        #[test]
        fn compare_with_vec_model(
            mut model in prop::collection::vec(model_test_range(), 0..1000)
        ) {
            let mut compressed = CompressedVersionsStore::from_slice(&model);

            // Check get()
            for i in 0..model.len() {
                assert_eq!(model[i], compressed.get(i).unwrap());
            }

            // Check set()
            let mut rng = rand::rng();
            for i in 0..model.len() {
                let new_value = rng.random_range(model_test_range());
                model[i] = new_value;
                compressed.set(i, new_value);
                assert_eq!(model[i], compressed.get(i).unwrap());
            }

            // Check len()
            assert_eq!(model.len(), compressed.len());
        }
    }
}
