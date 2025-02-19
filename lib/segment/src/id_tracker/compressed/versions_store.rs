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

const U32_BITS: u32 = 32;

impl CompressedVersionsStore {
    fn version_from_parts(lower: u32, upper: u32) -> SeqNumberType {
        u64::from(upper) << U32_BITS | u64::from(lower)
    }

    fn version_to_parts(value: SeqNumberType) -> (u32, u32) {
        let lower = value as u32;
        let upper = (value >> U32_BITS) as u32;
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
