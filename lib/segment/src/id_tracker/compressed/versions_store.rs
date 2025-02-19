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
struct CompressedVersionsStore {
    lower_bytes: Vec<u32>,
    upper_bytes: HashMap<u32, u32>,
}

impl CompressedVersionsStore {
    pub fn new() -> Self {
        Self {
            lower_bytes: Vec::new(),
            upper_bytes: HashMap::new(),
        }
    }

    pub fn get(&self, index: usize) -> Option<SeqNumberType> {
        self.lower_bytes.get(index).map(|&lower| {
            let upper = *self.upper_bytes.get(&(index as u32)).unwrap_or(&0);
            (upper as u64) << 32 | lower as u64
        })
    }

    pub fn set(&mut self, index: usize, value: SeqNumberType) {
        let lower = value as u32;
        let upper = (value >> 32) as u32;
        if index >= self.lower_bytes.len() {
            self.lower_bytes.resize(index + 1, 0);
        }
        self.lower_bytes[index] = lower;
        if upper != 0 {
            self.upper_bytes.insert(index as u32, upper);
        } else {
            self.upper_bytes.remove(&(index as u32));
        }
    }

    pub fn len(&self) -> usize {
        self.lower_bytes.len()
    }

    pub fn push(&mut self, value: u64) {
        let lower = value as u32;
        let upper = (value >> 32) as u32;
        self.lower_bytes.push(lower);
        if upper != 0 {
            self.upper_bytes
                .insert((self.lower_bytes.len() - 1) as u32, upper);
        }
    }
}
