use itertools::Itertools;
use posting_list::UnsizedValue;

/// Stores a sorted list of positions, internally stored as delta-encoded integers.
#[derive(Default, Clone, PartialEq, Debug)]
pub(super) struct PositionDeltas(Vec<u32>);

impl PositionDeltas {
    /// Creates a new instance from a vector of positions.
    pub fn encode(positions: Vec<u32>) -> Option<Self> {
        if positions.is_empty() {
            return None;
        }
        let deltas = positions
            .into_iter()
            .sorted_unstable()
            .dedup()
            .scan(0, |prev, this| {
                let delta = this - *prev;
                *prev = this;
                Some(delta)
            })
            .collect();
        Some(Self(deltas))
    }

    pub fn iter_decoded(&self) -> impl Iterator<Item = u32> + '_ {
        self.0.iter().scan(0, |prev, &delta| {
            let pos = *prev + delta;
            *prev = pos;
            Some(pos)
        })
    }

    pub fn to_sorted_positions(&self) -> Vec<u32> {
        self.iter_decoded().collect()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    fn minimal_bit_width(deltas: &[u32]) -> u8 {
        let max_delta = *deltas.iter().max().unwrap_or(&0);

        // Determine appropriate bit width
        if max_delta <= u32::from(u8::MAX) {
            8
        } else if max_delta <= u32::from(u16::MAX) {
            16
        } else {
            32
        }
    }
}

impl UnsizedValue for PositionDeltas {
    fn write_len(&self) -> usize {
        let bit_width = Self::minimal_bit_width(&self.0);

        1 + // Header byte for bit width
        bit_width as usize / u8::BITS as usize * self.0.len() // number of bytes required for deltas
    }

    fn write_to(&self, dst: &mut [u8]) {
        let deltas = &self.0;
        if deltas.is_empty() {
            dst[0] = 0; // bit_width = 0 for empty
            return;
        }

        // Store bit width
        let bit_width = Self::minimal_bit_width(deltas);
        dst[0] = bit_width;

        // Store deltas
        let write_start = 1;
        match bit_width {
            8 => {
                for (i, &delta) in deltas.iter().enumerate() {
                    dst[write_start + i] = delta as u8;
                }
            }
            16 => {
                for (i, &delta) in deltas.iter().enumerate() {
                    let bytes = (delta as u16).to_le_bytes();
                    let start = write_start + i * 2;
                    dst[start..start + 2].copy_from_slice(&bytes);
                }
            }
            32 => {
                for (i, &delta) in deltas.iter().enumerate() {
                    let bytes = delta.to_le_bytes();
                    let start = write_start + i * 4;
                    dst[start..start + 4].copy_from_slice(&bytes);
                }
            }
            _ => unreachable!("Unsupported bit width"),
        }
    }

    fn from_bytes(data: &[u8]) -> Self {
        let (bit_width, src) = data.split_first().expect("Invalid data");

        let vec = match bit_width {
            8 => {
                // already u8 slice
                src.iter().map(|&x| u32::from(x)).collect()
            }
            16 => src
                .chunks_exact(2)
                .map(|bytes| u32::from_le_bytes([bytes[0], bytes[1], 0, 0]))
                .collect(),
            32 => src
                .chunks_exact(4)
                .map(|bytes| u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
                .collect(),
            _ => unreachable!("Unsupported bit width"),
        };

        Self(vec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_bytes() {
        // encoded as little-endian u16
        let data = &[16u8, 2, 0, 4, 0, 5, 0];
        let positions = PositionDeltas::from_bytes(data);
        assert_eq!(positions, PositionDeltas(vec![2, 4, 5]));
    }

    #[test]
    fn test_empty_positions() {
        let positions = PositionDeltas::encode(vec![]);
        assert!(positions.is_none());
    }

    #[test]
    fn test_single_position() {
        let positions = PositionDeltas::encode(vec![42]).unwrap();
        assert_eq!(positions.len(), 1);
        assert_eq!(positions.to_sorted_positions(), vec![42]);

        // Test serialization/deserialization
        let len = positions.write_len();
        let mut buffer = vec![0u8; len];
        positions.write_to(&mut buffer);

        let deserialized = PositionDeltas::from_bytes(&buffer);
        assert_eq!(deserialized.len(), 1);
        assert_eq!(deserialized.to_sorted_positions(), vec![42]);
    }

    #[test]
    fn test_multiple_positions() {
        let original = vec![10, 15, 20, 25, 30];
        let positions = PositionDeltas::encode(original.clone()).unwrap();
        assert_eq!(positions.len(), 5);
        assert_eq!(positions.to_sorted_positions(), original);

        // Test serialization/deserialization
        let len = positions.write_len();
        let mut buffer = vec![0u8; len];
        positions.write_to(&mut buffer);

        let deserialized = PositionDeltas::from_bytes(&buffer);
        assert_eq!(deserialized.len(), 5);
        assert_eq!(deserialized.to_sorted_positions(), original);
    }

    #[test]
    fn test_positions_with_duplicates() {
        let input = vec![5, 10, 5, 15, 10, 20];
        let expected = vec![5, 10, 15, 20];
        let positions = PositionDeltas::encode(input).unwrap();

        assert_eq!(positions.len(), 4);
        assert_eq!(positions.to_sorted_positions(), expected);

        // Test serialization/deserialization
        let len = positions.write_len();
        let mut buffer = vec![0u8; len];
        positions.write_to(&mut buffer);

        let deserialized = PositionDeltas::from_bytes(&buffer);
        assert_eq!(deserialized.len(), 4);
        assert_eq!(deserialized.to_sorted_positions(), expected);
    }

    #[test]
    fn test_unsorted_positions() {
        let input = vec![30, 10, 20, 15, 25];
        let expected = vec![10, 15, 20, 25, 30];
        let positions = PositionDeltas::encode(input).unwrap();

        assert_eq!(positions.len(), 5);
        assert_eq!(positions.to_sorted_positions(), expected);

        // Test serialization/deserialization
        let len = positions.write_len();
        let mut buffer = vec![0u8; len];
        positions.write_to(&mut buffer);

        let deserialized = PositionDeltas::from_bytes(&buffer);
        assert_eq!(deserialized.len(), 5);
        assert_eq!(deserialized.to_sorted_positions(), expected);
    }

    #[test]
    fn test_large_gaps() {
        let original = vec![1, 1000, 2000, 10000];
        let positions = PositionDeltas::encode(original.clone()).unwrap();

        // Test serialization/deserialization
        let len = positions.write_len();
        let mut buffer = vec![0u8; len];
        positions.write_to(&mut buffer);

        let deserialized = PositionDeltas::from_bytes(&buffer);
        assert_eq!(deserialized.to_sorted_positions(), original);
    }

    #[test]
    fn test_small_consecutive_positions() {
        let original = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let positions = PositionDeltas::encode(original.clone()).unwrap();

        // Test serialization/deserialization
        let len = positions.write_len();
        let mut buffer = vec![0u8; len];
        positions.write_to(&mut buffer);

        let deserialized = PositionDeltas::from_bytes(&buffer);
        assert_eq!(deserialized.to_sorted_positions(), original);
    }
}
