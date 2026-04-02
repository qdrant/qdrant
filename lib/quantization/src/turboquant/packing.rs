//! Bit-packing and unpacking for sub-byte centroid indices.
//!
//! For b-bit quantization, multiple indices are packed per byte:
//!   4-bit: 2 per byte (nibble packing)
//!   3-bit: 8 per 3 bytes (24-bit groups)
//!   2-bit: 4 per byte
//!   1-bit: 8 per byte

/// Compute the packed byte length for `padded_dim` indices at `bits` per index.
pub fn packed_len(padded_dim: usize, bits: u8) -> usize {
    match bits {
        4 => padded_dim / 2,
        3 => {
            debug_assert!(padded_dim.is_multiple_of(8));
            (padded_dim * 3) / 8
        }
        2 => padded_dim / 4,
        1 => padded_dim / 8,
        _ => panic!("unsupported bits: {bits}"),
    }
}

/// Pack `u8` centroid indices (values in `0..2^bits`) into a sub-byte packed buffer.
pub fn pack_indices(indices: &[u8], bits: u8) -> Vec<u8> {
    match bits {
        4 => {
            let packed_dim = indices.len() / 2;
            let mut out = vec![0u8; packed_dim];
            for i in 0..packed_dim {
                let even = indices[i * 2] & 0x0F;
                let odd = indices[i * 2 + 1] & 0x0F;
                out[i] = (odd << 4) | even;
            }
            out
        }
        3 => {
            assert_eq!(
                indices.len() % 8,
                0,
                "length must be divisible by 8 for 3-bit packing"
            );
            let num_groups = indices.len() / 8;
            let mut out = vec![0u8; num_groups * 3];
            for g in 0..num_groups {
                let base = g * 8;
                let mut packed_24: u32 = 0;
                for i in 0..8 {
                    packed_24 |= u32::from(indices[base + i] & 0x07) << (i * 3);
                }
                out[g * 3] = (packed_24 & 0xFF) as u8;
                out[g * 3 + 1] = ((packed_24 >> 8) & 0xFF) as u8;
                out[g * 3 + 2] = ((packed_24 >> 16) & 0xFF) as u8;
            }
            out
        }
        2 => {
            let packed_dim = indices.len() / 4;
            let mut out = vec![0u8; packed_dim];
            for (i, item) in out.iter_mut().enumerate().take(packed_dim) {
                let base = i * 4;
                *item = (indices[base] & 0x03)
                    | ((indices[base + 1] & 0x03) << 2)
                    | ((indices[base + 2] & 0x03) << 4)
                    | ((indices[base + 3] & 0x03) << 6);
            }
            out
        }
        1 => {
            let packed_dim = indices.len() / 8;
            let mut out = vec![0u8; packed_dim];
            for (i, item) in out.iter_mut().enumerate().take(packed_dim) {
                let base = i * 8;
                let mut byte = 0u8;
                for b in 0..8 {
                    byte |= (indices[base + b] & 1) << b;
                }
                *item = byte;
            }
            out
        }
        _ => panic!("unsupported bits: {bits}"),
    }
}

/// Unpack sub-byte packed indices back to individual `u8` values.
pub fn unpack_indices(packed: &[u8], bits: u8, padded_dim: usize) -> Vec<u8> {
    let mut out = vec![0u8; padded_dim];
    match bits {
        4 => {
            for (i, &byte) in packed.iter().enumerate() {
                let even = byte & 0x0F;
                let odd = (byte >> 4) & 0x0F;
                let base = i * 2;
                if base < padded_dim {
                    out[base] = even;
                }
                if base + 1 < padded_dim {
                    out[base + 1] = odd;
                }
            }
        }
        3 => {
            let num_groups = packed.len() / 3;
            for g in 0..num_groups {
                let b0 = u32::from(packed[g * 3]);
                let b1 = u32::from(packed[g * 3 + 1]);
                let b2 = u32::from(packed[g * 3 + 2]);
                let packed_24 = b0 | (b1 << 8) | (b2 << 16);
                let base = g * 8;
                for i in 0..8 {
                    if base + i < padded_dim {
                        out[base + i] = ((packed_24 >> (i * 3)) & 0x07) as u8;
                    }
                }
            }
        }
        2 => {
            for (i, &byte) in packed.iter().enumerate() {
                let base = i * 4;
                if base < padded_dim {
                    out[base] = byte & 0x03;
                }
                if base + 1 < padded_dim {
                    out[base + 1] = (byte >> 2) & 0x03;
                }
                if base + 2 < padded_dim {
                    out[base + 2] = (byte >> 4) & 0x03;
                }
                if base + 3 < padded_dim {
                    out[base + 3] = (byte >> 6) & 0x03;
                }
            }
        }
        1 => {
            for (i, &byte) in packed.iter().enumerate() {
                let base = i * 8;
                for b in 0..8 {
                    if base + b < padded_dim {
                        out[base + b] = (byte >> b) & 1;
                    }
                }
            }
        }
        _ => panic!("unsupported bits: {bits}"),
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_4bit() {
        let indices: Vec<u8> = (0..16).collect();
        let packed = pack_indices(&indices, 4);
        assert_eq!(packed.len(), packed_len(16, 4));
        let unpacked = unpack_indices(&packed, 4, 16);
        assert_eq!(indices, unpacked);
    }

    #[test]
    fn roundtrip_3bit() {
        let indices: Vec<u8> = (0..8).map(|i| i % 8).collect();
        let packed = pack_indices(&indices, 3);
        assert_eq!(packed.len(), packed_len(8, 3));
        let unpacked = unpack_indices(&packed, 3, 8);
        assert_eq!(indices, unpacked);
    }

    #[test]
    fn roundtrip_2bit() {
        let indices: Vec<u8> = (0..16).map(|i| i % 4).collect();
        let packed = pack_indices(&indices, 2);
        assert_eq!(packed.len(), packed_len(16, 2));
        let unpacked = unpack_indices(&packed, 2, 16);
        assert_eq!(indices, unpacked);
    }

    #[test]
    fn roundtrip_1bit() {
        let indices: Vec<u8> = (0..16).map(|i| i % 2).collect();
        let packed = pack_indices(&indices, 1);
        assert_eq!(packed.len(), packed_len(16, 1));
        let unpacked = unpack_indices(&packed, 1, 16);
        assert_eq!(indices, unpacked);
    }
}
