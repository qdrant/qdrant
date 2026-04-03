// ============================================================================
// Bit packing for quantization indices
// ============================================================================

/// Pack b-bit indices into a compact byte array (MSB-first bitstream).
pub(super) fn pack_indices(indices: &[u8], levels: usize) -> Vec<u8> {
    let total_bits = indices.len() * levels;
    let total_bytes = (total_bits + 7) / 8;
    let mut packed = vec![0u8; total_bytes];
    let mut bit_pos = 0usize;
    for &idx in indices {
        for b in (0..levels).rev() {
            let byte_idx = bit_pos / 8;
            let bit_idx = 7 - (bit_pos % 8);
            if (idx >> b) & 1 != 0 {
                packed[byte_idx] |= 1 << bit_idx;
            }
            bit_pos += 1;
        }
    }
    packed
}

/// Unpack the index at `coord_index` from a packed bitstream.
pub(super) fn unpack_index(packed: &[u8], coord_index: usize, levels: usize) -> u8 {
    let bit_start = coord_index * levels;
    let mut result = 0u8;
    for b in 0..levels {
        let bit_pos = bit_start + b;
        let byte_idx = bit_pos / 8;
        let bit_idx = 7 - (bit_pos % 8);
        result = (result << 1) | ((packed[byte_idx] >> bit_idx) & 1);
    }
    result
}
