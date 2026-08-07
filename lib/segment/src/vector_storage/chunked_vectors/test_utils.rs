//! Helpers shared by the `read_only` and `update_only` test modules.

use common::counter::hardware_counter::HardwareCounterCell;
use common::universal_io::MmapFile;

use super::update_only::UpdateOnlyChunkedVectors;
use crate::vector_storage::VectorOffsetType;

pub(super) fn make_vec(seed: usize, dim: usize) -> Vec<f32> {
    (0..dim).map(|i| (seed * dim + i) as f32).collect()
}

/// Append one durable batch of `make_vec(seed)` vectors through the writer,
/// each at the offset equal to its own seed — the seed range doubles as the
/// batch's target [`VectorOffsetType`] range.
pub(super) fn append_range(
    writer: &mut UpdateOnlyChunkedVectors<f32, MmapFile>,
    seeds: std::ops::Range<usize>,
    dim: usize,
    hw: &HardwareCounterCell,
) {
    let batch: Vec<(VectorOffsetType, Vec<f32>)> = seeds
        .map(|seed| (seed as VectorOffsetType, make_vec(seed, dim)))
        .collect();
    writer
        .append_many(
            batch
                .iter()
                .map(|(offset, vector)| (*offset, vector.as_slice())),
            hw,
        )
        .unwrap();
}
