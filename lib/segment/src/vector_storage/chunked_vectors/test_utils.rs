//! Helpers shared by the `read_only` and `update_only` test modules.

use common::counter::hardware_counter::HardwareCounterCell;
use common::universal_io::MmapFs;

use super::update_only::UpdateOnlyChunkedVectors;
use crate::vector_storage::VectorOffsetType;

pub(super) fn make_vec(seed: usize, dim: usize) -> Vec<f32> {
    (0..dim).map(|i| (seed * dim + i) as f32).collect()
}

/// Append one durable batch of `make_vec(seed)` vectors through the writer.
pub(super) fn append_range(
    writer: &mut UpdateOnlyChunkedVectors<f32>,
    start_key: VectorOffsetType,
    seeds: std::ops::Range<usize>,
    dim: usize,
    hw: &HardwareCounterCell,
) {
    let batch: Vec<Vec<f32>> = seeds.map(|seed| make_vec(seed, dim)).collect();
    writer
        .append_many(
            &MmapFs,
            start_key,
            batch.iter().map(|vector| vector.as_slice()),
            hw,
        )
        .unwrap();
}
