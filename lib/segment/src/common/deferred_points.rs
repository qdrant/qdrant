use common::types::PointOffsetType;

use crate::common::BYTES_IN_KB;
use crate::types::{SegmentConfig, VectorStorageDatatype};

/// Internal id from which points of a segment with `config` are deferred, for a deferred-points
/// threshold of `threshold_kb` (in KB, like the indexing threshold); `None` for a zero threshold
/// or a segment without dense vectors.
///
/// Mirrors the leader's conversion over the segment's dense vectors. The leader counts only
/// vectors with HNSW enabled, which a segment config doesn't record, so a large vector with HNSW
/// disabled makes the cutoff stricter here than on the leader.
pub fn segment_deferred_internal_id(
    config: &SegmentConfig,
    threshold_kb: usize,
) -> Option<PointOffsetType> {
    let threshold_bytes = threshold_kb.saturating_mul(BYTES_IN_KB);
    if threshold_bytes == 0 {
        return None;
    }
    config
        .vector_data
        .values()
        .map(|vector_config| {
            deferred_point_offset(
                threshold_bytes,
                vector_config.size,
                vector_config.datatype,
                vector_config.multivector_config.is_some(),
            )
        })
        .min()
}

/// First internal id from which an appendable segment defers points, for a deferred-points
/// threshold of `threshold_bytes` and a per-point vector of `dim` elements of `datatype`.
///
/// Multivector size can't be predicted, so a multivector counts as a fixed 16 inner vectors.
pub fn deferred_point_offset(
    threshold_bytes: usize,
    dim: usize,
    datatype: Option<VectorStorageDatatype>,
    is_multivector: bool,
) -> PointOffsetType {
    const MULTIVECTOR_SIZE: usize = 16;

    let element_bytes = match datatype {
        Some(VectorStorageDatatype::Float16) => 2,
        Some(VectorStorageDatatype::Uint8) => 1,
        // Placeholder: Turbo4 is ~0.5 byte/dim + per-row scale.
        // Mirroring Uint8 (1 byte) until accurate accounting is implemented.
        Some(VectorStorageDatatype::Turbo4) => 1,
        Some(VectorStorageDatatype::Turbo8) => 1,
        Some(VectorStorageDatatype::Turbo16) => 2,
        Some(VectorStorageDatatype::Float32) | None => 4,
    };

    let vector_bytes = if is_multivector {
        element_bytes * dim * MULTIVECTOR_SIZE
    } else {
        element_bytes * dim
    };

    let deferred_from = threshold_bytes.div_ceil(vector_bytes);
    PointOffsetType::try_from(deferred_from).unwrap_or(PointOffsetType::MAX)
}
