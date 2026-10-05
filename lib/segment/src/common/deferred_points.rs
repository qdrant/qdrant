use common::types::PointOffsetType;

use crate::types::VectorStorageDatatype;

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
