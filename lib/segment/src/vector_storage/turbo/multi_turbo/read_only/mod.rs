//! Read-only counterpart of the TurboQuant multivector storage.
//!
//! Multivectors are always stored in the appendable chunked layout, so — unlike
//! the dense storage — there is a single backend and no layout split. Mirrors
//! the reference [`multi_dense::read_only`](crate::vector_storage::multi_dense)
//! module: split into [`lifecycle`] (`preopen` / `open`), [`read_ops`]
//! (retrieval + scoring) and [`live_reload`].

use common::universal_io::UniversalRead;
use quantization::turboquant::quantization::TurboQuantizer;

use crate::common::flags::in_memory_bitvec_flags::InMemoryBitvecFlags;
use crate::types::{Distance, MultiVectorConfig};
use crate::vector_storage::chunked_vectors::ChunkedVectorsRead;
use crate::vector_storage::multi_dense::appendable_mmap_multi_dense_vector_storage::MultivectorMmapOffset;
use crate::vector_storage::quantized::quantized_chunked_mmap_storage::QuantizedChunkedStorageRead;

mod lifecycle;
mod live_reload;
mod read_ops;

/// Read-only TurboQuant multivector vector storage over a [`UniversalRead`]
/// backend. Read-only counterpart of
/// [`AppendableMmapMultiTurboVectorStorage`](super::AppendableMmapMultiTurboVectorStorage).
///
/// Multivectors are always stored in the appendable chunked layout, so — unlike
/// the dense storage — there is a single backend and no layout split.
pub struct ReadOnlyChunkedMultiTurboVectorStorage<S: UniversalRead> {
    /// Flat inner-vector space: one fixed-size encoded record per inner vector.
    storage: QuantizedChunkedStorageRead<S>,
    /// Maps each point to its record range in the inner space.
    offsets: ChunkedVectorsRead<MultivectorMmapOffset, S>,
    /// Quantizer used to de/quantize and score; rebuilt from `(dim, distance)`.
    quantizer: TurboQuantizer,
    /// Persisted soft-deletion flags, materialized in memory.
    deleted: InMemoryBitvecFlags,
    /// Distance used for scoring / query preprocessing.
    distance: Distance,
    /// Original (un-padded) inner vector dimensionality.
    dim: usize,
    multi_vector_config: MultiVectorConfig,
}

impl<S: UniversalRead> std::fmt::Debug for ReadOnlyChunkedMultiTurboVectorStorage<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadOnlyChunkedMultiTurboVectorStorage")
            .field("dim", &self.dim)
            .field("distance", &self.distance)
            .field("total_vector_count", &self.offsets.len())
            .field("deleted_count", &self.deleted.count())
            .finish_non_exhaustive()
    }
}
