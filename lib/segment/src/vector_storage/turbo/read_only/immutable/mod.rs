//! Read-only counterpart of the single-file (immutable) dense TurboQuant
//! storage.
//!
//! Vector data is immutable, so it is read straight from the single-file
//! [`QuantizedStorage`] blob; deletions are tracked in memory and folded from
//! the live-reload delta. Split across submodules the same way the other
//! read-only vector storages are: [`lifecycle`] (`preopen` / `open`),
//! [`read_ops`] (retrieval + scoring), [`live_reload`] (picking up a writer's
//! deletions).

use common::universal_io::UniversalRead;
use quantization::EncodedStorage;
use quantization::turboquant::quantization::TurboQuantizer;

use crate::common::flags::in_memory_bitvec_flags::InMemoryBitvecFlags;
use crate::types::Distance;
use crate::vector_storage::quantized::quantized_storage::QuantizedStorage;

mod lifecycle;
mod live_reload;
mod read_ops;

/// Read-only single-file TurboQuant dense vector storage over a [`UniversalRead`]
/// backend `S`. Read-only counterpart of the writable
/// [`TurboVectorStorageImpl`](crate::vector_storage::turbo::TurboVectorStorageImpl).
pub struct ReadOnlyImmutableTurboVectorStorage<S: UniversalRead> {
    /// Read-only single-file encoded vector blob.
    storage: QuantizedStorage<S>,
    /// Quantizer used to de/quantize and score; rebuilt from `(dim, distance)`.
    quantizer: TurboQuantizer,
    /// Persisted soft-deletion flags, materialized in memory.
    deleted: InMemoryBitvecFlags,
    /// Distance used for scoring / query preprocessing.
    distance: Distance,
    /// Original (un-padded) vector dimensionality.
    dim: usize,
}

impl<S: UniversalRead> std::fmt::Debug for ReadOnlyImmutableTurboVectorStorage<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadOnlyImmutableTurboVectorStorage")
            .field("dim", &self.dim)
            .field("distance", &self.distance)
            .field("total_vector_count", &self.storage.vectors_count())
            .field("deleted_count", &self.deleted.count())
            .finish_non_exhaustive()
    }
}
