//! Read-only counterparts of the writable dense TurboQuant storages.
//!
//! Opens the TurboQuant-encoded blob and deletion flags over an arbitrary
//! [`UniversalRead`] backend (mmap / cache / remote), so a read-only segment can
//! retrieve and score a `Turbo4`-typed vector storage. The quantizer is fully
//! determined by `(dim, distance)` (fixed TQDT constants), so nothing beyond the
//! encoded bytes and flags is read from disk.
//!
//! Mirroring the reference read-only dense storages, the on-disk layout is a
//! *type*, not a runtime flag — the writable layout the segment produced picks
//! the read-only type at open time:
//! - [`ReadOnlyImmutableTurboVectorStorage`] — single-file, read-only counterpart
//!   of [`TurboVectorStorageImpl`](super::TurboVectorStorageImpl), living in the
//!   nested [`immutable`] submodule,
//! - [`ReadOnlyChunkedTurboVectorStorage`] — chunked, read-only counterpart of
//!   [`AppendableMmapTurboVectorStorage`](super::AppendableMmapTurboVectorStorage).
//!
//! Scoring is reused verbatim from the writable storages via the shared
//! [`TurboScoring`](crate::vector_storage::TurboScoring) trait; the pure
//! codec/scoring logic lives in [`super::shared`](super::shared).
//!
//! The chunked implementation is split across submodules the same way the other
//! read-only vector storages are:
//! - [`lifecycle`]: `preopen` / `open` construction,
//! - [`read_ops`]: retrieval and scoring trait impls,
//! - [`live_reload`]: picking up a writer's appends and deletions.

use common::universal_io::UniversalRead;
use quantization::EncodedStorage;
use quantization::turboquant::quantization::TurboQuantizer;

use crate::common::flags::in_memory_bitvec_flags::InMemoryBitvecFlags;
use crate::types::Distance;
use crate::vector_storage::quantized::quantized_chunked_mmap_storage::QuantizedChunkedStorageRead;

mod immutable;
mod lifecycle;
mod live_reload;
mod read_ops;

pub use immutable::ReadOnlyImmutableTurboVectorStorage;

/// Read-only chunked TurboQuant dense vector storage over a [`UniversalRead`]
/// backend `S`. Read-only counterpart of the writable
/// [`AppendableMmapTurboVectorStorage`](super::AppendableMmapTurboVectorStorage).
pub struct ReadOnlyChunkedTurboVectorStorage<S: UniversalRead> {
    /// Read-only chunked encoded vector blob.
    storage: QuantizedChunkedStorageRead<S>,
    /// Quantizer used to de/quantize and score; rebuilt from `(dim, distance)`.
    quantizer: TurboQuantizer,
    /// Persisted soft-deletion flags, materialized in memory.
    deleted: InMemoryBitvecFlags,
    /// Distance used for scoring / query preprocessing.
    distance: Distance,
    /// Original (un-padded) vector dimensionality.
    dim: usize,
}

impl<S: UniversalRead> std::fmt::Debug for ReadOnlyChunkedTurboVectorStorage<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadOnlyChunkedTurboVectorStorage")
            .field("dim", &self.dim)
            .field("distance", &self.distance)
            .field("total_vector_count", &self.storage.vectors_count())
            .field("deleted_count", &self.deleted.count())
            .finish_non_exhaustive()
    }
}
