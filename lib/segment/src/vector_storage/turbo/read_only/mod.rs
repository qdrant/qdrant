//! Read-only counterpart of [`TurboVectorStorage`](super::TurboVectorStorage).
//!
//! Opens the TurboQuant-encoded blob and deletion flags over an arbitrary
//! [`UniversalRead`] backend (mmap / cache / remote), so a read-only segment can
//! retrieve and score a `Turbo4`-typed vector storage. The quantizer is fully
//! determined by `(dim, distance)` (fixed TQDT constants), so nothing beyond the
//! encoded bytes and flags is read from disk.
//!
//! Scoring is reused verbatim from the writable storage via the shared
//! [`TurboScoring`](super::TurboScoring) trait — this storage only provides the
//! read surface it needs.
//!
//! The implementation is split across submodules the same way the other
//! read-only vector storages are:
//! - [`lifecycle`]: `preopen` / `open` construction,
//! - [`read_ops`]: retrieval and scoring trait impls,
//! - [`live_reload`]: picking up a writer's appends and deletions.

use std::borrow::Cow;

use common::types::PointOffsetType;
use common::universal_io::UniversalRead;
use quantization::EncodedStorage;
use quantization::turboquant::quantization::TurboQuantizer;

use crate::common::flags::in_memory_bitvec_flags::InMemoryBitvecFlags;
use crate::common::operation_error::OperationResult;
use crate::types::{Distance, MultiVectorConfig};
use crate::vector_storage::chunked_vectors::ChunkedVectorsRead;
use crate::vector_storage::multi_dense::appendable_mmap_multi_dense_vector_storage::MultivectorMmapOffset;
use crate::vector_storage::quantized::quantized_chunked_mmap_storage::QuantizedChunkedStorageRead;
use crate::vector_storage::quantized::quantized_storage::QuantizedStorage;

mod lifecycle;
mod live_reload;
mod read_ops;

/// Read-only TurboQuant encoded backend over a [`UniversalRead`] `S`.
///
/// Read-only counterpart of the writable
/// [`TurboEncodedVectorStorage`](super::turbo_encoded_vectors::TurboEncodedVectorStorage):
/// the storage layout is picked at open time from the segment's storage type.
enum ReadOnlyTurboEncoded<S: UniversalRead> {
    /// Single-file layout, written by the non-appendable `Mmap` / `InRamMmap`.
    Single(QuantizedStorage<S>),
    /// Chunked layout, written by the appendable `ChunkedMmap` / `InRamChunkedMmap`.
    Chunked(QuantizedChunkedStorageRead<S>),
}

impl<S: UniversalRead> ReadOnlyTurboEncoded<S> {
    fn get_quantized_vector(&self, key: PointOffsetType) -> Cow<'_, [u8]> {
        match self {
            Self::Single(s) => s.get_vector_data(key),
            Self::Chunked(s) => s.get_vector_data(key),
        }
    }

    fn get_quantized_vector_opt(&self, key: PointOffsetType) -> Option<Cow<'_, [u8]>> {
        match self {
            Self::Single(s) => s.get_vector_data_opt(key),
            Self::Chunked(s) => s.get_vector_data_opt(key),
        }
    }

    /// Run `f` for each vector in the batch, batching the underlying reads
    /// where the backend supports it (the single-file backend pipelines
    /// io_uring / prefetches mmap; the chunked backend batches through
    /// [`EncodedStorage::for_each_batch`]).
    fn for_each_in_batch<F: FnMut(usize, &[u8])>(
        &self,
        keys: &[PointOffsetType],
        mut f: F,
    ) -> OperationResult<()> {
        match self {
            Self::Single(s) => s.for_each_in_batch(keys, f),
            Self::Chunked(s) => {
                s.for_each_batch(keys, |idx, bytes| f(idx, &bytes));
                Ok(())
            }
        }
    }

    fn vectors_count(&self) -> usize {
        match self {
            Self::Single(s) => s.vectors_count(),
            Self::Chunked(s) => s.vectors_count(),
        }
    }

    fn is_on_disk(&self) -> bool {
        match self {
            Self::Single(s) => s.is_on_disk(),
            Self::Chunked(s) => s.is_on_disk(),
        }
    }

    fn populate(&self) -> OperationResult<()> {
        match self {
            Self::Single(s) => {
                s.populate();
                Ok(())
            }
            Self::Chunked(s) => s.populate(),
        }
    }
}

/// Read-only TurboQuant dense vector storage over a [`UniversalRead`] backend.
pub struct ReadOnlyTurboVectorStorage<S: UniversalRead> {
    /// Read-only encoded vector blob over one of the two backends.
    storage: ReadOnlyTurboEncoded<S>,
    /// Quantizer used to de/quantize and score; rebuilt from `(dim, distance)`.
    quantizer: TurboQuantizer,
    /// Persisted soft-deletion flags, materialized in memory.
    deleted: InMemoryBitvecFlags,
    /// Distance used for scoring / query preprocessing.
    distance: Distance,
    /// Original (un-padded) vector dimensionality.
    dim: usize,
}

impl<S: UniversalRead> std::fmt::Debug for ReadOnlyTurboVectorStorage<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadOnlyTurboVectorStorage")
            .field("dim", &self.dim)
            .field("distance", &self.distance)
            .field("total_vector_count", &self.storage.vectors_count())
            .field("deleted_count", &self.deleted.count())
            .finish_non_exhaustive()
    }
}

/// Read-only TurboQuant multivector vector storage over a [`UniversalRead`]
/// backend. Read-only counterpart of
/// [`TurboMultiVectorStorage`](super::multi::TurboMultiVectorStorage).
///
/// Multivectors are always stored in the appendable chunked layout, so — unlike
/// the dense storage — there is a single backend.
pub struct ReadOnlyTurboMultiVectorStorage<S: UniversalRead> {
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

impl<S: UniversalRead> std::fmt::Debug for ReadOnlyTurboMultiVectorStorage<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadOnlyTurboMultiVectorStorage")
            .field("dim", &self.dim)
            .field("distance", &self.distance)
            .field("total_vector_count", &self.offsets.len())
            .field("deleted_count", &self.deleted.count())
            .finish_non_exhaustive()
    }
}
