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

#[cfg(test)]
mod tests {
    use common::counter::hardware_counter::HardwareCounterCell;
    use common::sorted_slice::SortedSlice;
    use common::types::PointOffsetType;
    use common::universal_io::{MmapFile, MmapFs, Populate};
    use tempfile::Builder;

    use super::*;
    use crate::common::live_reload::LiveReload;
    use crate::data_types::vectors::VectorRef;
    use crate::vector_storage::turbo::open_appendable_turbo_vector_storage;
    use crate::vector_storage::{VectorStorage, VectorStorageRead};

    /// A point appended *after* the reader opened, and soft-deleted only on
    /// disk, must show up as deleted once `live_reload` folds in the persisted
    /// flag of the appended offset.
    #[test]
    fn live_reload_picks_up_appended_vector_deletion() {
        const DIM: usize = 4;
        let dir = Builder::new()
            .prefix("ro_turbo_appended_deleted")
            .tempdir()
            .unwrap();
        let hw = HardwareCounterCell::disposable();

        let mut writer =
            open_appendable_turbo_vector_storage(dir.path(), DIM, Distance::Dot, false).unwrap();
        writer
            .insert_vector(0, VectorRef::from(&vec![1.0; DIM]), &hw)
            .unwrap();
        writer.flusher()().unwrap();

        let mut reader = ReadOnlyChunkedTurboVectorStorage::<MmapFile>::open(
            &MmapFs,
            dir.path(),
            DIM,
            Distance::Dot,
            Populate::No,
        )
        .unwrap();

        writer
            .insert_vector(1, VectorRef::from(&vec![0.0; DIM]), &hw)
            .unwrap();
        writer.delete_vector(1).unwrap();
        writer.flusher()().unwrap();

        let deleted_ids: Vec<PointOffsetType> = vec![];
        let new_ids: Vec<PointOffsetType> = vec![1];
        reader
            .live_reload(
                &MmapFs,
                &SortedSlice::new(&deleted_ids).unwrap(),
                &SortedSlice::new(&new_ids).unwrap(),
                &hw,
            )
            .unwrap();

        assert_eq!(reader.total_vector_count(), 2);
        assert!(reader.is_deleted_vector(1));
    }

    /// A batch of appended offsets, only some of them deleted on disk, must be
    /// reloaded exactly: each appended offset picks up its own persisted flag,
    /// and offsets present at open time stay untouched.
    #[test]
    fn live_reload_batches_appended_vector_deletions() {
        const DIM: usize = 4;
        let dir = Builder::new()
            .prefix("ro_turbo_appended_batch")
            .tempdir()
            .unwrap();
        let hw = HardwareCounterCell::disposable();

        let mut writer =
            open_appendable_turbo_vector_storage(dir.path(), DIM, Distance::Dot, false).unwrap();
        for id in 0..3u32 {
            writer
                .insert_vector(id, VectorRef::from(&vec![1.0; DIM]), &hw)
                .unwrap();
        }
        writer.flusher()().unwrap();

        let mut reader = ReadOnlyChunkedTurboVectorStorage::<MmapFile>::open(
            &MmapFs,
            dir.path(),
            DIM,
            Distance::Dot,
            Populate::No,
        )
        .unwrap();

        for id in 3..8u32 {
            writer
                .insert_vector(id, VectorRef::from(&vec![0.0; DIM]), &hw)
                .unwrap();
        }
        let deleted_appended: Vec<PointOffsetType> = vec![4, 6];
        for &id in &deleted_appended {
            writer.delete_vector(id).unwrap();
        }
        writer.flusher()().unwrap();

        let deleted_ids: Vec<PointOffsetType> = vec![];
        let new_ids: Vec<PointOffsetType> = (3..8).collect();
        reader
            .live_reload(
                &MmapFs,
                &SortedSlice::new(&deleted_ids).unwrap(),
                &SortedSlice::new(&new_ids).unwrap(),
                &hw,
            )
            .unwrap();

        assert_eq!(reader.total_vector_count(), 8);
        for id in 3..8 {
            assert_eq!(
                reader.is_deleted_vector(id),
                deleted_appended.contains(&id),
                "appended offset {id}",
            );
        }
        assert!(!reader.is_deleted_vector(0));
    }
}
