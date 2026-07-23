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

#[cfg(test)]
mod tests {
    use common::counter::hardware_counter::HardwareCounterCell;
    use common::mmap::AdviceSetting;
    use common::sorted_slice::SortedSlice;
    use common::types::PointOffsetType;
    use common::universal_io::{MmapFile, MmapFs, Populate};
    use tempfile::Builder;

    use super::*;
    use crate::common::live_reload::LiveReload;
    use crate::data_types::vectors::{MultiDenseVectorInternal, VectorElementType, VectorRef};
    use crate::vector_storage::turbo::multi_turbo::open_appendable_turbo_multi_vector_storage;
    use crate::vector_storage::{VectorStorage, VectorStorageRead};

    /// A multivector appended *after* the reader opened, and soft-deleted only
    /// on disk, must show up as deleted once `live_reload` folds in the
    /// persisted flag of the appended offset.
    #[test]
    fn live_reload_picks_up_appended_vector_deletion() {
        const DIM: usize = 8;
        let dir = Builder::new()
            .prefix("ro_multi_turbo_appended_deleted")
            .tempdir()
            .unwrap();
        let hw = HardwareCounterCell::disposable();

        let multi = |value: VectorElementType| {
            MultiDenseVectorInternal::try_from(vec![vec![value; DIM]]).unwrap()
        };

        let mut writer = open_appendable_turbo_multi_vector_storage(
            dir.path(),
            DIM,
            Distance::Dot,
            MultiVectorConfig::default(),
            false,
        )
        .unwrap();
        writer
            .insert_vector(0, VectorRef::from(&multi(1.0)), &hw)
            .unwrap();
        writer.flusher()().unwrap();

        let mut reader = ReadOnlyChunkedMultiTurboVectorStorage::<MmapFile>::open(
            &MmapFs,
            dir.path(),
            DIM,
            Distance::Dot,
            MultiVectorConfig::default(),
            AdviceSetting::Global,
            Populate::No,
        )
        .unwrap();

        writer
            .insert_vector(1, VectorRef::from(&multi(0.0)), &hw)
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
}
