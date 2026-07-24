use common::counter::hardware_counter::HardwareCounterCell;
use common::universal_io::UniversalRead;

use crate::common::operation_error::OperationResult;
use crate::data_types::vectors::{
    QueryVector, VectorElementType, VectorElementTypeByte, VectorElementTypeHalf,
};
use crate::vector_storage::dense::read_only::{
    ReadOnlyChunkedDenseVectorStorage, ReadOnlyImmutableDenseVectorStorage,
};
use crate::vector_storage::multi_dense::read_only::ReadOnlyChunkedMultiDenseVectorStorage;
use crate::vector_storage::sparse::read_only::ReadOnlySparseVectorStorage;
use crate::vector_storage::turbo::multi_turbo::read_only::ReadOnlyChunkedMultiTurboVectorStorage;
use crate::vector_storage::turbo::read_only::{
    ReadOnlyChunkedTurboVectorStorage, ReadOnlyImmutableTurboVectorStorage,
};
use crate::vector_storage::{
    RawScorer, RawScorerBuilder, raw_multi_scorer_impl, raw_scorer_impl, raw_sparse_scorer_impl,
    raw_turbo_multi_scorer_impl, raw_turbo_scorer_impl,
};

mod lifecycle;
mod live_reload;
mod read_ops;

/// Read-only counterpart of [`super::super::VectorStorageEnum`].
///
/// Wraps each on-disk storage type with its [`super`] read-only variant.
/// Volatile, empty and test-only variants are intentionally absent.
pub enum VectorStorageReadEnum<S: UniversalRead> {
    Dense(Box<ReadOnlyImmutableDenseVectorStorage<VectorElementType, S>>),
    DenseByte(Box<ReadOnlyImmutableDenseVectorStorage<VectorElementTypeByte, S>>),
    DenseHalf(Box<ReadOnlyImmutableDenseVectorStorage<VectorElementTypeHalf, S>>),
    DenseChunked(Box<ReadOnlyChunkedDenseVectorStorage<VectorElementType, S>>),
    DenseChunkedByte(Box<ReadOnlyChunkedDenseVectorStorage<VectorElementTypeByte, S>>),
    DenseChunkedHalf(Box<ReadOnlyChunkedDenseVectorStorage<VectorElementTypeHalf, S>>),
    MultiDenseChunked(Box<ReadOnlyChunkedMultiDenseVectorStorage<VectorElementType, S>>),
    MultiDenseChunkedByte(Box<ReadOnlyChunkedMultiDenseVectorStorage<VectorElementTypeByte, S>>),
    MultiDenseChunkedHalf(Box<ReadOnlyChunkedMultiDenseVectorStorage<VectorElementTypeHalf, S>>),
    DenseTurbo(Box<ReadOnlyImmutableTurboVectorStorage<S>>),
    DenseTurboChunked(Box<ReadOnlyChunkedTurboVectorStorage<S>>),
    MultiDenseTurbo(Box<ReadOnlyChunkedMultiTurboVectorStorage<S>>),
    Sparse(Box<ReadOnlySparseVectorStorage<S>>),
}

impl<S: UniversalRead> RawScorerBuilder for VectorStorageReadEnum<S> {
    fn build_raw_scorer<'a>(
        &'a self,
        query: QueryVector,
        hardware_counter: HardwareCounterCell,
    ) -> OperationResult<Box<dyn RawScorer + 'a>> {
        match self {
            VectorStorageReadEnum::Dense(s) => raw_scorer_impl(query, s.as_ref(), hardware_counter),
            VectorStorageReadEnum::DenseByte(s) => {
                raw_scorer_impl(query, s.as_ref(), hardware_counter)
            }
            VectorStorageReadEnum::DenseHalf(s) => {
                raw_scorer_impl(query, s.as_ref(), hardware_counter)
            }
            VectorStorageReadEnum::DenseChunked(s) => {
                raw_scorer_impl(query, s.as_ref(), hardware_counter)
            }
            VectorStorageReadEnum::DenseChunkedByte(s) => {
                raw_scorer_impl(query, s.as_ref(), hardware_counter)
            }
            VectorStorageReadEnum::DenseChunkedHalf(s) => {
                raw_scorer_impl(query, s.as_ref(), hardware_counter)
            }
            VectorStorageReadEnum::MultiDenseChunked(s) => {
                raw_multi_scorer_impl(query, s.as_ref(), hardware_counter)
            }
            VectorStorageReadEnum::MultiDenseChunkedByte(s) => {
                raw_multi_scorer_impl(query, s.as_ref(), hardware_counter)
            }
            VectorStorageReadEnum::MultiDenseChunkedHalf(s) => {
                raw_multi_scorer_impl(query, s.as_ref(), hardware_counter)
            }
            VectorStorageReadEnum::DenseTurbo(s) => {
                raw_turbo_scorer_impl(query, s.as_ref(), hardware_counter)
            }
            VectorStorageReadEnum::DenseTurboChunked(s) => {
                raw_turbo_scorer_impl(query, s.as_ref(), hardware_counter)
            }
            VectorStorageReadEnum::MultiDenseTurbo(s) => {
                raw_turbo_multi_scorer_impl(query, s.as_ref(), hardware_counter)
            }
            VectorStorageReadEnum::Sparse(s) => {
                raw_sparse_scorer_impl(query, s.as_ref(), hardware_counter)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use common::counter::hardware_counter::HardwareCounterCell;
    use common::generic_consts::Random;
    use common::mmap::AdviceSetting;
    use common::sorted_slice::SortedSlice;
    use common::types::PointOffsetType;
    use common::universal_io::{CachedFs, CachedReadFs, MmapFile, MmapFs};
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};
    use tempfile::Builder;

    use super::*;
    use crate::common::live_reload::LiveReload;
    use crate::data_types::vectors::{
        DenseVector, MultiDenseVectorInternal, TypedMultiDenseVectorRef, VectorRef,
    };
    use crate::segment_constructor::batched_reader::merge_from_single_source;
    use crate::types::{
        Distance, Indexes, MultiVectorConfig, VectorDataConfig, VectorStorageDatatype,
        VectorStorageType,
    };
    use crate::vector_storage::dense::appendable_dense_vector_storage::{
        self, open_appendable_memmap_vector_storage_impl,
    };
    use crate::vector_storage::dense::dense_vector_storage::{self, open_dense_vector_storage};
    use crate::vector_storage::dense::volatile_dense_vector_storage::new_volatile_dense_vector_storage;
    use crate::vector_storage::multi_dense::appendable_mmap_multi_dense_vector_storage::{
        self, open_appendable_memmap_multi_vector_storage_impl,
    };
    use crate::vector_storage::{VectorStorage, VectorStorageRead};

    const DIM: usize = 16;

    fn dense_config(
        storage_type: VectorStorageType,
        multivector_config: Option<MultiVectorConfig>,
    ) -> VectorDataConfig {
        VectorDataConfig {
            size: DIM,
            distance: Distance::Dot,
            storage_type,
            index: Indexes::Plain {},
            quantization_config: None,
            multivector_config,
            datatype: Some(VectorStorageDatatype::Float32),
        }
    }

    fn rand_vec(rng: &mut StdRng) -> DenseVector {
        std::iter::repeat_with(|| rng.random_range(-1.0..1.0))
            .take(DIM)
            .collect()
    }

    /// `Memory` and `Empty` storage types are a no-op: nothing to open read-only.
    #[test]
    fn open_memory_and_empty_are_noop() {
        let dir = Builder::new().prefix("disp_noop").tempdir().unwrap();
        for storage_type in [VectorStorageType::Memory, VectorStorageType::Empty] {
            let opened = VectorStorageReadEnum::<MmapFile>::open(
                &MmapFs,
                &dense_config(storage_type, None),
                dir.path(),
                None,
            )
            .unwrap();
            assert!(opened.is_none(), "{storage_type:?} should be a no-op");
        }
    }

    /// `ChunkedMmap` single-dense routes to the chunked read-only storage.
    #[test]
    fn open_routes_chunked_mmap_to_dense_chunked() {
        let dir = Builder::new().prefix("disp_chunked").tempdir().unwrap();
        let mut rng = StdRng::seed_from_u64(1);
        let hw = HardwareCounterCell::disposable();
        let vectors: Vec<DenseVector> = (0..300).map(|_| rand_vec(&mut rng)).collect();

        {
            let mut storage = open_appendable_memmap_vector_storage_impl::<VectorElementType>(
                dir.path(),
                DIM,
                Distance::Dot,
                AdviceSetting::Global,
                false,
            )
            .unwrap();
            for (id, vector) in vectors.iter().enumerate() {
                storage
                    .insert_vector(id as PointOffsetType, VectorRef::from(vector), &hw)
                    .unwrap();
            }
            storage.flusher()().unwrap();
        }

        let storage = VectorStorageReadEnum::<MmapFile>::open(
            &MmapFs,
            &dense_config(VectorStorageType::ChunkedMmap, None),
            dir.path(),
            None,
        )
        .unwrap()
        .unwrap();

        assert!(matches!(storage, VectorStorageReadEnum::DenseChunked(_)));
        assert_eq!(storage.total_vector_count(), vectors.len());
        let got: DenseVector = storage
            .get_vector::<Random>(7)
            .to_owned()
            .try_into()
            .unwrap();
        assert_eq!(got, vectors[7]);
    }

    /// `Mmap` single-dense config routes to the immutable `DenseVectorStorageImpl`.
    #[test]
    fn open_routes_mmap_to_dense() {
        let dir = Builder::new().prefix("disp_mmap").tempdir().unwrap();
        let mut rng = StdRng::seed_from_u64(2);
        let hw = HardwareCounterCell::disposable();
        let vectors: Vec<DenseVector> = (0..3).map(|_| rand_vec(&mut rng)).collect();

        {
            // The immutable mmap storage is built by copying from another storage.
            let mut storage =
                open_dense_vector_storage(dir.path(), DIM, Distance::Dot, false).unwrap();
            let mut staging = new_volatile_dense_vector_storage(DIM, Distance::Dot);
            for (id, vector) in vectors.iter().enumerate() {
                staging
                    .insert_vector(id as PointOffsetType, VectorRef::from(vector), &hw)
                    .unwrap();
            }
            merge_from_single_source(&mut storage, &staging, vectors.len() as PointOffsetType)
                .unwrap();
            storage.flusher()().unwrap();
        }

        let storage = VectorStorageReadEnum::<MmapFile>::open(
            &MmapFs,
            &dense_config(VectorStorageType::Mmap, None),
            dir.path(),
            None,
        )
        .unwrap()
        .unwrap();

        assert!(matches!(storage, VectorStorageReadEnum::Dense(_)));
        assert_eq!(storage.total_vector_count(), vectors.len());
        let got: DenseVector = storage
            .get_vector::<Random>(1)
            .to_owned()
            .try_into()
            .unwrap();
        assert_eq!(got, vectors[1]);
    }

    /// A multivector config routes to the chunked multi-dense read-only storage.
    #[test]
    fn open_routes_multivector_to_multi_dense_chunked() {
        let dir = Builder::new().prefix("disp_multi").tempdir().unwrap();
        let mut rng = StdRng::seed_from_u64(3);
        let hw = HardwareCounterCell::disposable();
        let multis: Vec<MultiDenseVectorInternal> = (0..200)
            .map(|_| {
                let inner = rng.random_range(1..=3);
                let vectors = std::iter::repeat_with(|| rand_vec(&mut rng))
                    .take(inner)
                    .collect::<Vec<_>>();
                MultiDenseVectorInternal::try_from(vectors).unwrap()
            })
            .collect();

        {
            let mut storage =
                open_appendable_memmap_multi_vector_storage_impl::<VectorElementType>(
                    dir.path(),
                    DIM,
                    Distance::Dot,
                    MultiVectorConfig::default(),
                    AdviceSetting::Global,
                    false,
                )
                .unwrap();
            for (id, multivec) in multis.iter().enumerate() {
                storage
                    .insert_vector(id as PointOffsetType, VectorRef::from(multivec), &hw)
                    .unwrap();
            }
            storage.flusher()().unwrap();
        }

        let storage = VectorStorageReadEnum::<MmapFile>::open(
            &MmapFs,
            &dense_config(
                VectorStorageType::ChunkedMmap,
                Some(MultiVectorConfig::default()),
            ),
            dir.path(),
            None,
        )
        .unwrap()
        .unwrap();

        assert!(matches!(
            storage,
            VectorStorageReadEnum::MultiDenseChunked(_)
        ));
        assert_eq!(storage.total_vector_count(), multis.len());
        let stored = storage.get_vector::<Random>(5);
        let multi: TypedMultiDenseVectorRef<VectorElementType> =
            stored.as_vec_ref().try_into().unwrap();
        assert_eq!(multi.to_owned(), multis[5]);
    }

    /// Build a per-directory `CachedFs` with the listing snapshot taken, the
    /// way the segment open path does before `preopen`.
    fn snapshot_cached_fs(dir: &Path) -> CachedFs<MmapFs> {
        let mut cached_fs = CachedFs::new(MmapFs, dir).unwrap();
        cached_fs.cache_file_info().unwrap();
        cached_fs
    }

    /// `preopen` must schedule exactly the files `open` goes on to consume.
    ///
    /// Merely opening after a `preopen` proves nothing: `CachedFs` falls back
    /// to a plain inner open for any path that was never scheduled, so a
    /// scheduled-vs-opened path mismatch would still yield a correct storage.
    /// To make the prefetch pool the *only* possible source, the storage files
    /// are unlinked between `preopen` and `open`: the already-open handles
    /// parked in the pool stay readable, while any fallback open hits
    /// `NotFound`.
    #[test]
    fn preopen_then_open_chunked_through_cached_fs() {
        let dir = Builder::new().prefix("preopen_chunked").tempdir().unwrap();
        let mut rng = StdRng::seed_from_u64(4);
        let hw = HardwareCounterCell::disposable();
        let vectors: Vec<DenseVector> = (0..300).map(|_| rand_vec(&mut rng)).collect();

        {
            let mut storage = open_appendable_memmap_vector_storage_impl::<VectorElementType>(
                dir.path(),
                DIM,
                Distance::Dot,
                AdviceSetting::Global,
                false,
            )
            .unwrap();
            for (id, vector) in vectors.iter().enumerate() {
                storage
                    .insert_vector(id as PointOffsetType, VectorRef::from(vector), &hw)
                    .unwrap();
            }
            storage.flusher()().unwrap();
        }

        let config = dense_config(VectorStorageType::ChunkedMmap, None);
        let cached_fs = snapshot_cached_fs(dir.path());
        VectorStorageReadEnum::<MmapFile>::preopen(&cached_fs, &config, dir.path(), None).unwrap();

        // Everything `open` reads must now come from the prefetch pool.
        for dir_name in [
            appendable_dense_vector_storage::VECTORS_DIR_PATH,
            appendable_dense_vector_storage::DELETED_DIR_PATH,
        ] {
            fs_err::remove_dir_all(dir.path().join(dir_name)).unwrap();
        }

        let storage = VectorStorageReadEnum::open(&cached_fs, &config, dir.path(), None)
            .unwrap()
            .unwrap();
        assert_eq!(storage.total_vector_count(), vectors.len());
        let got: DenseVector = storage
            .get_vector::<Random>(7)
            .to_owned()
            .try_into()
            .unwrap();
        assert_eq!(got, vectors[7]);
    }

    /// [`preopen_then_open_chunked_through_cached_fs`], for the immutable
    /// (plain mmap) dense storage.
    #[test]
    fn preopen_then_open_mmap_through_cached_fs() {
        let dir = Builder::new().prefix("preopen_mmap").tempdir().unwrap();
        let mut rng = StdRng::seed_from_u64(5);
        let hw = HardwareCounterCell::disposable();
        let vectors: Vec<DenseVector> = (0..3).map(|_| rand_vec(&mut rng)).collect();

        {
            // The immutable mmap storage is built by copying from another storage.
            let mut storage =
                open_dense_vector_storage(dir.path(), DIM, Distance::Dot, false).unwrap();
            let mut staging = new_volatile_dense_vector_storage(DIM, Distance::Dot);
            for (id, vector) in vectors.iter().enumerate() {
                staging
                    .insert_vector(id as PointOffsetType, VectorRef::from(vector), &hw)
                    .unwrap();
            }
            merge_from_single_source(&mut storage, &staging, vectors.len() as PointOffsetType)
                .unwrap();
            storage.flusher()().unwrap();
        }

        let config = dense_config(VectorStorageType::Mmap, None);
        let cached_fs = snapshot_cached_fs(dir.path());
        VectorStorageReadEnum::<MmapFile>::preopen(&cached_fs, &config, dir.path(), None).unwrap();

        // Everything `open` reads must now come from the prefetch pool.
        for file_name in [
            dense_vector_storage::VECTORS_PATH,
            dense_vector_storage::DELETED_PATH,
        ] {
            fs_err::remove_file(dir.path().join(file_name)).unwrap();
        }

        let storage = VectorStorageReadEnum::open(&cached_fs, &config, dir.path(), None)
            .unwrap()
            .unwrap();
        assert_eq!(storage.total_vector_count(), vectors.len());
        let got: DenseVector = storage
            .get_vector::<Random>(1)
            .to_owned()
            .try_into()
            .unwrap();
        assert_eq!(got, vectors[1]);
    }

    /// [`preopen_then_open_chunked_through_cached_fs`], for the chunked
    /// multi-dense storage (which adds the offsets directory).
    #[test]
    fn preopen_then_open_multi_through_cached_fs() {
        let dir = Builder::new().prefix("preopen_multi").tempdir().unwrap();
        let mut rng = StdRng::seed_from_u64(6);
        let hw = HardwareCounterCell::disposable();
        let multis: Vec<MultiDenseVectorInternal> = (0..200)
            .map(|_| {
                let inner = rng.random_range(1..=3);
                let vectors = std::iter::repeat_with(|| rand_vec(&mut rng))
                    .take(inner)
                    .collect::<Vec<_>>();
                MultiDenseVectorInternal::try_from(vectors).unwrap()
            })
            .collect();

        {
            let mut storage =
                open_appendable_memmap_multi_vector_storage_impl::<VectorElementType>(
                    dir.path(),
                    DIM,
                    Distance::Dot,
                    MultiVectorConfig::default(),
                    AdviceSetting::Global,
                    false,
                )
                .unwrap();
            for (id, multivec) in multis.iter().enumerate() {
                storage
                    .insert_vector(id as PointOffsetType, VectorRef::from(multivec), &hw)
                    .unwrap();
            }
            storage.flusher()().unwrap();
        }

        let config = dense_config(
            VectorStorageType::ChunkedMmap,
            Some(MultiVectorConfig::default()),
        );
        let cached_fs = snapshot_cached_fs(dir.path());
        VectorStorageReadEnum::<MmapFile>::preopen(&cached_fs, &config, dir.path(), None).unwrap();

        // Everything `open` reads must now come from the prefetch pool.
        for dir_name in [
            appendable_mmap_multi_dense_vector_storage::VECTORS_DIR_PATH,
            appendable_mmap_multi_dense_vector_storage::OFFSETS_DIR_PATH,
            appendable_mmap_multi_dense_vector_storage::DELETED_DIR_PATH,
        ] {
            fs_err::remove_dir_all(dir.path().join(dir_name)).unwrap();
        }

        let storage = VectorStorageReadEnum::open(&cached_fs, &config, dir.path(), None)
            .unwrap()
            .unwrap();
        assert_eq!(storage.total_vector_count(), multis.len());
        let stored = storage.get_vector::<Random>(5);
        let multi: TypedMultiDenseVectorRef<VectorElementType> =
            stored.as_vec_ref().try_into().unwrap();
        assert_eq!(multi.to_owned(), multis[5]);
    }

    /// The enum dispatches `live_reload` to the active (chunked) variant.
    #[test]
    fn live_reload_dispatches_to_active_variant() {
        let dir = Builder::new().prefix("disp_reload").tempdir().unwrap();
        let mut rng = StdRng::seed_from_u64(9);
        let hw = HardwareCounterCell::disposable();
        let first: Vec<DenseVector> = (0..200).map(|_| rand_vec(&mut rng)).collect();
        let second: Vec<DenseVector> = (0..100).map(|_| rand_vec(&mut rng)).collect();

        let mut writer = open_appendable_memmap_vector_storage_impl::<VectorElementType>(
            dir.path(),
            DIM,
            Distance::Dot,
            AdviceSetting::Global,
            false,
        )
        .unwrap();
        for (id, vector) in first.iter().enumerate() {
            writer
                .insert_vector(id as PointOffsetType, VectorRef::from(vector), &hw)
                .unwrap();
        }
        writer.flusher()().unwrap();

        let mut storage = VectorStorageReadEnum::<MmapFile>::open(
            &MmapFs,
            &dense_config(VectorStorageType::ChunkedMmap, None),
            dir.path(),
            None,
        )
        .unwrap()
        .unwrap();
        assert_eq!(storage.total_vector_count(), first.len());

        for (offset, vector) in second.iter().enumerate() {
            writer
                .insert_vector(
                    (first.len() + offset) as PointOffsetType,
                    VectorRef::from(vector),
                    &hw,
                )
                .unwrap();
        }
        let deleted_ids: Vec<PointOffsetType> = vec![5, 100];
        for &id in &deleted_ids {
            writer.delete_vector(id).unwrap();
        }
        writer.flusher()().unwrap();

        let new_ids: Vec<PointOffsetType> = (first.len()..first.len() + second.len())
            .map(|offset| offset as PointOffsetType)
            .collect();
        storage
            .live_reload(
                &MmapFs,
                &SortedSlice::new(&deleted_ids).unwrap(),
                &SortedSlice::new(&new_ids).unwrap(),
                &hw,
            )
            .unwrap();

        assert_eq!(storage.total_vector_count(), first.len() + second.len());
        assert_eq!(storage.deleted_vector_count(), deleted_ids.len());
        for &id in &deleted_ids {
            assert!(storage.is_deleted_vector(id));
        }
    }

    fn turbo_config(
        storage_type: VectorStorageType,
        multivector_config: Option<MultiVectorConfig>,
    ) -> VectorDataConfig {
        VectorDataConfig {
            size: DIM,
            distance: Distance::Dot,
            storage_type,
            index: Indexes::Plain {},
            quantization_config: None,
            multivector_config,
            datatype: Some(VectorStorageDatatype::Turbo4),
        }
    }

    #[test]
    fn read_only_turbo_dense_round_trip() {
        use std::borrow::Cow;
        use std::sync::atomic::AtomicBool;

        use crate::data_types::vectors::QueryVector;
        use crate::vector_storage::turbo::{
            TurboVectorStorageImpl, open_appendable_turbo_vector_storage,
        };
        use crate::vector_storage::{DenseTQVectorStorage, VectorStorage, raw_turbo_scorer_impl};

        /// Bulk-load `encoded` into a freshly-opened writable target and flush it,
        /// so the read-only reopen below must round-trip through disk.
        fn build_target(
            target: &mut (impl DenseTQVectorStorage + VectorStorage),
            encoded: &[(Vec<u8>, bool)],
            stopped: &std::sync::atomic::AtomicBool,
        ) {
            let mut source = encoded
                .iter()
                .map(|(bytes, deleted)| (Cow::Borrowed(bytes.as_slice()), *deleted));
            target.update_from(&mut source, stopped).unwrap();
            target.flusher()().unwrap();
        }

        const COUNT: PointOffsetType = 300;

        let mut rng = StdRng::seed_from_u64(17);
        let hw = HardwareCounterCell::disposable();
        let stopped = AtomicBool::new(false);
        let vectors: Vec<DenseVector> = (0..COUNT).map(|_| rand_vec(&mut rng)).collect();
        let mut deleted_ids = Vec::new();

        let ref_dir = Builder::new().prefix("ro_turbo_ref").tempdir().unwrap();
        let mut reference =
            open_appendable_turbo_vector_storage(ref_dir.path(), DIM, Distance::Dot, false)
                .unwrap();
        for (id, vector) in vectors.iter().enumerate() {
            reference
                .insert_vector(id as PointOffsetType, VectorRef::from(vector), &hw)
                .unwrap();
        }
        for id in 0..COUNT {
            if rng.random_bool(0.1) {
                reference.delete_vector(id).unwrap();
                deleted_ids.push(id);
            }
        }
        reference.flusher()().unwrap();

        let encoded: Vec<(Vec<u8>, bool)> = (0..COUNT)
            .map(|id| {
                (
                    reference.get_quantized_vector(id).to_vec(),
                    reference.is_deleted_vector(id),
                )
            })
            .collect();

        for storage_type in [VectorStorageType::Mmap, VectorStorageType::ChunkedMmap] {
            let dir = Builder::new().prefix("ro_turbo").tempdir().unwrap();

            // The two writable backends are distinct concrete types, so each
            // builds its own target; the read-only reopen then routes to the
            // matching read-only variant.
            match storage_type {
                VectorStorageType::Mmap => {
                    let mut target = TurboVectorStorageImpl::<MmapFile>::open_mmap(
                        dir.path(),
                        DIM,
                        Distance::Dot,
                        false,
                    )
                    .unwrap();
                    build_target(&mut target, &encoded, &stopped);
                }
                VectorStorageType::ChunkedMmap => {
                    let mut target =
                        open_appendable_turbo_vector_storage(dir.path(), DIM, Distance::Dot, false)
                            .unwrap();
                    build_target(&mut target, &encoded, &stopped);
                }
                VectorStorageType::InRamMmap
                | VectorStorageType::InRamChunkedMmap
                | VectorStorageType::Memory
                | VectorStorageType::Empty => {
                    unreachable!("unexpected storage type {storage_type:?}")
                }
            }

            let ro = VectorStorageReadEnum::<MmapFile>::open(
                &MmapFs,
                &turbo_config(storage_type, None),
                dir.path(),
                None,
            )
            .unwrap()
            .unwrap();

            let routed = match storage_type {
                VectorStorageType::Mmap => matches!(ro, VectorStorageReadEnum::DenseTurbo(_)),
                VectorStorageType::ChunkedMmap => {
                    matches!(ro, VectorStorageReadEnum::DenseTurboChunked(_))
                }
                VectorStorageType::Memory
                | VectorStorageType::InRamChunkedMmap
                | VectorStorageType::InRamMmap
                | VectorStorageType::Empty => false,
            };
            assert!(
                routed,
                "{storage_type:?} should route to its dense Turbo read-only variant",
            );
            assert_eq!(ro.total_vector_count(), vectors.len());
            assert_eq!(ro.deleted_vector_count(), deleted_ids.len());
            for id in 0..COUNT {
                assert_eq!(
                    ro.is_deleted_vector(id),
                    deleted_ids.contains(&id),
                    "{storage_type:?} deleted flag @{id}",
                );
            }

            for id in [0, 7, 42, COUNT - 1] {
                let ro_vec: DenseVector =
                    ro.get_vector::<Random>(id).to_owned().try_into().unwrap();
                let ref_vec: DenseVector = reference
                    .get_vector::<Random>(id)
                    .to_owned()
                    .try_into()
                    .unwrap();
                assert_eq!(ro_vec, ref_vec, "{storage_type:?} get_vector @{id}");
            }

            ro.read_vector_bytes::<Random, _>((0..COUNT).map(|id| ((), id)), |(), id, bytes| {
                assert_eq!(
                    bytes, encoded[id as usize].0,
                    "{storage_type:?} raw bytes @{id}",
                );
            })
            .unwrap();

            let query = QueryVector::Nearest(vectors[3].clone().into());
            let ro_scorer = ro
                .build_raw_scorer(query.clone(), HardwareCounterCell::disposable())
                .unwrap();
            let ref_scorer =
                raw_turbo_scorer_impl(query, &reference, HardwareCounterCell::disposable())
                    .unwrap();
            for id in 0..COUNT {
                assert_eq!(
                    ro_scorer.score_point(id),
                    ref_scorer.score_point(id),
                    "{storage_type:?} score @{id}",
                );
            }
        }
    }

    #[test]
    fn read_only_turbo_multi_round_trip() {
        use crate::data_types::vectors::QueryVector;
        use crate::vector_storage::turbo::multi_turbo::open_appendable_turbo_multi_vector_storage;
        use crate::vector_storage::{MultiTQVectorStorageRead, raw_turbo_multi_scorer_impl};

        const COUNT: PointOffsetType = 128;
        let multivector_config = MultiVectorConfig::default();
        let dir = Builder::new().prefix("ro_turbo_multi").tempdir().unwrap();
        let mut rng = StdRng::seed_from_u64(23);
        let hw = HardwareCounterCell::disposable();

        let multis: Vec<MultiDenseVectorInternal> = (0..COUNT)
            .map(|_| {
                let n = rng.random_range(1..=3usize);
                let flattened: Vec<f32> = std::iter::repeat_with(|| rng.random_range(-1.0..1.0))
                    .take(n * DIM)
                    .collect();
                MultiDenseVectorInternal::new(flattened, DIM)
            })
            .collect();
        let mut deleted_ids = Vec::new();

        let mut writable = open_appendable_turbo_multi_vector_storage(
            dir.path(),
            DIM,
            Distance::Dot,
            multivector_config,
            false,
        )
        .unwrap();
        for (id, multi) in multis.iter().enumerate() {
            writable
                .insert_vector(
                    id as PointOffsetType,
                    TypedMultiDenseVectorRef::from(multi).into(),
                    &hw,
                )
                .unwrap();
        }
        for id in 0..COUNT {
            if rng.random_bool(0.1) {
                writable.delete_vector(id).unwrap();
                deleted_ids.push(id);
            }
        }
        writable.flusher()().unwrap();

        let ro = VectorStorageReadEnum::<MmapFile>::open(
            &MmapFs,
            &turbo_config(VectorStorageType::ChunkedMmap, Some(multivector_config)),
            dir.path(),
            None,
        )
        .unwrap()
        .unwrap();

        assert!(
            matches!(ro, VectorStorageReadEnum::MultiDenseTurbo(_)),
            "multivector Turbo4 should route to MultiDenseTurbo",
        );
        assert_eq!(ro.total_vector_count(), multis.len());
        assert_eq!(ro.deleted_vector_count(), deleted_ids.len());
        for id in 0..COUNT {
            assert_eq!(
                ro.is_deleted_vector(id),
                deleted_ids.contains(&id),
                "deleted flag @{id}",
            );
        }

        ro.read_vector_bytes::<Random, _>((0..COUNT).map(|id| ((), id)), |(), id, bytes| {
            assert_eq!(
                bytes,
                writable.get_multi_tq::<Random>(id).to_vec(),
                "raw records @{id}",
            );
        })
        .unwrap();

        let query = QueryVector::Nearest(multis[5].clone().into());
        let ro_scorer = ro
            .build_raw_scorer(query.clone(), HardwareCounterCell::disposable())
            .unwrap();
        let wr_scorer =
            raw_turbo_multi_scorer_impl(query, &writable, HardwareCounterCell::disposable())
                .unwrap();
        for id in 0..COUNT {
            assert_eq!(
                ro_scorer.score_point(id),
                wr_scorer.score_point(id),
                "score @{id}",
            );
        }
    }
}
