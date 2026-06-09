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
use crate::vector_storage::{
    RawScorer, RawScorerBuilder, raw_multi_scorer_impl, raw_scorer_impl, raw_sparse_scorer_impl,
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
            VectorStorageReadEnum::Sparse(s) => {
                raw_sparse_scorer_impl(query, s.as_ref(), hardware_counter)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use common::counter::hardware_counter::HardwareCounterCell;
    use common::generic_consts::Random;
    use common::mmap::AdviceSetting;
    use common::sorted_slice::SortedSlice;
    use common::types::PointOffsetType;
    use common::universal_io::{MmapFile, MmapFs};
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
    use crate::vector_storage::dense::appendable_dense_vector_storage::open_appendable_memmap_vector_storage_impl;
    use crate::vector_storage::dense::dense_vector_storage::open_dense_vector_storage;
    use crate::vector_storage::dense::volatile_dense_vector_storage::new_volatile_dense_vector_storage;
    use crate::vector_storage::multi_dense::appendable_mmap_multi_dense_vector_storage::open_appendable_memmap_multi_vector_storage_impl;
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
}
