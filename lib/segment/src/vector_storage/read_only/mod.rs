use std::path::Path;

use common::bitvec::BitSlice;
use common::generic_consts::AccessPattern;
use common::mmap::{Advice, AdviceSetting};
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::named_vectors::CowVector;
use crate::data_types::vectors::{VectorElementType, VectorElementTypeByte, VectorElementTypeHalf};
use crate::types::{Distance, VectorDataConfig, VectorStorageDatatype, VectorStorageType};
use crate::vector_storage::VectorStorageRead;
use crate::vector_storage::dense::dense_vector_storage::{
    DenseVectorStorageImpl, open_dense_vector_storage_impl,
};
use crate::vector_storage::dense::read_only::chunked_vector_storage::ReadOnlyChunkedDenseVectorStorage;
use crate::vector_storage::multi_dense::read_only::chunked_vector_storage::ReadOnlyChunkedMultiDenseVectorStorage;
use crate::vector_storage::sparse::read_only::sparse_vector_storage::ReadOnlySparseVectorStorage;

/// Read-only counterpart of [`super::super::VectorStorageEnum`].
///
/// Wraps each on-disk storage type with its [`super`] read-only variant.
/// Volatile, empty and test-only variants are intentionally absent.
pub enum VectorStorageReadEnum<S: UniversalRead> {
    Dense(Box<DenseVectorStorageImpl<VectorElementType, S>>),
    DenseByte(Box<DenseVectorStorageImpl<VectorElementTypeByte, S>>),
    DenseHalf(Box<DenseVectorStorageImpl<VectorElementTypeHalf, S>>),
    DenseChunked(Box<ReadOnlyChunkedDenseVectorStorage<VectorElementType, S>>),
    DenseChunkedByte(Box<ReadOnlyChunkedDenseVectorStorage<VectorElementTypeByte, S>>),
    DenseChunkedHalf(Box<ReadOnlyChunkedDenseVectorStorage<VectorElementTypeHalf, S>>),
    MultiDenseChunked(Box<ReadOnlyChunkedMultiDenseVectorStorage<VectorElementType, S>>),
    MultiDenseChunkedByte(Box<ReadOnlyChunkedMultiDenseVectorStorage<VectorElementTypeByte, S>>),
    MultiDenseChunkedHalf(Box<ReadOnlyChunkedMultiDenseVectorStorage<VectorElementTypeHalf, S>>),
    Sparse(Box<ReadOnlySparseVectorStorage<S>>),
}

impl<S: UniversalRead> VectorStorageReadEnum<S> {
    /// Open the read-only counterpart of a dense vector storage from its
    /// `VectorDataConfig`, mirroring `open_vector_storage`. Sparse storages are
    /// opened separately via `ReadOnlySparseVectorStorage::open`.
    #[allow(dead_code)] // pending: read-only segment constructor will use this
    pub fn open(
        fs: &S::Fs,
        vector_config: &VectorDataConfig,
        path: &Path,
    ) -> OperationResult<Option<Self>>
    where
        S::Fs: Clone,
    {
        let dim = vector_config.size;
        let distance = vector_config.distance;
        let datatype = vector_config.datatype.unwrap_or_default();

        let (advice, populate, chunked) = match vector_config.storage_type {
            VectorStorageType::Mmap => (AdviceSetting::Global, false, false),
            VectorStorageType::InRamMmap => (AdviceSetting::from(Advice::Normal), true, false),
            VectorStorageType::ChunkedMmap => (AdviceSetting::Global, false, true),
            VectorStorageType::InRamChunkedMmap => {
                (AdviceSetting::from(Advice::Normal), true, true)
            }
            // No on-disk data to open for these storage types: no-op.
            VectorStorageType::Memory | VectorStorageType::Empty => return Ok(None),
        };

        // Multivectors always use the appendable chunked layout.
        if vector_config.multivector_config.is_some() {
            return Ok(Some(match datatype {
                VectorStorageDatatype::Float32 => {
                    Self::MultiDenseChunked(Box::new(ReadOnlyChunkedMultiDenseVectorStorage::open(
                        fs, path, dim, distance, advice, populate,
                    )?))
                }
                VectorStorageDatatype::Uint8 => Self::MultiDenseChunkedByte(Box::new(
                    ReadOnlyChunkedMultiDenseVectorStorage::open(
                        fs, path, dim, distance, advice, populate,
                    )?,
                )),
                VectorStorageDatatype::Float16 => Self::MultiDenseChunkedHalf(Box::new(
                    ReadOnlyChunkedMultiDenseVectorStorage::open(
                        fs, path, dim, distance, advice, populate,
                    )?,
                )),
                VectorStorageDatatype::Turbo4 => {
                    return Err(OperationError::service_error(
                        "Turbo4 datatype storage is not yet supported",
                    ));
                }
            }));
        }

        // chunked-mmap is appendable; plain mmap is the immutable storage.
        Ok(Some(if chunked {
            match datatype {
                VectorStorageDatatype::Float32 => {
                    Self::DenseChunked(Box::new(ReadOnlyChunkedDenseVectorStorage::open(
                        fs, path, dim, distance, advice, populate,
                    )?))
                }
                VectorStorageDatatype::Uint8 => {
                    Self::DenseChunkedByte(Box::new(ReadOnlyChunkedDenseVectorStorage::open(
                        fs, path, dim, distance, advice, populate,
                    )?))
                }
                VectorStorageDatatype::Float16 => {
                    Self::DenseChunkedHalf(Box::new(ReadOnlyChunkedDenseVectorStorage::open(
                        fs, path, dim, distance, advice, populate,
                    )?))
                }
                VectorStorageDatatype::Turbo4 => {
                    return Err(OperationError::service_error(
                        "Turbo4 datatype storage is not yet supported",
                    ));
                }
            }
        } else {
            match datatype {
                VectorStorageDatatype::Float32 => {
                    Self::Dense(Box::new(open_dense_vector_storage_impl::<
                        VectorElementType,
                        S,
                    >(
                        fs.clone(), path, dim, distance, populate
                    )?))
                }
                VectorStorageDatatype::Uint8 => {
                    Self::DenseByte(Box::new(open_dense_vector_storage_impl::<
                        VectorElementTypeByte,
                        S,
                    >(
                        fs.clone(), path, dim, distance, populate
                    )?))
                }
                VectorStorageDatatype::Float16 => {
                    Self::DenseHalf(Box::new(open_dense_vector_storage_impl::<
                        VectorElementTypeHalf,
                        S,
                    >(
                        fs.clone(), path, dim, distance, populate
                    )?))
                }
                VectorStorageDatatype::Turbo4 => {
                    return Err(OperationError::service_error(
                        "Turbo4 datatype storage is not yet supported",
                    ));
                }
            }
        }))
    }
}

impl<S: UniversalRead> VectorStorageRead for VectorStorageReadEnum<S> {
    fn distance(&self) -> Distance {
        match self {
            VectorStorageReadEnum::Dense(s) => s.distance(),
            VectorStorageReadEnum::DenseByte(s) => s.distance(),
            VectorStorageReadEnum::DenseHalf(s) => s.distance(),
            VectorStorageReadEnum::DenseChunked(s) => s.distance(),
            VectorStorageReadEnum::DenseChunkedByte(s) => s.distance(),
            VectorStorageReadEnum::DenseChunkedHalf(s) => s.distance(),
            VectorStorageReadEnum::MultiDenseChunked(s) => s.distance(),
            VectorStorageReadEnum::MultiDenseChunkedByte(s) => s.distance(),
            VectorStorageReadEnum::MultiDenseChunkedHalf(s) => s.distance(),
            VectorStorageReadEnum::Sparse(s) => s.distance(),
        }
    }

    fn datatype(&self) -> VectorStorageDatatype {
        match self {
            VectorStorageReadEnum::Dense(s) => s.datatype(),
            VectorStorageReadEnum::DenseByte(s) => s.datatype(),
            VectorStorageReadEnum::DenseHalf(s) => s.datatype(),
            VectorStorageReadEnum::DenseChunked(s) => s.datatype(),
            VectorStorageReadEnum::DenseChunkedByte(s) => s.datatype(),
            VectorStorageReadEnum::DenseChunkedHalf(s) => s.datatype(),
            VectorStorageReadEnum::MultiDenseChunked(s) => s.datatype(),
            VectorStorageReadEnum::MultiDenseChunkedByte(s) => s.datatype(),
            VectorStorageReadEnum::MultiDenseChunkedHalf(s) => s.datatype(),
            VectorStorageReadEnum::Sparse(s) => s.datatype(),
        }
    }

    fn is_on_disk(&self) -> bool {
        match self {
            VectorStorageReadEnum::Dense(s) => s.is_on_disk(),
            VectorStorageReadEnum::DenseByte(s) => s.is_on_disk(),
            VectorStorageReadEnum::DenseHalf(s) => s.is_on_disk(),
            VectorStorageReadEnum::DenseChunked(s) => s.is_on_disk(),
            VectorStorageReadEnum::DenseChunkedByte(s) => s.is_on_disk(),
            VectorStorageReadEnum::DenseChunkedHalf(s) => s.is_on_disk(),
            VectorStorageReadEnum::MultiDenseChunked(s) => s.is_on_disk(),
            VectorStorageReadEnum::MultiDenseChunkedByte(s) => s.is_on_disk(),
            VectorStorageReadEnum::MultiDenseChunkedHalf(s) => s.is_on_disk(),
            VectorStorageReadEnum::Sparse(s) => s.is_on_disk(),
        }
    }

    fn total_vector_count(&self) -> usize {
        match self {
            VectorStorageReadEnum::Dense(s) => s.total_vector_count(),
            VectorStorageReadEnum::DenseByte(s) => s.total_vector_count(),
            VectorStorageReadEnum::DenseHalf(s) => s.total_vector_count(),
            VectorStorageReadEnum::DenseChunked(s) => s.total_vector_count(),
            VectorStorageReadEnum::DenseChunkedByte(s) => s.total_vector_count(),
            VectorStorageReadEnum::DenseChunkedHalf(s) => s.total_vector_count(),
            VectorStorageReadEnum::MultiDenseChunked(s) => s.total_vector_count(),
            VectorStorageReadEnum::MultiDenseChunkedByte(s) => s.total_vector_count(),
            VectorStorageReadEnum::MultiDenseChunkedHalf(s) => s.total_vector_count(),
            VectorStorageReadEnum::Sparse(s) => s.total_vector_count(),
        }
    }

    fn get_vector<P: AccessPattern>(&self, key: PointOffsetType) -> CowVector<'_> {
        match self {
            VectorStorageReadEnum::Dense(s) => s.get_vector::<P>(key),
            VectorStorageReadEnum::DenseByte(s) => s.get_vector::<P>(key),
            VectorStorageReadEnum::DenseHalf(s) => s.get_vector::<P>(key),
            VectorStorageReadEnum::DenseChunked(s) => s.get_vector::<P>(key),
            VectorStorageReadEnum::DenseChunkedByte(s) => s.get_vector::<P>(key),
            VectorStorageReadEnum::DenseChunkedHalf(s) => s.get_vector::<P>(key),
            VectorStorageReadEnum::MultiDenseChunked(s) => s.get_vector::<P>(key),
            VectorStorageReadEnum::MultiDenseChunkedByte(s) => s.get_vector::<P>(key),
            VectorStorageReadEnum::MultiDenseChunkedHalf(s) => s.get_vector::<P>(key),
            VectorStorageReadEnum::Sparse(s) => s.get_vector::<P>(key),
        }
    }

    fn read_vectors<P: AccessPattern, U: Copy>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        callback: impl FnMut(U, PointOffsetType, CowVector<'_>),
    ) {
        match self {
            VectorStorageReadEnum::Dense(s) => s.read_vectors::<P, U>(keys, callback),
            VectorStorageReadEnum::DenseByte(s) => s.read_vectors::<P, U>(keys, callback),
            VectorStorageReadEnum::DenseHalf(s) => s.read_vectors::<P, U>(keys, callback),
            VectorStorageReadEnum::DenseChunked(s) => s.read_vectors::<P, U>(keys, callback),
            VectorStorageReadEnum::DenseChunkedByte(s) => s.read_vectors::<P, U>(keys, callback),
            VectorStorageReadEnum::DenseChunkedHalf(s) => s.read_vectors::<P, U>(keys, callback),
            VectorStorageReadEnum::MultiDenseChunked(s) => s.read_vectors::<P, U>(keys, callback),
            VectorStorageReadEnum::MultiDenseChunkedByte(s) => {
                s.read_vectors::<P, U>(keys, callback)
            }
            VectorStorageReadEnum::MultiDenseChunkedHalf(s) => {
                s.read_vectors::<P, U>(keys, callback)
            }
            VectorStorageReadEnum::Sparse(s) => s.read_vectors::<P, U>(keys, callback),
        }
    }

    fn get_vector_opt<P: AccessPattern>(&self, key: PointOffsetType) -> Option<CowVector<'_>> {
        match self {
            VectorStorageReadEnum::Dense(s) => s.get_vector_opt::<P>(key),
            VectorStorageReadEnum::DenseByte(s) => s.get_vector_opt::<P>(key),
            VectorStorageReadEnum::DenseHalf(s) => s.get_vector_opt::<P>(key),
            VectorStorageReadEnum::DenseChunked(s) => s.get_vector_opt::<P>(key),
            VectorStorageReadEnum::DenseChunkedByte(s) => s.get_vector_opt::<P>(key),
            VectorStorageReadEnum::DenseChunkedHalf(s) => s.get_vector_opt::<P>(key),
            VectorStorageReadEnum::MultiDenseChunked(s) => s.get_vector_opt::<P>(key),
            VectorStorageReadEnum::MultiDenseChunkedByte(s) => s.get_vector_opt::<P>(key),
            VectorStorageReadEnum::MultiDenseChunkedHalf(s) => s.get_vector_opt::<P>(key),
            VectorStorageReadEnum::Sparse(s) => s.get_vector_opt::<P>(key),
        }
    }

    fn is_deleted_vector(&self, key: PointOffsetType) -> bool {
        match self {
            VectorStorageReadEnum::Dense(s) => s.is_deleted_vector(key),
            VectorStorageReadEnum::DenseByte(s) => s.is_deleted_vector(key),
            VectorStorageReadEnum::DenseHalf(s) => s.is_deleted_vector(key),
            VectorStorageReadEnum::DenseChunked(s) => s.is_deleted_vector(key),
            VectorStorageReadEnum::DenseChunkedByte(s) => s.is_deleted_vector(key),
            VectorStorageReadEnum::DenseChunkedHalf(s) => s.is_deleted_vector(key),
            VectorStorageReadEnum::MultiDenseChunked(s) => s.is_deleted_vector(key),
            VectorStorageReadEnum::MultiDenseChunkedByte(s) => s.is_deleted_vector(key),
            VectorStorageReadEnum::MultiDenseChunkedHalf(s) => s.is_deleted_vector(key),
            VectorStorageReadEnum::Sparse(s) => s.is_deleted_vector(key),
        }
    }

    fn deleted_vector_count(&self) -> usize {
        match self {
            VectorStorageReadEnum::Dense(s) => s.deleted_vector_count(),
            VectorStorageReadEnum::DenseByte(s) => s.deleted_vector_count(),
            VectorStorageReadEnum::DenseHalf(s) => s.deleted_vector_count(),
            VectorStorageReadEnum::DenseChunked(s) => s.deleted_vector_count(),
            VectorStorageReadEnum::DenseChunkedByte(s) => s.deleted_vector_count(),
            VectorStorageReadEnum::DenseChunkedHalf(s) => s.deleted_vector_count(),
            VectorStorageReadEnum::MultiDenseChunked(s) => s.deleted_vector_count(),
            VectorStorageReadEnum::MultiDenseChunkedByte(s) => s.deleted_vector_count(),
            VectorStorageReadEnum::MultiDenseChunkedHalf(s) => s.deleted_vector_count(),
            VectorStorageReadEnum::Sparse(s) => s.deleted_vector_count(),
        }
    }

    fn deleted_vector_bitslice(&self) -> &BitSlice {
        match self {
            VectorStorageReadEnum::Dense(s) => s.deleted_vector_bitslice(),
            VectorStorageReadEnum::DenseByte(s) => s.deleted_vector_bitslice(),
            VectorStorageReadEnum::DenseHalf(s) => s.deleted_vector_bitslice(),
            VectorStorageReadEnum::DenseChunked(s) => s.deleted_vector_bitslice(),
            VectorStorageReadEnum::DenseChunkedByte(s) => s.deleted_vector_bitslice(),
            VectorStorageReadEnum::DenseChunkedHalf(s) => s.deleted_vector_bitslice(),
            VectorStorageReadEnum::MultiDenseChunked(s) => s.deleted_vector_bitslice(),
            VectorStorageReadEnum::MultiDenseChunkedByte(s) => s.deleted_vector_bitslice(),
            VectorStorageReadEnum::MultiDenseChunkedHalf(s) => s.deleted_vector_bitslice(),
            VectorStorageReadEnum::Sparse(s) => s.deleted_vector_bitslice(),
        }
    }
}

#[cfg(test)]
mod tests {
    use common::counter::hardware_counter::HardwareCounterCell;
    use common::generic_consts::Random;
    use common::mmap::AdviceSetting;
    use common::universal_io::{MmapFile, MmapFs};
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};
    use tempfile::Builder;

    use super::*;
    use crate::data_types::vectors::{
        DenseVector, MultiDenseVectorInternal, TypedMultiDenseVectorRef, VectorRef,
    };
    use crate::types::{Indexes, MultiVectorConfig};
    use crate::vector_storage::VectorStorage;
    use crate::vector_storage::dense::appendable_dense_vector_storage::open_appendable_memmap_vector_storage_impl;
    use crate::vector_storage::dense::dense_vector_storage::open_dense_vector_storage;
    use crate::vector_storage::dense::volatile_dense_vector_storage::new_volatile_dense_vector_storage;
    use crate::vector_storage::multi_dense::appendable_mmap_multi_dense_vector_storage::open_appendable_memmap_multi_vector_storage_impl;

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
            let mut iter = (0..vectors.len() as PointOffsetType).map(|i| {
                (
                    staging.get_vector::<Random>(i),
                    staging.is_deleted_vector(i),
                )
            });
            storage.update_from(&mut iter, &Default::default()).unwrap();
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
}
