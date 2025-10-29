use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use bitvec::prelude::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use fs_err as fs;
use memory::madvise::AdviceSetting;

use crate::common::Flusher;
use crate::common::flags::bitvec_flags::BitvecFlags;
use crate::common::flags::dynamic_mmap_flags::DynamicMmapFlags;
use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::data_types::named_vectors::{CowMultiVector, CowVector};
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{TypedMultiDenseVectorRef, VectorElementType, VectorRef};
use crate::types::{Distance, MultiVectorConfig, VectorStorageDatatype};
use crate::vector_storage::chunked_mmap_vectors::ChunkedMmapVectors;
use crate::vector_storage::chunked_vector_storage::{ChunkedVectorStorage, VectorOffsetType};
use crate::vector_storage::in_ram_persisted_vectors::InRamPersistedVectors;
use crate::vector_storage::{
    AccessPattern, MultiVectorStorage, Random, Sequential, VectorStorage, VectorStorageEnum,
};

const VECTORS_DIR_PATH: &str = "vectors";
const OFFSETS_DIR_PATH: &str = "offsets";
const DELETED_DIR_PATH: &str = "deleted";

#[derive(Clone, Copy, Debug, Default, PartialEq)]
#[repr(C)]
pub struct MultivectorMmapOffset {
    offset: u32,
    count: u32,
    capacity: u32,
}

#[derive(Debug)]
pub struct AppendableMmapMultiDenseVectorStorage<
    T: PrimitiveVectorElement,
    S: ChunkedVectorStorage<T>,
    O: ChunkedVectorStorage<MultivectorMmapOffset>,
> {
    vectors: S,
    offsets: O,
    /// Flags marking deleted vectors
    ///
    /// Structure grows dynamically, but may be smaller than actual number of vectors. Must not
    /// depend on its length.
    deleted: BitvecFlags,
    distance: Distance,
    multi_vector_config: MultiVectorConfig,
    deleted_count: usize,
    _phantom: std::marker::PhantomData<T>,
}

impl<
    T: PrimitiveVectorElement,
    S: ChunkedVectorStorage<T>,
    O: ChunkedVectorStorage<MultivectorMmapOffset>,
> AppendableMmapMultiDenseVectorStorage<T, S, O>
{
    /// Set deleted flag for given key. Returns previous deleted state.
    #[inline]
    fn set_deleted(&mut self, key: PointOffsetType, deleted: bool) -> bool {
        if !deleted && self.vectors.len() <= key as usize {
            return false;
        }

        // set value
        let previous = self.deleted.set(key, deleted);

        // update counter
        if !previous && deleted {
            self.deleted_count += 1;
        } else if previous && !deleted {
            self.deleted_count -= 1;
        }

        previous
    }

    /// Populate all pages in the mmap.
    /// Block until all pages are populated.
    pub fn populate(&self) -> OperationResult<()> {
        // deleted bitvec is already loaded
        self.vectors.populate()?;
        self.offsets.populate()?;
        Ok(())
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        self.vectors.clear_cache()?;
        self.offsets.clear_cache()?;
        Ok(())
    }
}

impl<
    T: PrimitiveVectorElement,
    S: ChunkedVectorStorage<T> + Sync,
    O: ChunkedVectorStorage<MultivectorMmapOffset> + Sync,
> MultiVectorStorage<T> for AppendableMmapMultiDenseVectorStorage<T, S, O>
{
    fn vector_dim(&self) -> usize {
        self.vectors.dim()
    }

    /// Panics if key is not found
    fn get_multi<P: AccessPattern>(&self, key: PointOffsetType) -> TypedMultiDenseVectorRef<'_, T> {
        self.get_multi_opt::<P>(key).expect("vector not found")
    }

    /// Returns None if key is not found
    fn get_multi_opt<P: AccessPattern>(
        &self,
        key: PointOffsetType,
    ) -> Option<TypedMultiDenseVectorRef<'_, T>> {
        self.offsets
            .get::<P>(key as VectorOffsetType)
            .and_then(|mmap_offset| {
                let mmap_offset = mmap_offset.first().expect("mmap_offset must not be empty");
                self.vectors.get_many::<P>(
                    mmap_offset.offset as VectorOffsetType,
                    mmap_offset.count as usize,
                )
            })
            .map(|flattened_vectors| TypedMultiDenseVectorRef {
                flattened_vectors,
                dim: self.vectors.dim(),
            })
    }

    fn iterate_inner_vectors(&self) -> impl Iterator<Item = &[T]> + Clone + Send {
        (0..self.total_vector_count()).flat_map(|key| {
            let mmap_offset = self
                .offsets
                .get::<Sequential>(key as VectorOffsetType)
                .unwrap()
                .first()
                .unwrap();
            (0..mmap_offset.count).map(|i| {
                self.vectors
                    .get::<Sequential>((mmap_offset.offset + i) as VectorOffsetType)
                    .unwrap()
            })
        })
    }

    fn multi_vector_config(&self) -> &MultiVectorConfig {
        &self.multi_vector_config
    }

    fn size_of_available_vectors_in_bytes(&self) -> usize {
        if self.total_vector_count() > 0 {
            let total_size = self.vectors.len() * self.vector_dim() * std::mem::size_of::<T>();
            (total_size as u128 * self.available_vector_count() as u128
                / self.total_vector_count() as u128) as usize
        } else {
            0
        }
    }
}

impl<
    T: PrimitiveVectorElement,
    S: ChunkedVectorStorage<T> + Sync,
    O: ChunkedVectorStorage<MultivectorMmapOffset> + Sync,
> VectorStorage for AppendableMmapMultiDenseVectorStorage<T, S, O>
{
    fn distance(&self) -> Distance {
        self.distance
    }

    fn datatype(&self) -> VectorStorageDatatype {
        T::datatype()
    }

    fn is_on_disk(&self) -> bool {
        self.vectors.is_on_disk()
    }

    fn total_vector_count(&self) -> usize {
        self.offsets.len()
    }

    fn get_vector<P: AccessPattern>(&self, key: PointOffsetType) -> CowVector<'_> {
        self.get_vector_opt::<P>(key).expect("vector not found")
    }

    fn get_vector_opt<P: AccessPattern>(&self, key: PointOffsetType) -> Option<CowVector<'_>> {
        self.get_multi_opt::<P>(key).map(|multi_dense_vector| {
            CowVector::MultiDense(T::into_float_multivector(CowMultiVector::Borrowed(
                multi_dense_vector,
            )))
        })
    }

    fn insert_vector(
        &mut self,
        key: PointOffsetType,
        vector: VectorRef,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let multi_vector: TypedMultiDenseVectorRef<VectorElementType> = vector.try_into()?;
        let multi_vector = T::from_float_multivector(CowMultiVector::Borrowed(multi_vector));
        let multi_vector = multi_vector.as_vec_ref();
        assert_eq!(multi_vector.dim, self.vectors.dim());
        let multivector_size_in_bytes = std::mem::size_of_val(multi_vector.flattened_vectors);
        let max_vector_size_bytes = self.vectors.max_vector_size_bytes();
        if multivector_size_in_bytes >= max_vector_size_bytes {
            return Err(OperationError::service_error(format!(
                "Cannot insert multi vector of size {multivector_size_in_bytes} to the mmap vector storage.\
                 It's too large, maximum size is {max_vector_size_bytes}."
            )));
        }

        let mut offset = self
            .offsets
            .get::<Random>(key as VectorOffsetType)
            .map(|x| x.first().copied().unwrap_or_default())
            .unwrap_or_default();

        if multi_vector.vectors_count() > offset.capacity as usize {
            // append vector to the end
            let mut new_key = self.vectors.len();
            let chunk_left_keys = self.vectors.get_remaining_chunk_keys(new_key);
            if multi_vector.vectors_count() > chunk_left_keys {
                new_key += chunk_left_keys;
            }

            offset = MultivectorMmapOffset {
                offset: new_key as PointOffsetType,
                count: multi_vector.vectors_count() as PointOffsetType,
                capacity: multi_vector.vectors_count() as PointOffsetType,
            };
        } else {
            // use existing place to insert vector
            offset.count = multi_vector.vectors_count() as PointOffsetType;
        }

        self.vectors.insert_many(
            offset.offset as VectorOffsetType,
            multi_vector.flattened_vectors,
            multi_vector.vectors_count(),
            hw_counter,
        )?;
        self.offsets
            .insert(key as VectorOffsetType, &[offset], hw_counter)?;
        self.set_deleted(key, false);

        Ok(())
    }

    fn update_from<'a>(
        &mut self,
        other_vectors: &'a mut impl Iterator<Item = (CowVector<'a>, bool)>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        let start_index = self.offsets.len() as PointOffsetType;
        let disposed_hw_counter = HardwareCounterCell::disposable(); // Internal operation
        for (other_vector, other_deleted) in other_vectors {
            check_process_stopped(stopped)?;
            // Do not perform preprocessing - vectors should be already processed
            let other_vector: VectorRef = other_vector.as_vec_ref();
            let new_id = self.offsets.len() as PointOffsetType;
            self.insert_vector(new_id, other_vector, &disposed_hw_counter)?;
            self.set_deleted(new_id, other_deleted);
        }
        let end_index = self.offsets.len() as PointOffsetType;
        Ok(start_index..end_index)
    }

    fn flusher(&self) -> Flusher {
        Box::new({
            let vectors_flusher = self.vectors.flusher();
            let offsets_flusher = self.offsets.flusher();
            let deleted_flusher = self.deleted.flusher();
            move || {
                vectors_flusher()?;
                offsets_flusher()?;
                deleted_flusher()?;
                Ok(())
            }
        })
    }

    fn files(&self) -> Vec<PathBuf> {
        let mut files = self.vectors.files();
        files.extend(self.offsets.files());
        files.extend(self.deleted.files());
        files
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        let mut files = self.vectors.immutable_files();
        files.extend(self.offsets.immutable_files());
        files
    }

    fn delete_vector(&mut self, key: PointOffsetType) -> OperationResult<bool> {
        Ok(self.set_deleted(key, true))
    }

    fn is_deleted_vector(&self, key: PointOffsetType) -> bool {
        self.deleted.get(key)
    }

    fn deleted_vector_count(&self) -> usize {
        self.deleted_count
    }

    fn deleted_vector_bitslice(&self) -> &BitSlice {
        self.deleted.get_bitslice()
    }
}

pub fn open_appendable_memmap_multi_vector_storage(
    storage_element_type: VectorStorageDatatype,
    path: &Path,
    dim: usize,
    distance: Distance,
    multi_vector_config: MultiVectorConfig,
) -> OperationResult<VectorStorageEnum> {
    match storage_element_type {
        VectorStorageDatatype::Float32 => open_appendable_memmap_multi_vector_storage_full(
            path,
            dim,
            distance,
            multi_vector_config,
        ),
        VectorStorageDatatype::Uint8 => open_appendable_memmap_multi_vector_storage_byte(
            path,
            dim,
            distance,
            multi_vector_config,
        ),
        VectorStorageDatatype::Float16 => open_appendable_memmap_multi_vector_storage_half(
            path,
            dim,
            distance,
            multi_vector_config,
        ),
    }
}

pub fn open_appendable_memmap_multi_vector_storage_full(
    path: &Path,
    dim: usize,
    distance: Distance,
    multi_vector_config: MultiVectorConfig,
) -> OperationResult<VectorStorageEnum> {
    let storage = open_appendable_memmap_multi_vector_storage_impl::<VectorElementType>(
        path,
        dim,
        distance,
        multi_vector_config,
    )?;

    Ok(VectorStorageEnum::MultiDenseAppendableMemmap(Box::new(
        storage,
    )))
}

pub fn open_appendable_memmap_multi_vector_storage_byte(
    path: &Path,
    dim: usize,
    distance: Distance,
    multi_vector_config: MultiVectorConfig,
) -> OperationResult<VectorStorageEnum> {
    let storage =
        open_appendable_memmap_multi_vector_storage_impl(path, dim, distance, multi_vector_config)?;

    Ok(VectorStorageEnum::MultiDenseAppendableMemmapByte(Box::new(
        storage,
    )))
}

pub fn open_appendable_memmap_multi_vector_storage_half(
    path: &Path,
    dim: usize,
    distance: Distance,
    multi_vector_config: MultiVectorConfig,
) -> OperationResult<VectorStorageEnum> {
    let storage =
        open_appendable_memmap_multi_vector_storage_impl(path, dim, distance, multi_vector_config)?;

    Ok(VectorStorageEnum::MultiDenseAppendableMemmapHalf(Box::new(
        storage,
    )))
}

pub fn open_appendable_memmap_multi_vector_storage_impl<T: PrimitiveVectorElement>(
    path: &Path,
    dim: usize,
    distance: Distance,
    multi_vector_config: MultiVectorConfig,
) -> OperationResult<
    AppendableMmapMultiDenseVectorStorage<
        T,
        ChunkedMmapVectors<T>,
        ChunkedMmapVectors<MultivectorMmapOffset>,
    >,
> {
    fs::create_dir_all(path)?;

    let vectors_path = path.join(VECTORS_DIR_PATH);
    let offsets_path = path.join(OFFSETS_DIR_PATH);
    let deleted_path = path.join(DELETED_DIR_PATH);

    let populate = false;

    let vectors =
        ChunkedMmapVectors::open(&vectors_path, dim, AdviceSetting::Global, Some(populate))?;
    let offsets =
        ChunkedMmapVectors::open(&offsets_path, 1, AdviceSetting::Global, Some(populate))?;

    let deleted = BitvecFlags::new(DynamicMmapFlags::open(&deleted_path, populate)?);
    let deleted_count = deleted.count_trues();

    Ok(AppendableMmapMultiDenseVectorStorage {
        vectors,
        offsets,
        deleted,
        distance,
        multi_vector_config,
        deleted_count,
        _phantom: Default::default(),
    })
}

pub fn open_appendable_in_ram_multi_vector_storage(
    storage_element_type: VectorStorageDatatype,
    path: &Path,
    dim: usize,
    distance: Distance,
    multi_vector_config: MultiVectorConfig,
) -> OperationResult<VectorStorageEnum> {
    match storage_element_type {
        VectorStorageDatatype::Float32 => open_appendable_in_ram_multi_vector_storage_full(
            path,
            dim,
            distance,
            multi_vector_config,
        ),
        VectorStorageDatatype::Float16 => open_appendable_in_ram_multi_vector_storage_half(
            path,
            dim,
            distance,
            multi_vector_config,
        ),
        VectorStorageDatatype::Uint8 => open_appendable_in_ram_multi_vector_storage_byte(
            path,
            dim,
            distance,
            multi_vector_config,
        ),
    }
}

pub fn open_appendable_in_ram_multi_vector_storage_full(
    path: &Path,
    dim: usize,
    distance: Distance,
    multi_vector_config: MultiVectorConfig,
) -> OperationResult<VectorStorageEnum> {
    let storage = open_appendable_in_ram_multi_vector_storage_impl::<VectorElementType>(
        path,
        dim,
        distance,
        multi_vector_config,
    )?;

    Ok(VectorStorageEnum::MultiDenseAppendableInRam(Box::new(
        storage,
    )))
}

pub fn open_appendable_in_ram_multi_vector_storage_byte(
    path: &Path,
    dim: usize,
    distance: Distance,
    multi_vector_config: MultiVectorConfig,
) -> OperationResult<VectorStorageEnum> {
    let storage =
        open_appendable_in_ram_multi_vector_storage_impl(path, dim, distance, multi_vector_config)?;

    Ok(VectorStorageEnum::MultiDenseAppendableInRamByte(Box::new(
        storage,
    )))
}

pub fn open_appendable_in_ram_multi_vector_storage_half(
    path: &Path,
    dim: usize,
    distance: Distance,
    multi_vector_config: MultiVectorConfig,
) -> OperationResult<VectorStorageEnum> {
    let storage =
        open_appendable_in_ram_multi_vector_storage_impl(path, dim, distance, multi_vector_config)?;

    Ok(VectorStorageEnum::MultiDenseAppendableInRamHalf(Box::new(
        storage,
    )))
}

pub fn open_appendable_in_ram_multi_vector_storage_impl<T: PrimitiveVectorElement>(
    path: &Path,
    dim: usize,
    distance: Distance,
    multi_vector_config: MultiVectorConfig,
) -> OperationResult<
    AppendableMmapMultiDenseVectorStorage<
        T,
        InRamPersistedVectors<T>,
        InRamPersistedVectors<MultivectorMmapOffset>,
    >,
> {
    fs::create_dir_all(path)?;

    let vectors_path = path.join(VECTORS_DIR_PATH);
    let offsets_path = path.join(OFFSETS_DIR_PATH);
    let deleted_path = path.join(DELETED_DIR_PATH);

    let populate = true;

    let vectors = InRamPersistedVectors::open(&vectors_path, dim)?;
    let offsets = InRamPersistedVectors::open(&offsets_path, 1)?;

    let deleted = BitvecFlags::new(DynamicMmapFlags::open(&deleted_path, populate)?);
    let deleted_count = deleted.count_trues();

    Ok(AppendableMmapMultiDenseVectorStorage {
        vectors,
        offsets,
        deleted,
        distance,
        multi_vector_config,
        deleted_count,
        _phantom: Default::default(),
    })
}

/// Find files related to this dense vector storage
#[cfg(any(test, feature = "rocksdb"))]
pub(crate) fn find_storage_files(vector_storage_path: &Path) -> OperationResult<Vec<PathBuf>> {
    let vectors_path = vector_storage_path.join(VECTORS_DIR_PATH);
    let offsets_path = vector_storage_path.join(OFFSETS_DIR_PATH);
    let deleted_path = vector_storage_path.join(DELETED_DIR_PATH);

    let mut files = vec![];
    files.extend(common::disk::list_files(&vectors_path)?);
    files.extend(common::disk::list_files(&offsets_path)?);
    files.extend(common::disk::list_files(&deleted_path)?);
    Ok(files)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use tempfile::Builder;

    use super::*;
    use crate::data_types::vectors::MultiDenseVectorInternal;

    const RAND_SEED: u64 = 42;

    /// Test that `find_storage_files` finds all files that are reported by the storage.
    #[test]
    fn test_find_storage_files() {
        // Numbers chosen so we get 3 data chunks, not just 1
        const POINT_COUNT: PointOffsetType = 1000;
        const DIM: usize = 128;

        let mutli_vector_config = MultiVectorConfig::default();
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let mut storage = open_appendable_memmap_multi_vector_storage_full(
            dir.path(),
            DIM,
            Distance::Dot,
            mutli_vector_config,
        )
        .unwrap();

        let mut rng = StdRng::seed_from_u64(RAND_SEED);
        let hw_counter = HardwareCounterCell::disposable();

        // Insert points, delete 10% of it, and flush
        for internal_id in 0..POINT_COUNT {
            let size = rng.random_range(1..=4);
            let vectors = std::iter::repeat_with(|| {
                std::iter::repeat_with(|| rng.random_range(-1.0..1.0))
                    .take(DIM)
                    .collect()
            })
            .take(size)
            .collect::<Vec<Vec<_>>>();
            let multivec = MultiDenseVectorInternal::try_from(vectors).unwrap();
            storage
                .insert_vector(internal_id, VectorRef::from(&multivec), &hw_counter)
                .unwrap();
        }
        for internal_id in 0..POINT_COUNT {
            if !rng.random_bool(0.1) {
                continue;
            }
            storage.delete_vector(internal_id).unwrap();
        }
        storage.flusher()().unwrap();

        let storage_files = storage.files().into_iter().collect::<HashSet<_>>();
        let found_files = find_storage_files(dir.path())
            .unwrap()
            .into_iter()
            .collect::<HashSet<_>>();

        assert_eq!(
            storage_files, found_files,
            "find_storage_files must find same files that storage reports",
        );
    }
}
