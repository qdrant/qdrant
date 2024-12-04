use std::fs::create_dir_all;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use bitvec::prelude::BitSlice;
use common::types::PointOffsetType;
use memory::madvise::AdviceSetting;

use crate::common::operation_error::{check_process_stopped, OperationError, OperationResult};
use crate::common::Flusher;
use crate::data_types::named_vectors::{CowMultiVector, CowVector};
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{TypedMultiDenseVectorRef, VectorElementType, VectorRef};
use crate::types::{Distance, MultiVectorConfig, VectorStorageDatatype};
use crate::vector_storage::chunked_mmap_vectors::ChunkedMmapVectors;
use crate::vector_storage::chunked_vector_storage::{ChunkedVectorStorage, VectorOffsetType};
use crate::vector_storage::common::VECTOR_READ_BATCH_SIZE;
use crate::vector_storage::dense::dynamic_mmap_flags::DynamicMmapFlags;
use crate::vector_storage::in_ram_persisted_vectors::InRamPersistedVectors;
use crate::vector_storage::{MultiVectorStorage, VectorStorage, VectorStorageEnum};

const VECTORS_DIR_PATH: &str = "vectors";
const OFFSETS_DIR_PATH: &str = "offsets";
const DELETED_DIR_PATH: &str = "deleted";

#[derive(Clone, Copy, Debug, Default, PartialEq)]
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
    deleted: DynamicMmapFlags,
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
    fn set_deleted(&mut self, key: PointOffsetType, deleted: bool) -> OperationResult<bool> {
        if !deleted && self.vectors.len() <= key as usize {
            return Ok(false);
        }

        if self.deleted.len() <= key as usize {
            self.deleted.set_len(key as usize + 1)?;
        }
        let previous = self.deleted.set(key, deleted);
        if !previous && deleted {
            self.deleted_count += 1;
        } else if previous && !deleted {
            self.deleted_count -= 1;
        }
        Ok(previous)
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
    fn get_multi(&self, key: PointOffsetType) -> TypedMultiDenseVectorRef<T> {
        self.get_multi_opt(key).expect("vector not found")
    }

    /// Returns None if key is not found
    fn get_multi_opt(&self, key: PointOffsetType) -> Option<TypedMultiDenseVectorRef<T>> {
        self.offsets
            .get(key as VectorOffsetType)
            .and_then(|mmap_offset| {
                let mmap_offset = mmap_offset.first().expect("mmap_offset must not be empty");
                self.vectors.get_many(
                    mmap_offset.offset as VectorOffsetType,
                    mmap_offset.count as usize,
                )
            })
            .map(|flattened_vectors| TypedMultiDenseVectorRef {
                flattened_vectors,
                dim: self.vectors.dim(),
            })
    }

    fn get_batch_multi<'a>(
        &'a self,
        keys: &[PointOffsetType],
        vectors: &mut [TypedMultiDenseVectorRef<'a, T>],
    ) {
        debug_assert_eq!(keys.len(), vectors.len());
        debug_assert!(keys.len() <= VECTOR_READ_BATCH_SIZE);
        for (idx, key) in keys.iter().enumerate() {
            vectors[idx] = self.get_multi(*key);
        }
    }

    fn iterate_inner_vectors(&self) -> impl Iterator<Item = &[T]> + Clone + Send {
        (0..self.total_vector_count()).flat_map(|key| {
            let mmap_offset = self
                .offsets
                .get(key as VectorOffsetType)
                .unwrap()
                .first()
                .unwrap();
            (0..mmap_offset.count).map(|i| {
                self.vectors
                    .get((mmap_offset.offset + i) as VectorOffsetType)
                    .unwrap()
            })
        })
    }

    fn multi_vector_config(&self) -> &MultiVectorConfig {
        &self.multi_vector_config
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
        true
    }

    fn total_vector_count(&self) -> usize {
        self.offsets.len()
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

    fn get_vector(&self, key: PointOffsetType) -> CowVector {
        self.get_vector_opt(key).expect("vector not found")
    }

    fn get_vector_opt(&self, key: PointOffsetType) -> Option<CowVector> {
        self.get_multi_opt(key).map(|multi_dense_vector| {
            CowVector::MultiDense(T::into_float_multivector(CowMultiVector::Borrowed(
                multi_dense_vector,
            )))
        })
    }

    fn insert_vector(&mut self, key: PointOffsetType, vector: VectorRef) -> OperationResult<()> {
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
            .get(key as VectorOffsetType)
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
        )?;
        self.offsets.insert(key as VectorOffsetType, &[offset])?;
        self.set_deleted(key, false)?;

        Ok(())
    }

    fn update_from<'a>(
        &mut self,
        other_ids: &'a mut impl Iterator<Item = (CowVector<'a>, bool)>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        let start_index = self.offsets.len() as PointOffsetType;
        for (other_vector, other_deleted) in other_ids {
            check_process_stopped(stopped)?;
            // Do not perform preprocessing - vectors should be already processed
            let other_vector: VectorRef = other_vector.as_vec_ref();
            let new_id = self.offsets.len() as PointOffsetType;
            self.insert_vector(new_id, other_vector)?;
            self.set_deleted(new_id, other_deleted)?;
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

    fn delete_vector(&mut self, key: PointOffsetType) -> OperationResult<bool> {
        self.set_deleted(key, true)
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
    create_dir_all(path)?;

    let vectors_path = path.join(VECTORS_DIR_PATH);
    let offsets_path = path.join(OFFSETS_DIR_PATH);
    let deleted_path = path.join(DELETED_DIR_PATH);

    let vectors = ChunkedMmapVectors::open(
        &vectors_path,
        dim,
        Some(false),
        AdviceSetting::Global,
        Some(false),
    )?;
    let offsets = ChunkedMmapVectors::open(
        &offsets_path,
        1,
        Some(false),
        AdviceSetting::Global,
        Some(false),
    )?;

    let deleted: DynamicMmapFlags = DynamicMmapFlags::open(&deleted_path)?;
    let deleted_count = deleted.count_flags();

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
    create_dir_all(path)?;

    let vectors_path = path.join(VECTORS_DIR_PATH);
    let offsets_path = path.join(OFFSETS_DIR_PATH);
    let deleted_path = path.join(DELETED_DIR_PATH);

    let vectors = InRamPersistedVectors::open(&vectors_path, dim)?;
    let offsets = InRamPersistedVectors::open(&offsets_path, 1)?;

    let deleted: DynamicMmapFlags = DynamicMmapFlags::open(&deleted_path)?;
    let deleted_count = deleted.count_flags();

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
