use std::borrow::Cow;
use std::fs::create_dir_all;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use bitvec::prelude::BitSlice;
use common::types::PointOffsetType;
use memory::madvise::AdviceSetting;

use crate::common::operation_error::{check_process_stopped, OperationResult};
use crate::common::Flusher;
use crate::data_types::named_vectors::CowVector;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{VectorElementType, VectorRef};
use crate::types::{Distance, VectorStorageDatatype};
use crate::vector_storage::chunked_mmap_vectors::ChunkedMmapVectors;
use crate::vector_storage::chunked_vector_storage::{ChunkedVectorStorage, VectorOffsetType};
use crate::vector_storage::common::VECTOR_READ_BATCH_SIZE;
use crate::vector_storage::dense::dynamic_mmap_flags::DynamicMmapFlags;
use crate::vector_storage::in_ram_persisted_vectors::InRamPersistedVectors;
use crate::vector_storage::{DenseVectorStorage, VectorStorage, VectorStorageEnum};

const VECTORS_DIR_PATH: &str = "vectors";
const DELETED_DIR_PATH: &str = "deleted";

#[derive(Debug)]
pub struct AppendableMmapDenseVectorStorage<T: PrimitiveVectorElement, S: ChunkedVectorStorage<T>> {
    vectors: S,
    deleted: DynamicMmapFlags,
    distance: Distance,
    deleted_count: usize,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: PrimitiveVectorElement, S: ChunkedVectorStorage<T>> AppendableMmapDenseVectorStorage<T, S> {
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

impl<T: PrimitiveVectorElement, S: ChunkedVectorStorage<T>> DenseVectorStorage<T>
    for AppendableMmapDenseVectorStorage<T, S>
{
    fn vector_dim(&self) -> usize {
        self.vectors.dim()
    }

    fn get_dense(&self, key: PointOffsetType) -> &[T] {
        self.vectors
            .get(key as VectorOffsetType)
            .expect("mmap vector not found")
    }

    fn get_dense_batch<'a>(&'a self, keys: &[PointOffsetType], vectors: &mut [&'a [T]]) {
        debug_assert!(keys.len() == vectors.len());
        debug_assert!(keys.len() <= VECTOR_READ_BATCH_SIZE);

        let mut vector_offsets = [0; VECTOR_READ_BATCH_SIZE];
        for (i, key) in keys.iter().enumerate() {
            vector_offsets[i] = *key as VectorOffsetType;
        }
        self.vectors
            .get_batch(&vector_offsets[..keys.len()], vectors);
    }
}

impl<T: PrimitiveVectorElement, S: ChunkedVectorStorage<T>> VectorStorage
    for AppendableMmapDenseVectorStorage<T, S>
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
        self.vectors.len()
    }

    fn size_of_available_vectors_in_bytes(&self) -> usize {
        self.available_vector_count() * self.vector_dim() * std::mem::size_of::<T>()
    }

    fn get_vector(&self, key: PointOffsetType) -> CowVector {
        self.get_vector_opt(key).expect("vector not found")
    }

    fn get_vector_opt(&self, key: PointOffsetType) -> Option<CowVector> {
        self.vectors
            .get(key as VectorOffsetType)
            .map(|slice| CowVector::from(T::slice_to_float_cow(slice.into())))
    }

    fn insert_vector(&mut self, key: PointOffsetType, vector: VectorRef) -> OperationResult<()> {
        let vector: &[VectorElementType] = vector.try_into()?;
        let vector = T::slice_from_float_cow(Cow::from(vector));
        self.vectors
            .insert(key as VectorOffsetType, vector.as_ref())?;
        self.set_deleted(key, false)?;
        Ok(())
    }

    fn update_from<'a>(
        &mut self,
        other_ids: &'a mut impl Iterator<Item = (CowVector<'a>, bool)>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        let start_index = self.vectors.len() as PointOffsetType;
        for (other_vector, other_deleted) in other_ids {
            check_process_stopped(stopped)?;
            // Do not perform preprocessing - vectors should be already processed
            let other_vector = T::slice_from_float_cow(Cow::try_from(other_vector)?);
            let new_id = self.vectors.push(other_vector.as_ref())?;
            self.set_deleted(new_id as PointOffsetType, other_deleted)?;
        }
        let end_index = self.vectors.len() as PointOffsetType;
        Ok(start_index..end_index)
    }

    fn flusher(&self) -> Flusher {
        Box::new({
            let vectors_flusher = self.vectors.flusher();
            let deleted_flusher = self.deleted.flusher();
            move || {
                vectors_flusher()?;
                deleted_flusher()?;
                Ok(())
            }
        })
    }

    fn files(&self) -> Vec<PathBuf> {
        let mut files = self.vectors.files();
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

pub fn open_appendable_memmap_vector_storage(
    path: &Path,
    dim: usize,
    distance: Distance,
) -> OperationResult<VectorStorageEnum> {
    let storage =
        open_appendable_memmap_vector_storage_impl::<VectorElementType>(path, dim, distance)?;

    Ok(VectorStorageEnum::DenseAppendableMemmap(Box::new(storage)))
}

pub fn open_appendable_memmap_vector_storage_byte(
    path: &Path,
    dim: usize,
    distance: Distance,
) -> OperationResult<VectorStorageEnum> {
    let storage = open_appendable_memmap_vector_storage_impl(path, dim, distance)?;

    Ok(VectorStorageEnum::DenseAppendableMemmapByte(Box::new(
        storage,
    )))
}

pub fn open_appendable_memmap_vector_storage_half(
    path: &Path,
    dim: usize,
    distance: Distance,
) -> OperationResult<VectorStorageEnum> {
    let storage = open_appendable_memmap_vector_storage_impl(path, dim, distance)?;

    Ok(VectorStorageEnum::DenseAppendableMemmapHalf(Box::new(
        storage,
    )))
}

pub fn open_appendable_memmap_vector_storage_impl<T: PrimitiveVectorElement>(
    path: &Path,
    dim: usize,
    distance: Distance,
) -> OperationResult<AppendableMmapDenseVectorStorage<T, ChunkedMmapVectors<T>>> {
    create_dir_all(path)?;

    let vectors_path = path.join(VECTORS_DIR_PATH);
    let deleted_path = path.join(DELETED_DIR_PATH);

    let vectors = ChunkedMmapVectors::<T>::open(
        &vectors_path,
        dim,
        Some(false),
        AdviceSetting::Global,
        Some(false),
    )?;

    let deleted: DynamicMmapFlags = DynamicMmapFlags::open(&deleted_path)?;
    let deleted_count = deleted.count_flags();

    Ok(AppendableMmapDenseVectorStorage {
        vectors,
        deleted,
        distance,
        deleted_count,
        _phantom: Default::default(),
    })
}

pub fn open_appendable_in_ram_vector_storage(
    path: &Path,
    dim: usize,
    distance: Distance,
) -> OperationResult<VectorStorageEnum> {
    let storage =
        open_appendable_in_ram_vector_storage_impl::<VectorElementType>(path, dim, distance)?;

    Ok(VectorStorageEnum::DenseAppendableInRam(Box::new(storage)))
}

pub fn open_appendable_in_ram_vector_storage_byte(
    path: &Path,
    dim: usize,
    distance: Distance,
) -> OperationResult<VectorStorageEnum> {
    let storage = open_appendable_in_ram_vector_storage_impl(path, dim, distance)?;

    Ok(VectorStorageEnum::DenseAppendableInRamByte(Box::new(
        storage,
    )))
}

pub fn open_appendable_in_ram_vector_storage_half(
    path: &Path,
    dim: usize,
    distance: Distance,
) -> OperationResult<VectorStorageEnum> {
    let storage = open_appendable_in_ram_vector_storage_impl(path, dim, distance)?;

    Ok(VectorStorageEnum::DenseAppendableInRamHalf(Box::new(
        storage,
    )))
}

pub fn open_appendable_in_ram_vector_storage_impl<T: PrimitiveVectorElement>(
    path: &Path,
    dim: usize,
    distance: Distance,
) -> OperationResult<AppendableMmapDenseVectorStorage<T, InRamPersistedVectors<T>>> {
    create_dir_all(path)?;

    let vectors_path = path.join(VECTORS_DIR_PATH);
    let deleted_path = path.join(DELETED_DIR_PATH);

    let vectors = InRamPersistedVectors::<T>::open(&vectors_path, dim)?;

    let deleted: DynamicMmapFlags = DynamicMmapFlags::open(&deleted_path)?;
    let deleted_count = deleted.count_flags();

    Ok(AppendableMmapDenseVectorStorage {
        vectors,
        deleted,
        distance,
        deleted_count,
        _phantom: Default::default(),
    })
}
