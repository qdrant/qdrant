use std::borrow::Cow;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use common::bitvec::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::generic_consts::AccessPattern;
use common::mmap::AdviceSetting;
use common::types::PointOffsetType;
use common::universal_io::{MmapFile, MmapFs, Populate, UserData};
use fs_err as fs;

use crate::common::Flusher;
use crate::common::flags::bitvec_flags::BitvecFlags;
use crate::common::flags::dynamic_stored_flags::DynamicStoredFlags;
use crate::common::operation_error::{OperationResult, check_process_stopped};
use crate::data_types::named_vectors::CowVector;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{VectorElementType, VectorRef};
use crate::types::{Distance, VectorStorageDatatype};
use crate::vector_storage::chunked_vectors::ChunkedVectors;
use crate::vector_storage::{
    DenseVectorStorage, DenseVectorStorageRead, VectorOffsetType, VectorStorage, VectorStorageEnum,
    VectorStorageRead,
};

pub(crate) const VECTORS_DIR_PATH: &str = "vectors";
pub(crate) const DELETED_DIR_PATH: &str = "deleted";

#[derive(Debug)]
pub struct AppendableMmapDenseVectorStorage<T: PrimitiveVectorElement> {
    vectors: ChunkedVectors<T, MmapFile>,
    /// Flags marking deleted vectors
    ///
    /// Structure grows dynamically, but may be smaller than actual number of vectors. Must not
    /// depend on its length.
    deleted: BitvecFlags<MmapFile>,
    distance: Distance,
    deleted_count: usize,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: PrimitiveVectorElement> AppendableMmapDenseVectorStorage<T> {
    /// Set deleted flag for given key. Returns previous deleted state.
    #[inline]
    fn set_deleted(&mut self, key: PointOffsetType, deleted: bool) -> bool {
        if !deleted && self.vectors.len() <= key as usize {
            return false;
        }

        // mark deletion
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
        Ok(())
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        self.deleted.clear_cache()?;
        self.vectors.clear_cache()?;
        Ok(())
    }
}

impl<T: PrimitiveVectorElement> DenseVectorStorageRead<T> for AppendableMmapDenseVectorStorage<T> {
    fn vector_dim(&self) -> usize {
        self.vectors.dim()
    }

    fn get_dense<P: AccessPattern>(&self, key: PointOffsetType) -> Cow<'_, [T]> {
        self.vectors
            .get::<P>(key as VectorOffsetType)
            .expect("mmap vector not found")
    }

    fn for_each_in_dense_batch<F>(&self, keys: &[PointOffsetType], callback: F)
    where
        F: FnMut(usize, &[T]),
    {
        self.vectors.for_each_in_batch(keys, callback);
    }
}

impl<T: PrimitiveVectorElement> DenseVectorStorage<T> for AppendableMmapDenseVectorStorage<T> {
    fn update_from<'a>(
        &mut self,
        other_vectors: &mut impl Iterator<Item = (Cow<'a, [T]>, bool)>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        let start_index = self.vectors.len() as PointOffsetType;
        let disposed_hw = HardwareCounterCell::disposable(); // This function is only used for internal operations.
        for (other_vector, other_deleted) in other_vectors {
            check_process_stopped(stopped)?;
            let new_id = self.vectors.push(other_vector.as_ref(), &disposed_hw)?;
            self.set_deleted(new_id as PointOffsetType, other_deleted);
        }
        let end_index = self.vectors.len() as PointOffsetType;
        Ok(start_index..end_index)
    }
}

impl<T: PrimitiveVectorElement> VectorStorageRead for AppendableMmapDenseVectorStorage<T> {
    fn size_of_available_vectors_in_bytes(&self) -> usize {
        self.available_vector_count() * self.vector_dim() * std::mem::size_of::<T>()
    }

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

    fn get_vector<P: AccessPattern>(&self, key: PointOffsetType) -> CowVector<'_> {
        self.vectors
            .get::<P>(key as VectorOffsetType)
            .map(|slice| CowVector::from(T::slice_to_float_cow(slice)))
            .expect("Vector not found")
    }

    fn read_vectors<P: AccessPattern, U: Copy + UserData>(
        &self,
        keys: impl IntoIterator<Item = (U, PointOffsetType)>,
        mut callback: impl FnMut(U, PointOffsetType, CowVector<'_>),
    ) {
        let keys = keys
            .into_iter()
            .map(|(user_data, point_offset)| ((user_data, point_offset), point_offset, 1));

        for ((user_data, point_offset), vector) in self.vectors.iter_vectors::<P, _>(keys) {
            let vector = CowVector::from(T::slice_to_float_cow(vector));
            callback(user_data, point_offset, vector);
        }
    }

    fn get_vector_opt<P: AccessPattern>(&self, key: PointOffsetType) -> Option<CowVector<'_>> {
        self.vectors
            .get::<P>(key as VectorOffsetType)
            .map(|slice| CowVector::from(T::slice_to_float_cow(slice)))
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

impl<T: PrimitiveVectorElement> VectorStorage for AppendableMmapDenseVectorStorage<T> {
    fn insert_vector(
        &mut self,
        key: PointOffsetType,
        vector: VectorRef,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let vector: &[VectorElementType] = vector.try_into()?;
        let vector = T::slice_from_float_cow(Cow::from(vector));
        self.vectors
            .insert(key as VectorOffsetType, vector.as_ref(), hw_counter)?;
        self.set_deleted(key, false);
        Ok(())
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

    fn immutable_files(&self) -> Vec<PathBuf> {
        self.vectors.immutable_files()
    }

    fn delete_vector(&mut self, key: PointOffsetType) -> OperationResult<bool> {
        Ok(!self.set_deleted(key, true))
    }
}

pub fn open_appendable_memmap_vector_storage_full(
    path: &Path,
    dim: usize,
    distance: Distance,
    madvise: AdviceSetting,
    populate: bool,
) -> OperationResult<VectorStorageEnum> {
    let storage = open_appendable_memmap_vector_storage_impl::<VectorElementType>(
        path, dim, distance, madvise, populate,
    )?;

    Ok(VectorStorageEnum::DenseAppendableMemmap(Box::new(storage)))
}

pub fn open_appendable_memmap_vector_storage_byte(
    path: &Path,
    dim: usize,
    distance: Distance,
    madvise: AdviceSetting,
    populate: bool,
) -> OperationResult<VectorStorageEnum> {
    let storage =
        open_appendable_memmap_vector_storage_impl(path, dim, distance, madvise, populate)?;

    Ok(VectorStorageEnum::DenseAppendableMemmapByte(Box::new(
        storage,
    )))
}

pub fn open_appendable_memmap_vector_storage_half(
    path: &Path,
    dim: usize,
    distance: Distance,
    madvise: AdviceSetting,
    populate: bool,
) -> OperationResult<VectorStorageEnum> {
    let storage =
        open_appendable_memmap_vector_storage_impl(path, dim, distance, madvise, populate)?;

    Ok(VectorStorageEnum::DenseAppendableMemmapHalf(Box::new(
        storage,
    )))
}

pub fn open_appendable_memmap_vector_storage_impl<T: PrimitiveVectorElement>(
    path: &Path,
    dim: usize,
    distance: Distance,
    madvise: AdviceSetting,
    populate: bool,
) -> OperationResult<AppendableMmapDenseVectorStorage<T>> {
    fs::create_dir_all(path)?;

    let vectors_path = path.join(VECTORS_DIR_PATH);
    let deleted_path = path.join(DELETED_DIR_PATH);

    let vectors = ChunkedVectors::open(
        MmapFs,
        &vectors_path,
        dim,
        madvise,
        Populate::from(populate),
    )?;

    let deleted = BitvecFlags::new(
        MmapFs,
        DynamicStoredFlags::open(&MmapFs, &deleted_path, Populate::from(populate))?,
    )?;
    let deleted_count = deleted.count_trues();

    Ok(AppendableMmapDenseVectorStorage {
        vectors,
        deleted,
        distance,
        deleted_count,
        _phantom: Default::default(),
    })
}

/// Find files related to this dense vector storage
#[cfg(test)]
pub(crate) fn find_storage_files(vector_storage_path: &Path) -> OperationResult<Vec<PathBuf>> {
    let vectors_path = vector_storage_path.join(VECTORS_DIR_PATH);
    let deleted_path = vector_storage_path.join(DELETED_DIR_PATH);

    let mut files = vec![];
    files.extend(common::disk::list_files(&vectors_path)?);
    files.extend(common::disk::list_files(&deleted_path)?);
    Ok(files)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};
    use tempfile::Builder;

    use super::*;

    const RAND_SEED: u64 = 42;

    /// Test that `find_storage_files` finds all files that are reported by the storage.
    #[test]
    fn test_find_storage_files() {
        // Numbers chosen so we get 3 data chunks, not just 1
        const POINT_COUNT: PointOffsetType = 2500;
        const DIM: usize = 128;

        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let mut storage = open_appendable_memmap_vector_storage_full(
            dir.path(),
            DIM,
            Distance::Dot,
            AdviceSetting::Global,
            false,
        )
        .unwrap();

        let mut rng = StdRng::seed_from_u64(RAND_SEED);
        let hw_counter = HardwareCounterCell::disposable();

        // Insert points, delete 10% of it, and flush
        for internal_id in 0..POINT_COUNT {
            let point = std::iter::repeat_with(|| rng.random_range(-1.0..1.0))
                .take(DIM)
                .collect::<Vec<_>>();
            storage
                .insert_vector(internal_id, VectorRef::from(&point), &hw_counter)
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
