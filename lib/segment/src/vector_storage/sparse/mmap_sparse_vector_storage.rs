use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bitvec::slice::BitSlice;
use common::counter::hardware_counter::HardwareCounterCell;
use common::iterator_ext::IteratorExt;
use common::types::PointOffsetType;
use gridstore::Gridstore;
use gridstore::config::{Compression, StorageOptions};
use parking_lot::RwLock;
use sparse::common::sparse_vector::SparseVector;

use crate::common::flags::bitvec_flags::BitvecFlags;
use crate::common::flags::dynamic_mmap_flags::DynamicMmapFlags;
use crate::common::operation_error::{OperationError, OperationResult, check_process_stopped};
use crate::data_types::named_vectors::CowVector;
use crate::data_types::vectors::VectorRef;
use crate::types::VectorStorageDatatype;
use crate::vector_storage::sparse::stored_sparse_vectors::StoredSparseVector;
use crate::vector_storage::{SparseVectorStorage, VectorStorage};

const DELETED_DIRNAME: &str = "deleted";
const STORAGE_DIRNAME: &str = "store";

/// Memory-mapped mutable sparse vector storage.
#[derive(Debug)]
pub struct MmapSparseVectorStorage {
    storage: Arc<RwLock<Gridstore<StoredSparseVector>>>,
    /// Flags marking deleted vectors
    ///
    /// Structure grows dynamically, but may be smaller than actual number of vectors. Must not
    /// depend on its length.
    deleted: BitvecFlags,
    /// Current number of deleted vectors.
    deleted_count: usize,
    /// Maximum point offset in the storage + 1. This also means the total amount of point offsets
    next_point_offset: usize,
}

impl MmapSparseVectorStorage {
    pub fn open_or_create(path: &Path) -> OperationResult<Self> {
        let deleted_dir = path.join(DELETED_DIRNAME);
        if deleted_dir.is_dir() {
            // Storage already exists, open it
            return Self::open(path);
        }

        Self::create(path)
    }

    fn open(path: &Path) -> OperationResult<Self> {
        // Storage
        let storage_dir = path.join(STORAGE_DIRNAME);
        let storage = Gridstore::open(storage_dir).map_err(|err| {
            OperationError::service_error(format!(
                "Failed to open mmap sparse vector storage: {err}"
            ))
        })?;

        // Payload storage does not need to be populated
        // as it is not required in the index search step
        let populate = false;

        // Deleted flags
        let deleted_path = path.join(DELETED_DIRNAME);
        let deleted = BitvecFlags::new(DynamicMmapFlags::open(&deleted_path, populate)?);

        let deleted_count = deleted.count_trues();
        let next_point_offset = deleted
            .get_bitslice()
            .last_one()
            .max(Some(storage.max_point_id() as usize))
            .unwrap_or_default();

        Ok(Self {
            storage: Arc::new(RwLock::new(storage)),
            deleted,
            deleted_count,
            next_point_offset,
        })
    }

    fn create(path: &Path) -> OperationResult<Self> {
        let path = path.to_path_buf();

        // Storage
        let storage_dir = path.join(STORAGE_DIRNAME);
        std::fs::create_dir_all(&storage_dir)?;
        let storage_config = StorageOptions {
            // Don't use built-in compression, as we will use bitpacking instead
            compression: Some(Compression::None),
            ..Default::default()
        };

        let storage = Gridstore::new(storage_dir, storage_config).map_err(|err| {
            OperationError::service_error(format!(
                "Failed to create storage for mmap sparse vectors: {err}"
            ))
        })?;

        // Payload storage does not need to be populated
        // as it is not required in the index search step
        let populate = false;

        // Deleted flags
        let deleted_path = path.join(DELETED_DIRNAME);
        let deleted = BitvecFlags::new(DynamicMmapFlags::open(&deleted_path, populate)?);

        Ok(Self {
            storage: Arc::new(RwLock::new(storage)),
            deleted,
            deleted_count: 0,
            next_point_offset: 0,
        })
    }

    #[inline]
    fn set_deleted(&mut self, key: PointOffsetType, deleted: bool) -> bool {
        if !deleted && key as usize >= self.next_point_offset {
            return false;
        }

        // set deleted flag
        let previous_value = self.deleted.set(key, deleted);

        // update deleted_count if it changed
        match (previous_value, deleted) {
            (false, true) => self.deleted_count += 1,
            (true, false) => self.deleted_count = self.deleted_count.saturating_sub(1),
            _ => {}
        }
        previous_value
    }

    fn update_stored(
        &mut self,
        key: PointOffsetType,
        vector: Option<&SparseVector>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let mut storage_guard = self.storage.write();
        if let Some(vector) = vector {
            // upsert vector
            storage_guard
                .put_value(
                    key,
                    &StoredSparseVector::from(vector),
                    hw_counter.ref_vector_io_write_counter(),
                )
                .map_err(OperationError::service_error)?;
        } else {
            // delete vector
            storage_guard.delete_value(key);
        }

        self.next_point_offset = std::cmp::max(self.next_point_offset, key as usize + 1);

        Ok(())
    }

    /// Populate all pages in the mmap.
    /// Block until all pages are populated.
    pub fn populate(&self) -> OperationResult<()> {
        // deleted bitvec is already in-memory
        self.storage.read().populate()?;
        Ok(())
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        self.deleted.clear_cache()?;
        self.storage.read().clear_cache()?;
        Ok(())
    }
}

impl SparseVectorStorage for MmapSparseVectorStorage {
    fn get_sparse(&self, key: PointOffsetType) -> OperationResult<SparseVector> {
        self.get_sparse_opt(key)?
            .ok_or_else(|| OperationError::service_error(format!("Key {key} not found")))
    }

    fn get_sparse_opt(&self, key: PointOffsetType) -> OperationResult<Option<SparseVector>> {
        self.storage
            .read()
            .get_value(key, &HardwareCounterCell::disposable()) // Vector storage read IO not measured
            .map(SparseVector::try_from)
            .transpose()
    }

    fn get_sparse_sequential(&self, key: PointOffsetType) -> OperationResult<SparseVector> {
        let stored_sparse = self
            .storage
            .read()
            .get_value_sequential(key, &HardwareCounterCell::disposable())
            .ok_or_else(|| OperationError::service_error(format!("Key {key} not found")))?;
        SparseVector::try_from(stored_sparse)
    }
}

impl VectorStorage for MmapSparseVectorStorage {
    fn distance(&self) -> crate::types::Distance {
        super::SPARSE_VECTOR_DISTANCE
    }

    fn datatype(&self) -> crate::types::VectorStorageDatatype {
        VectorStorageDatatype::Float32
    }

    fn is_on_disk(&self) -> bool {
        true
    }

    fn total_vector_count(&self) -> usize {
        self.next_point_offset
    }

    fn get_vector(&self, key: PointOffsetType) -> CowVector<'_> {
        let vector = self.get_vector_opt(key);
        vector.unwrap_or_else(CowVector::default_sparse)
    }

    fn get_vector_sequential(&self, key: PointOffsetType) -> CowVector<'_> {
        self.get_sparse_sequential(key)
            .map(CowVector::from)
            .unwrap_or_else(|_| CowVector::default_sparse())
    }

    /// Get vector by key, if it exists.
    ///
    /// Ignore any error
    fn get_vector_opt(&self, key: PointOffsetType) -> Option<CowVector<'_>> {
        match self.get_sparse_opt(key) {
            Ok(Some(vector)) => Some(CowVector::from(vector)),
            _ => None,
        }
    }

    fn insert_vector(
        &mut self,
        key: PointOffsetType,
        vector: VectorRef,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let vector = <&SparseVector>::try_from(vector)?;
        debug_assert!(vector.is_sorted(), "Vector is not sorted {vector:?}");
        self.set_deleted(key, false);
        self.update_stored(key, Some(vector), hw_counter)?;
        Ok(())
    }

    fn update_from<'a>(
        &mut self,
        other_vectors: &'a mut impl Iterator<Item = (CowVector<'a>, bool)>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        let hw_counter = HardwareCounterCell::disposable(); // This function is only used for internal operations. No need to measure.
        let start_index = self.next_point_offset as PointOffsetType;
        for (other_vector, other_deleted) in
            other_vectors.check_stop(|| stopped.load(Ordering::Relaxed))
        {
            // Do not perform preprocessing - vectors should be already processed
            let other_vector = other_vector.as_vec_ref().try_into()?;
            let new_id = self.next_point_offset as PointOffsetType;
            self.next_point_offset += 1;
            self.set_deleted(new_id, other_deleted);

            let vector = (!other_deleted).then_some(other_vector);
            self.update_stored(new_id, vector, &hw_counter)?;
        }

        // return cancelled error if stopped
        check_process_stopped(stopped)?;

        Ok(start_index..self.next_point_offset as PointOffsetType)
    }

    fn flusher(&self) -> crate::common::Flusher {
        let storage = self.storage.clone();
        let deleted_flags_flusher = self.deleted.flusher();
        Box::new(move || {
            deleted_flags_flusher()?;
            storage.read().flush().map_err(|err| {
                OperationError::service_error(format!(
                    "Failed to flush mmap sparse vector storage: {err}"
                ))
            })
        })
    }

    fn files(&self) -> Vec<PathBuf> {
        let mut files = self.storage.read().files();
        files.extend(self.deleted.files());

        files
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        self.storage.read().immutable_files()
    }

    fn delete_vector(
        &mut self,
        key: common::types::PointOffsetType,
    ) -> crate::common::operation_error::OperationResult<bool> {
        let was_deleted = !self.set_deleted(key, true);

        let hw_counter = HardwareCounterCell::disposable(); // Deletions not measured
        self.update_stored(key, None, &hw_counter)?;

        Ok(was_deleted)
    }

    fn is_deleted_vector(&self, key: common::types::PointOffsetType) -> bool {
        self.deleted.get(key)
    }

    fn deleted_vector_count(&self) -> usize {
        self.deleted_count
    }

    fn deleted_vector_bitslice(&self) -> &BitSlice {
        self.deleted.get_bitslice()
    }
}

/// Find files related to this sparse vector storage
#[cfg(any(test, feature = "rocksdb"))]
pub(crate) fn find_storage_files(vector_storage_path: &Path) -> OperationResult<Vec<PathBuf>> {
    let storage_path = vector_storage_path.join(STORAGE_DIRNAME);
    let deleted_path = vector_storage_path.join(DELETED_DIRNAME);

    let mut files = vec![];
    files.extend(common::disk::list_files(&storage_path)?);
    files.extend(common::disk::list_files(&deleted_path)?);
    Ok(files)
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;
    use std::path::{Path, PathBuf};

    use common::counter::hardware_counter::HardwareCounterCell;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use sparse::common::sparse_vector;
    use sparse::common::sparse_vector_fixture::random_sparse_vector;
    use tempfile::Builder;

    use super::*;
    use crate::vector_storage::VectorStorage;
    use crate::vector_storage::sparse::mmap_sparse_vector_storage::{
        MmapSparseVectorStorage, VectorRef,
    };

    const RAND_SEED: u64 = 42;

    fn visit_files_recursively(dir: &Path, cb: &mut impl FnMut(PathBuf)) -> std::io::Result<()> {
        if dir.is_dir() {
            for entry in std::fs::read_dir(dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_dir() {
                    visit_files_recursively(&path, cb)?;
                } else {
                    cb(entry.path());
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_files_consistency() {
        let tmp_dir = tempfile::Builder::new()
            .prefix("test_storage")
            .tempdir()
            .unwrap();
        let storage = MmapSparseVectorStorage::open_or_create(tmp_dir.path()).unwrap();

        let mut existing_files = HashSet::new();
        visit_files_recursively(tmp_dir.path(), &mut |path| {
            existing_files.insert(path);
        })
        .unwrap();

        let storage_files = storage.files().into_iter().collect::<HashSet<_>>();

        assert_eq!(storage_files, existing_files);
    }

    #[test]
    fn test_create_insert_close_and_load() {
        let tmp_dir = tempfile::Builder::new()
            .prefix("test_storage")
            .tempdir()
            .unwrap();

        let vector = sparse_vector::SparseVector {
            indices: vec![1, 2, 3],
            values: vec![0.1, 0.2, 0.3],
        };

        let hw_counter = HardwareCounterCell::new();

        {
            let mut storage = MmapSparseVectorStorage::open_or_create(tmp_dir.path()).unwrap();

            storage
                .insert_vector(0, VectorRef::from(&vector), &hw_counter)
                .unwrap();
            storage
                .insert_vector(2, VectorRef::from(&vector), &hw_counter)
                .unwrap();
            storage
                .insert_vector(4, VectorRef::from(&vector), &hw_counter)
                .unwrap();
            storage.flusher()().unwrap();
        }

        let storage = MmapSparseVectorStorage::open(tmp_dir.path()).unwrap();
        let result_vector = storage.get_vector(0);

        match result_vector {
            crate::data_types::named_vectors::CowVector::Sparse(sparse) => {
                assert_eq!(sparse.values, vector.values);
            }
            _ => panic!("Expected sparse vector"),
        };
    }

    /// Test that `find_storage_files` finds all files that are reported by the storage.
    #[test]
    fn test_find_storage_files() {
        const POINT_COUNT: PointOffsetType = 1000;
        const DIM: usize = 1024;

        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let mut storage = MmapSparseVectorStorage::open_or_create(dir.path()).unwrap();

        let mut rng = StdRng::seed_from_u64(RAND_SEED);
        let hw_counter = HardwareCounterCell::disposable();

        // Insert points, delete 10% of it, and flush
        for internal_id in 0..POINT_COUNT {
            let vector = random_sparse_vector(&mut rng, DIM);
            storage
                .insert_vector(internal_id, VectorRef::from(&vector), &hw_counter)
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
