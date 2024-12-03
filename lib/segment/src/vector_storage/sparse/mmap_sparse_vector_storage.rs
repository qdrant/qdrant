use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bitvec::slice::BitSlice;
use blob_store::BlobStore;
use common::iterator_ext::IteratorExt;
use common::types::PointOffsetType;
use memory::madvise::AdviceSetting;
use memory::mmap_ops::{self};
use memory::mmap_type::MmapType;
use parking_lot::RwLock;
use sparse::common::sparse_vector::SparseVector;

use super::simple_sparse_vector_storage::SPARSE_VECTOR_DISTANCE;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::named_vectors::CowVector;
use crate::data_types::vectors::VectorRef;
use crate::types::VectorStorageDatatype;
use crate::vector_storage::dense::dynamic_mmap_flags::DynamicMmapFlags;
use crate::vector_storage::{SparseVectorStorage, VectorStorage};

const METADATA_FILENAME: &str = "metadata.dat";
const DELETED_DIRNAME: &str = "deleted";
const STORAGE_DIRNAME: &str = "store";

/// When resizing bitslice, grow by this extra amount.
const BITSLICE_GROWTH_SLACK: usize = 1024;

#[repr(C)]
#[derive(Debug)]
struct Metadata {
    /// Current number of deleted vectors.
    deleted_count: usize,
    /// Maximum point offset in the storage + 1. This also means the total amount of point offsets
    next_point_offset: usize,
    /// Total number of non-zero elements in all vectors. Used to estimate average vector size.
    total_sparse_size: usize,
    /// Reserved for extensibility
    _reserved: [u8; 64],
}

/// Memory-mapped mutable sparse vector storage.
#[derive(Debug)]
pub struct MmapSparseVectorStorage {
    storage: Arc<RwLock<BlobStore<SparseVector>>>,
    /// BitSlice for deleted flags. Grows dynamically upto last set flag.
    deleted: DynamicMmapFlags,
    /// Other information about the storage, stored in a memory-mapped file.
    metadata: MmapType<Metadata>,
    path: PathBuf,
}

impl MmapSparseVectorStorage {
    pub fn open_or_create(path: &Path) -> OperationResult<Self> {
        let meta_path = path.join(METADATA_FILENAME);
        if meta_path.is_file() {
            // Storage already exists, open it
            return Self::open(path);
        }

        Self::create(path)
    }

    fn open(path: &Path) -> OperationResult<Self> {
        let path = path.to_path_buf();

        // Storage
        let storage_dir = path.join(STORAGE_DIRNAME);
        let storage = BlobStore::open(storage_dir).map_err(|err| {
            OperationError::service_error(format!(
                "Failed to open mmap sparse vector storage: {err}"
            ))
        })?;

        // Deleted flags
        let deleted_path = path.join(DELETED_DIRNAME);
        let deleted = DynamicMmapFlags::open(&deleted_path)?;

        // Metadata
        let metadata_path = path.join(METADATA_FILENAME);
        let metadata_mmap =
            mmap_ops::open_write_mmap(&metadata_path, AdviceSetting::Global, false)?;
        let metadata: MmapType<Metadata> = unsafe { MmapType::try_from(metadata_mmap)? };

        debug_assert_eq!(
            metadata.next_point_offset as PointOffsetType,
            storage.max_point_id()
        );

        Ok(Self {
            storage: Arc::new(RwLock::new(storage)),
            deleted,
            metadata,
            path,
        })
    }

    fn create(path: &Path) -> OperationResult<Self> {
        let path = path.to_path_buf();

        // Storage
        let storage_dir = path.join(STORAGE_DIRNAME);
        std::fs::create_dir_all(&storage_dir)?;
        let storage = BlobStore::new(storage_dir, Default::default()).map_err(|err| {
            OperationError::service_error(format!(
                "Failed to create storage for mmap sparse vectors: {err}"
            ))
        })?;

        // Deleted flags
        let deleted_path = path.join(DELETED_DIRNAME);
        let deleted = DynamicMmapFlags::open(&deleted_path)?;

        // Metadata
        let metadata_path = path.join(METADATA_FILENAME);
        mmap_ops::create_and_ensure_length(&metadata_path, size_of::<Metadata>())?;
        let metadata_mmap =
            mmap_ops::open_write_mmap(&metadata_path, AdviceSetting::Global, false)?;
        let mut metadata = unsafe { MmapType::try_from(metadata_mmap)? };
        // Initialize metadata
        *metadata = Metadata {
            deleted_count: 0,
            next_point_offset: 0,
            total_sparse_size: 0,
            _reserved: [0; 64],
        };

        Ok(Self {
            storage: Arc::new(RwLock::new(storage)),
            deleted,
            metadata,
            path,
        })
    }

    fn set_deleted_flag(&mut self, key: PointOffsetType, deleted: bool) -> OperationResult<bool> {
        if (key as usize) < self.deleted.len() {
            return Ok(self.deleted.set(key, deleted));
        }

        // Bitslice is too small; grow and set the deletion flag, but only if we need to set it to true.
        if deleted {
            self.deleted.set_len(key as usize + BITSLICE_GROWTH_SLACK)?;
            return Ok(self.deleted.set(key, true));
        }

        Ok(false)
    }

    #[inline]
    fn set_deleted(&mut self, key: PointOffsetType, deleted: bool) -> OperationResult<bool> {
        if !deleted && key as usize >= self.metadata.next_point_offset {
            return Ok(false);
        }
        // set deleted flag
        let previous_value = self.set_deleted_flag(key, deleted)?;

        // update deleted_count if it changed
        match (previous_value, deleted) {
            (false, true) => self.metadata.deleted_count += 1,
            (true, false) => {
                self.metadata.deleted_count = self.metadata.deleted_count.saturating_sub(1)
            }
            _ => {}
        }
        Ok(previous_value)
    }

    fn update_stored(
        &mut self,
        key: PointOffsetType,
        vector: Option<&SparseVector>,
    ) -> OperationResult<()> {
        let mut storage_guard = self.storage.write();
        if let Some(vector) = vector {
            // upsert vector
            if let Some(old_vector) = storage_guard.get_value(key) {
                // it is an update
                self.metadata.total_sparse_size = self
                    .metadata
                    .total_sparse_size
                    .saturating_sub(old_vector.values.len());
            }

            self.metadata.total_sparse_size += vector.values.len();
            storage_guard
                .put_value(key, vector)
                .map_err(OperationError::service_error)?;
        } else {
            // delete vector
            if let Some(old_vector) = storage_guard.delete_value(key) {
                self.metadata.total_sparse_size = self
                    .metadata
                    .total_sparse_size
                    .saturating_sub(old_vector.values.len());
            }
        }

        self.metadata.next_point_offset =
            std::cmp::max(self.metadata.next_point_offset, key as usize + 1);

        Ok(())
    }
}

impl SparseVectorStorage for MmapSparseVectorStorage {
    fn get_sparse(
        &self,
        key: PointOffsetType,
    ) -> crate::common::operation_error::OperationResult<SparseVector> {
        self.get_sparse_opt(key)?
            .ok_or_else(|| OperationError::service_error(format!("Key {key} not found")))
    }

    fn get_sparse_opt(
        &self,
        key: PointOffsetType,
    ) -> crate::common::operation_error::OperationResult<Option<SparseVector>> {
        Ok(self.storage.read().get_value(key))
    }
}

impl VectorStorage for MmapSparseVectorStorage {
    fn distance(&self) -> crate::types::Distance {
        SPARSE_VECTOR_DISTANCE
    }

    fn datatype(&self) -> crate::types::VectorStorageDatatype {
        VectorStorageDatatype::Float32
    }

    fn is_on_disk(&self) -> bool {
        true
    }

    fn total_vector_count(&self) -> usize {
        self.metadata.next_point_offset
    }

    fn get_vector(&self, key: PointOffsetType) -> CowVector {
        let vector = self.get_vector_opt(key);
        vector.unwrap_or_else(CowVector::default_sparse)
    }

    /// Get vector by key, if it exists.
    ///
    /// Ignore any error
    fn get_vector_opt(&self, key: PointOffsetType) -> Option<CowVector> {
        match self.get_sparse_opt(key) {
            Ok(Some(vector)) => Some(CowVector::from(vector)),
            _ => None,
        }
    }

    fn insert_vector(&mut self, key: PointOffsetType, vector: VectorRef) -> OperationResult<()> {
        let vector = <&SparseVector>::try_from(vector)?;
        debug_assert!(vector.is_sorted());
        self.set_deleted(key, false)?;
        self.update_stored(key, Some(vector))?;
        Ok(())
    }

    fn update_from<'a>(
        &mut self,
        other_vectors: &'a mut impl Iterator<Item = (CowVector<'a>, bool)>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        let start_index = self.metadata.next_point_offset as PointOffsetType;
        for (other_vector, other_deleted) in
            other_vectors.check_stop(|| stopped.load(Ordering::Relaxed))
        {
            // Do not perform preprocessing - vectors should be already processed
            let other_vector = other_vector.as_vec_ref().try_into()?;
            let new_id = self.metadata.next_point_offset as PointOffsetType;
            self.metadata.next_point_offset += 1;
            self.set_deleted(new_id, other_deleted)?;

            let vector = (!other_deleted).then_some(other_vector);
            self.update_stored(new_id, vector)?;
        }
        Ok(start_index..self.metadata.next_point_offset as PointOffsetType)
    }

    fn flusher(&self) -> crate::common::Flusher {
        let storage = self.storage.clone();
        let deleted_flusher = self.deleted.flusher();
        let metadata_flusher = self.metadata.flusher();
        Box::new(move || {
            storage.read().flush().map_err(|err| {
                OperationError::service_error(format!(
                    "Failed to flush mmap sparse vector storage: {err}"
                ))
            })?;
            deleted_flusher()?;
            metadata_flusher()?;
            Ok(())
        })
    }

    fn files(&self) -> Vec<std::path::PathBuf> {
        let mut files = self.storage.read().files();
        files.extend(self.deleted.files());
        files.push(self.path.join(METADATA_FILENAME));

        files
    }

    fn delete_vector(
        &mut self,
        key: common::types::PointOffsetType,
    ) -> crate::common::operation_error::OperationResult<bool> {
        let was_deleted = !self.set_deleted(key, true)?;

        self.update_stored(key, None)?;

        Ok(was_deleted)
    }

    fn is_deleted_vector(&self, key: common::types::PointOffsetType) -> bool {
        self.deleted.get(key as usize)
    }

    fn deleted_vector_count(&self) -> usize {
        self.metadata.deleted_count
    }

    fn deleted_vector_bitslice(&self) -> &BitSlice {
        self.deleted.get_bitslice()
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;
    use std::path::{Path, PathBuf};

    use sparse::common::sparse_vector;

    use crate::vector_storage::sparse::mmap_sparse_vector_storage::{
        MmapSparseVectorStorage, VectorRef,
    };
    use crate::vector_storage::VectorStorage;

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

        assert_eq!(storage_files.len(), 8);
        assert!(storage_files.iter().all(|f| f.exists()));
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

        {
            let mut storage = MmapSparseVectorStorage::open_or_create(tmp_dir.path()).unwrap();

            storage.insert_vector(0, VectorRef::from(&vector)).unwrap();
            storage.insert_vector(2, VectorRef::from(&vector)).unwrap();
            storage.insert_vector(4, VectorRef::from(&vector)).unwrap();
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
}
