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
use memory::mmap_type::{MmapBitSlice, MmapType};
use parking_lot::RwLock;
use sparse::common::sparse_vector::SparseVector;

use super::simple_sparse_vector_storage::SPARSE_VECTOR_DISTANCE;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::named_vectors::CowVector;
use crate::data_types::vectors::VectorRef;
use crate::types::VectorStorageDatatype;
use crate::vector_storage::{SparseVectorStorage, VectorStorage};

const METADATA_FILENAME: &str = "metadata.dat";
const DELETED_FILENAME: &str = "deleted.dat";
const STORAGE_DIRNAME: &str = "store";

const DELETED_MMAP_ADVICE: AdviceSetting = AdviceSetting::Global;
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
    deleted: MmapBitSlice,
    /// Other information about the storage, stored in a memory-mapped file.
    metadata: MmapType<Metadata>,
    path: PathBuf,
    populate: bool,
}

impl MmapSparseVectorStorage {
    pub fn open_or_create(path: &Path) -> OperationResult<Self> {
        // Don't preload mmaps.
        // To be changed once we want to use this as the only sparse storage
        const POPULATE_MMAPS: bool = false;

        let meta_path = path.join(METADATA_FILENAME);
        if meta_path.is_file() {
            // Storage already exists, open it
            return Self::open(path, POPULATE_MMAPS);
        }

        Self::create(path, POPULATE_MMAPS)
    }

    fn open(path: &Path, populate: bool) -> OperationResult<Self> {
        let path = path.to_path_buf();

        // Storage
        let storage_dir = path.join(STORAGE_DIRNAME);
        let storage = BlobStore::open(storage_dir).map_err(|err| {
            OperationError::service_error(format!(
                "Failed to open mmap sparse vector storage: {err}"
            ))
        })?;

        // Deleted flags
        let deleted_path = path.join(DELETED_FILENAME);
        let deleted_mmap = mmap_ops::open_write_mmap(&deleted_path, DELETED_MMAP_ADVICE, populate)?;
        let deleted = MmapBitSlice::try_from(deleted_mmap, 0)?;

        // Metadata
        let metadata_path = path.join(METADATA_FILENAME);
        let metadata_mmap =
            mmap_ops::open_write_mmap(&metadata_path, AdviceSetting::Global, populate)?;
        let metadata = unsafe { MmapType::try_from(metadata_mmap)? };

        Ok(Self {
            storage: Arc::new(RwLock::new(storage)),
            deleted,
            metadata,
            path,
            populate,
        })
    }

    fn create(path: &Path, populate: bool) -> OperationResult<Self> {
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
        let deleted_path = path.join(DELETED_FILENAME);
        mmap_ops::create_and_ensure_length(&deleted_path, size_of::<usize>())?;
        let deleted_mmap = mmap_ops::open_write_mmap(&deleted_path, DELETED_MMAP_ADVICE, populate)?;
        let deleted = MmapBitSlice::try_from(deleted_mmap, 0)?;

        // Metadata
        let metadata_path = path.join(METADATA_FILENAME);
        mmap_ops::create_and_ensure_length(&metadata_path, size_of::<Metadata>())?;
        let metadata_mmap =
            mmap_ops::open_write_mmap(&metadata_path, AdviceSetting::Global, populate)?;
        let mut metadata = unsafe { MmapType::try_from(metadata_mmap)? };
        // initialize metadata
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
            populate,
        })
    }

    fn set_deleted_bitslice(
        &mut self,
        key: PointOffsetType,
        deleted: bool,
    ) -> OperationResult<bool> {
        if (key as usize) < self.deleted.len() {
            return Ok(unsafe { self.deleted.replace_unchecked(key as usize, deleted) });
        }

        if deleted {
            // Bitslice is too small; grow and set the deletion flag.
            let bitslice_path = self.path.join(DELETED_FILENAME);
            unsafe {
                self.deleted.extend(
                    &bitslice_path,
                    key as usize + BITSLICE_GROWTH_SLACK,
                    DELETED_MMAP_ADVICE,
                    self.populate,
                )?;
                return Ok(self.deleted.replace_unchecked(key as usize, deleted));
            }
        }

        Ok(false)
    }

    #[inline]
    fn set_deleted(&mut self, key: PointOffsetType, deleted: bool) -> OperationResult<bool> {
        if !deleted && key as usize >= self.metadata.next_point_offset {
            return Ok(false);
        }
        // set deleted flag
        let previously_deleted = self.set_deleted_bitslice(key, deleted)?;

        // update deleted_count if it changed
        match (previously_deleted, deleted) {
            (false, true) => self.metadata.deleted_count += 1,
            (true, false) => {
                self.metadata.deleted_count = self.metadata.deleted_count.saturating_sub(1)
            }
            _ => {}
        }
        Ok(previously_deleted)
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
        Box::new(move || {
            storage.read().flush().map_err(|err| {
                OperationError::service_error(format!(
                    "Failed to flush mmap sparse vector storage: {err}"
                ))
            })?;
            Ok(())
        })
    }

    fn files(&self) -> Vec<std::path::PathBuf> {
        let mut files = self.storage.read().files();
        files.extend([
            self.path.join(DELETED_FILENAME),
            self.path.join(METADATA_FILENAME),
        ]);

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
        self.deleted.get(key as usize).map(|b| *b).unwrap_or(false)
    }

    fn deleted_vector_count(&self) -> usize {
        self.metadata.deleted_count
    }

    fn deleted_vector_bitslice(&self) -> &BitSlice {
        self.deleted.as_ref()
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;
    use std::path::{Path, PathBuf};

    use crate::vector_storage::sparse::mmap_sparse_vector_storage::MmapSparseVectorStorage;
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

        assert_eq!(storage_files.len(), 7);
        assert!(storage_files.iter().all(|f| f.exists()));
        assert_eq!(storage_files, existing_files);
    }
}
