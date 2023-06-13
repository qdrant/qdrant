use std::mem::size_of;
use std::ops::Range;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use bitvec::prelude::{BitSlice, BitVec};
use log::debug;
use parking_lot::RwLock;
use rocksdb::DB;
use serde::{Deserialize, Serialize};

use super::chunked_vectors::ChunkedVectors;
use super::vector_storage_base::VectorStorage;
use super::VectorStorageEnum;
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::common::Flusher;
use crate::data_types::vectors::VectorElementType;
use crate::entry::entry_point::{check_process_stopped, OperationError, OperationResult};
use crate::types::{Distance, PointOffsetType, QuantizationConfig};
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;

/// In-memory vector storage with on-update persistence using `store`
pub struct SimpleVectorStorage {
    dim: usize,
    distance: Distance,
    vectors: ChunkedVectors<VectorElementType>,
    quantized_vectors: Option<QuantizedVectors>,
    db_wrapper: DatabaseColumnWrapper,
    update_buffer: StoredRecord,
    /// BitVec for deleted flags. Grows dynamically upto last set flag.
    deleted: BitVec,
    /// Current number of deleted vectors.
    deleted_count: usize,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct StoredRecord {
    pub deleted: bool,
    pub vector: Vec<VectorElementType>,
}

pub fn open_simple_vector_storage(
    database: Arc<RwLock<DB>>,
    database_column_name: &str,
    dim: usize,
    distance: Distance,
) -> OperationResult<Arc<AtomicRefCell<VectorStorageEnum>>> {
    let mut vectors = ChunkedVectors::new(dim);
    let (mut deleted, mut deleted_count) = (BitVec::new(), 0);

    let db_wrapper = DatabaseColumnWrapper::new(database, database_column_name);

    for (key, value) in db_wrapper.lock_db().iter()? {
        let point_id: PointOffsetType = bincode::deserialize(&key)
            .map_err(|_| OperationError::service_error("cannot deserialize point id from db"))?;
        let stored_record: StoredRecord = bincode::deserialize(&value)
            .map_err(|_| OperationError::service_error("cannot deserialize record from db"))?;

        vectors.insert(point_id, &stored_record.vector)?;

        // Propagate deleted flag
        if stored_record.deleted {
            bitvec_grow_deleted_and_set(&mut deleted, point_id, true);
            deleted_count += 1;
        }
    }

    // Have enough deleted flags for all vectors
    bitvec_grow_deleted_to(&mut deleted, vectors.len());

    debug!("Segment vectors: {}", vectors.len());
    debug!(
        "Estimated segment size {} MB",
        vectors.len() * dim * size_of::<VectorElementType>() / 1024 / 1024
    );

    Ok(Arc::new(AtomicRefCell::new(VectorStorageEnum::Simple(
        SimpleVectorStorage {
            dim,
            distance,
            vectors,
            quantized_vectors: None,
            db_wrapper,
            update_buffer: StoredRecord {
                deleted: false,
                vector: vec![0.; dim],
            },
            deleted,
            deleted_count,
        },
    ))))
}

impl SimpleVectorStorage {
    /// Set deleted flag for given key. Returns previous deleted state.
    #[inline]
    fn set_deleted(&mut self, key: PointOffsetType, deleted: bool) -> bool {
        let was_deleted = self.deleted.replace(key as usize, deleted);
        if was_deleted != deleted {
            if deleted {
                self.deleted_count += 1;
            } else {
                self.deleted_count -= 1;
            }
        }
        was_deleted
    }

    /// Grow deleted flags to `new_len`.
    ///
    /// New flags are `false`, meaning their vectors are considered not deleted.
    #[inline]
    fn grow_deleted_to(&mut self, new_len: usize) {
        bitvec_grow_deleted_to(&mut self.deleted, new_len);
    }

    /// Grow the deleted flags to fit the given `key` offset, and mark it as deleted.
    ///
    /// New flags added as a result of growing are `false`, meaning their vectors are considered not deleted.
    fn grow_deleted_and_set(&mut self, key: PointOffsetType, deleted: bool) -> bool {
        let was_deleted = bitvec_grow_deleted_and_set(&mut self.deleted, key, deleted);
        if was_deleted != deleted {
            if deleted {
                self.deleted_count += 1;
            } else {
                self.deleted_count -= 1;
            }
        }
        was_deleted
    }

    fn update_stored(
        &mut self,
        key: PointOffsetType,
        deleted: bool,
        vector: Option<&[VectorElementType]>,
    ) -> OperationResult<()> {
        // Write vector state to buffer record
        let record = &mut self.update_buffer;
        record.deleted = deleted;
        if let Some(vector) = vector {
            record.vector.copy_from_slice(vector);
        }

        // Store updated record
        self.db_wrapper.put(
            bincode::serialize(&key).unwrap(),
            bincode::serialize(&record).unwrap(),
        )?;

        Ok(())
    }
}

impl VectorStorage for SimpleVectorStorage {
    fn vector_dim(&self) -> usize {
        self.dim
    }

    fn distance(&self) -> Distance {
        self.distance
    }

    fn total_vector_count(&self) -> usize {
        self.vectors.len()
    }

    fn get_vector(&self, key: PointOffsetType) -> &[VectorElementType] {
        self.vectors.get(key)
    }

    fn insert_vector(
        &mut self,
        key: PointOffsetType,
        vector: &[VectorElementType],
    ) -> OperationResult<()> {
        self.vectors.insert(key, vector)?;
        self.grow_deleted_and_set(key, false);
        self.update_stored(key, false, Some(vector))?;
        Ok(())
    }

    fn update_from(
        &mut self,
        other: &VectorStorageEnum,
        other_ids: &mut dyn Iterator<Item = PointOffsetType>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        // Grow deleted flags so we can flag new vectors
        // Grows to maximum number of added vectors, may be bigger than actual amount
        self.grow_deleted_to(self.total_vector_count() + other.total_vector_count());

        let start_index = self.vectors.len() as PointOffsetType;
        for point_id in other_ids {
            check_process_stopped(stopped)?;
            // Do not perform preprocessing - vectors should be already processed
            let other_vector = other.get_vector(point_id);
            let other_deleted = other.is_deleted_vector(point_id);
            let new_id = self.vectors.push(other_vector)?;
            self.set_deleted(new_id, other_deleted);
            self.update_stored(new_id, other_deleted, Some(other_vector))?;
        }
        let end_index = self.vectors.len() as PointOffsetType;

        // Truncate deleted flags to exact number of vectors we have
        self.deleted.truncate(self.total_vector_count());

        Ok(start_index..end_index)
    }

    fn flusher(&self) -> Flusher {
        self.db_wrapper.flusher()
    }

    fn quantize(
        &mut self,
        path: &Path,
        quantization_config: &QuantizationConfig,
        max_threads: usize,
        stopped: &AtomicBool,
    ) -> OperationResult<()> {
        let vector_data_iterator = (0..self.vectors.len() as u32).map(|i| self.vectors.get(i));
        self.quantized_vectors = Some(QuantizedVectors::create(
            vector_data_iterator,
            quantization_config,
            self.distance,
            self.dim,
            self.vectors.len(),
            path,
            false,
            max_threads,
            stopped,
        )?);
        Ok(())
    }

    fn load_quantization(&mut self, path: &Path) -> OperationResult<()> {
        if QuantizedVectors::config_exists(path) {
            self.quantized_vectors = Some(QuantizedVectors::load(path, false, self.distance)?);
        }
        Ok(())
    }

    fn quantized_storage(&self) -> Option<&QuantizedVectors> {
        self.quantized_vectors.as_ref()
    }

    fn files(&self) -> Vec<std::path::PathBuf> {
        if let Some(quantized_vectors) = &self.quantized_vectors {
            quantized_vectors.files()
        } else {
            vec![]
        }
    }

    fn delete_vector(&mut self, key: PointOffsetType) -> OperationResult<bool> {
        let is_deleted = !self.set_deleted(key, true);
        if is_deleted {
            self.update_stored(key, true, None)?;
        }
        Ok(is_deleted)
    }

    fn is_deleted_vector(&self, key: PointOffsetType) -> bool {
        self.deleted[key as usize]
    }

    fn deleted_vector_count(&self) -> usize {
        self.deleted_count
    }

    fn deleted_vector_bitslice(&self) -> &BitSlice {
        self.deleted.as_bitslice()
    }

    fn is_appendable(&self) -> bool {
        true
    }
}

/// Grow deleted flags to given `new_len`.
///
/// New flags are `false`, meaning their vectors are considered not deleted.
///
/// This does nothing if `new_len <= current_len`.
#[inline]
fn bitvec_grow_deleted_to(flags: &mut BitVec, new_len: usize) {
    if new_len <= flags.len() {
        return;
    }
    flags.resize(new_len, false);
}

/// Grow the deleted flags to fit the given `key` offset, and mark it as `deleted`.
///
/// New flags added as a result of growing are `false`, meaning their vectors are considered not deleted.
fn bitvec_grow_deleted_and_set(flags: &mut BitVec, key: PointOffsetType, deleted: bool) -> bool {
    bitvec_grow_deleted_to(flags, key as usize + 1);
    unsafe { flags.replace_unchecked(key as usize, deleted) }
}
