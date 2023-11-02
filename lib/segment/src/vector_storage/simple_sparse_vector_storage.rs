use std::ops::Range;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use bitvec::prelude::{BitSlice, BitVec};
use common::types::PointOffsetType;
use parking_lot::RwLock;
use rocksdb::DB;
use serde::{Deserialize, Serialize};
use sparse::common::sparse_vector::SparseVector;

use super::SparseVectorStorage;
use crate::common::operation_error::{check_process_stopped, OperationError, OperationResult};
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::common::Flusher;
use crate::data_types::vectors::VectorRef;
use crate::types::Distance;
use crate::vector_storage::bitvec::bitvec_set_deleted;
use crate::vector_storage::{VectorStorage, VectorStorageEnum};

/// In-memory vector storage with on-update persistence using `store`
pub struct SimpleSparseVectorStorage {
    distance: Distance,
    vectors: Vec<SparseVector>,
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
    pub vector: SparseVector,
}

#[allow(unused)]
pub fn open_simple_sparse_vector_storage(
    database: Arc<RwLock<DB>>,
    database_column_name: &str,
    distance: Distance,
) -> OperationResult<Arc<AtomicRefCell<VectorStorageEnum>>> {
    let (mut deleted, mut deleted_count) = (BitVec::new(), 0);
    let mut vectors = Vec::new();
    let db_wrapper = DatabaseColumnWrapper::new(database, database_column_name);

    db_wrapper.lock_db().iter()?;
    for (key, value) in db_wrapper.lock_db().iter()? {
        let point_id: PointOffsetType = bincode::deserialize(&key)
            .map_err(|_| OperationError::service_error("cannot deserialize point id from db"))?;
        let stored_record: StoredRecord = bincode::deserialize(&value)
            .map_err(|_| OperationError::service_error("cannot deserialize record from db"))?;

        // Propagate deleted flag
        if stored_record.deleted {
            bitvec_set_deleted(&mut deleted, point_id, true);
            deleted_count += 1;
        }

        // Resize storage if needed
        vectors.resize_with(
            std::cmp::max(vectors.len(), point_id as usize + 1),
            Default::default,
        );
        vectors[point_id as usize] = stored_record.vector;
    }

    Ok(Arc::new(AtomicRefCell::new(
        VectorStorageEnum::SparseSimple(SimpleSparseVectorStorage {
            distance,
            vectors,
            db_wrapper,
            update_buffer: StoredRecord {
                deleted: false,
                vector: SparseVector::default(),
            },
            deleted,
            deleted_count,
        }),
    )))
}

impl SimpleSparseVectorStorage {
    /// Set deleted flag for given key. Returns previous deleted state.
    #[inline]
    fn set_deleted(&mut self, key: PointOffsetType, deleted: bool) -> bool {
        if key as usize >= self.vectors.len() {
            return false;
        }
        let was_deleted = bitvec_set_deleted(&mut self.deleted, key, deleted);
        if was_deleted != deleted {
            if !was_deleted {
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
        vector: Option<&SparseVector>,
    ) -> OperationResult<()> {
        // Write vector state to buffer record
        let record = &mut self.update_buffer;
        record.deleted = deleted;
        if let Some(vector) = vector {
            record.vector = vector.clone();
        }

        // Store updated record
        self.db_wrapper.put(
            bincode::serialize(&key).unwrap(),
            bincode::serialize(&record).unwrap(),
        )?;

        Ok(())
    }
}

impl SparseVectorStorage for SimpleSparseVectorStorage {
    fn get_sparse(&self, key: PointOffsetType) -> &SparseVector {
        self.vectors.get(key as usize).expect("Invalid point id")
    }
}

impl VectorStorage for SimpleSparseVectorStorage {
    fn vector_dim(&self) -> usize {
        0 // not applicable
    }

    fn distance(&self) -> Distance {
        self.distance
    }

    fn is_on_disk(&self) -> bool {
        true
    }

    fn total_vector_count(&self) -> usize {
        self.vectors.len()
    }

    fn get_vector(&self, key: PointOffsetType) -> VectorRef {
        self.get_sparse(key).into()
    }

    fn insert_vector(&mut self, key: PointOffsetType, vector: VectorRef) -> OperationResult<()> {
        let vector: &SparseVector = vector.try_into()?;
        self.vectors.insert(key as usize, vector.clone());
        self.set_deleted(key, false);
        self.update_stored(key, false, Some(vector))?;
        Ok(())
    }

    fn update_from(
        &mut self,
        other: &VectorStorageEnum,
        other_ids: &mut dyn Iterator<Item = PointOffsetType>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        let start_index = self.vectors.len() as PointOffsetType;
        for point_id in other_ids {
            check_process_stopped(stopped)?;
            // Do not perform preprocessing - vectors should be already processed
            let other_vector: &SparseVector = other.get_vector(point_id).try_into()?;
            let other_deleted = other.is_deleted_vector(point_id);
            match self.vectors.get(point_id as usize) {
                Some(_stored_vector) => {
                    self.set_deleted(point_id, other_deleted);
                    self.update_stored(point_id, other_deleted, Some(other_vector))?;
                }
                None => {
                    self.vectors
                        .resize_with(point_id as usize + 1, Default::default);
                    self.vectors[point_id as usize] = other_vector.clone();
                }
            }
            self.set_deleted(point_id, other_deleted);
            self.update_stored(point_id, other_deleted, Some(other_vector))?;
        }
        let end_index = self.vectors.len() as PointOffsetType;
        Ok(start_index..end_index)
    }

    fn flusher(&self) -> Flusher {
        self.db_wrapper.flusher()
    }

    fn files(&self) -> Vec<std::path::PathBuf> {
        vec![]
    }

    fn delete_vector(&mut self, key: PointOffsetType) -> OperationResult<bool> {
        let is_deleted = !self.set_deleted(key, true);
        if is_deleted {
            self.update_stored(key, true, None)?;
        }
        Ok(is_deleted)
    }

    fn is_deleted_vector(&self, key: PointOffsetType) -> bool {
        self.deleted.get(key as usize).map(|b| *b).unwrap_or(false)
    }

    fn deleted_vector_count(&self) -> usize {
        self.deleted_count
    }

    fn deleted_vector_bitslice(&self) -> &BitSlice {
        self.deleted.as_bitslice()
    }
}
