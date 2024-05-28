use std::borrow::Cow;
use std::mem::size_of;
use std::ops::Range;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use bitvec::prelude::{BitSlice, BitVec};
use common::types::PointOffsetType;
use log::debug;
use parking_lot::RwLock;
use rocksdb::DB;

use crate::common::operation_error::{check_process_stopped, OperationError, OperationResult};
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::common::Flusher;
use crate::data_types::named_vectors::CowVector;
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{VectorElementType, VectorRef};
use crate::types::{Distance, VectorStorageDatatype};
use crate::vector_storage::bitvec::bitvec_set_deleted;
use crate::vector_storage::chunked_vectors::ChunkedVectors;
use crate::vector_storage::common::StoredRecord;
use crate::vector_storage::{DenseVectorStorage, VectorStorage, VectorStorageEnum};

type StoredDenseVector<T> = StoredRecord<Vec<T>>;

/// In-memory vector storage with on-update persistence using `store`
pub struct SimpleDenseVectorStorage<T: PrimitiveVectorElement> {
    dim: usize,
    distance: Distance,
    vectors: ChunkedVectors<T>,
    db_wrapper: DatabaseColumnWrapper,
    update_buffer: StoredDenseVector<T>,
    /// BitVec for deleted flags. Grows dynamically upto last set flag.
    deleted: BitVec,
    /// Current number of deleted vectors.
    deleted_count: usize,
}

fn open_simple_dense_vector_storage_impl<T: PrimitiveVectorElement>(
    database: Arc<RwLock<DB>>,
    database_column_name: &str,
    dim: usize,
    distance: Distance,
    stopped: &AtomicBool,
) -> OperationResult<SimpleDenseVectorStorage<T>> {
    let mut vectors = ChunkedVectors::new(dim);
    let (mut deleted, mut deleted_count) = (BitVec::new(), 0);

    let db_wrapper = DatabaseColumnWrapper::new(database, database_column_name);

    for (key, value) in db_wrapper.lock_db().iter()? {
        let point_id: PointOffsetType = bincode::deserialize(&key)
            .map_err(|_| OperationError::service_error("cannot deserialize point id from db"))?;
        let stored_record: StoredDenseVector<T> = bincode::deserialize(&value)
            .map_err(|_| OperationError::service_error("cannot deserialize record from db"))?;

        // Propagate deleted flag
        if stored_record.deleted {
            bitvec_set_deleted(&mut deleted, point_id, true);
            deleted_count += 1;
        }
        vectors.insert(point_id, &stored_record.vector)?;

        check_process_stopped(stopped)?;
    }

    debug!("Segment vectors: {}", vectors.len());
    debug!(
        "Estimated segment size {} MB",
        vectors.len() * dim * size_of::<T>() / 1024 / 1024
    );

    Ok(SimpleDenseVectorStorage {
        dim,
        distance,
        vectors,
        db_wrapper,
        update_buffer: StoredRecord {
            deleted: false,
            vector: vec![T::default(); dim],
        },
        deleted,
        deleted_count,
    })
}

pub fn open_simple_dense_vector_storage(
    database: Arc<RwLock<DB>>,
    database_column_name: &str,
    dim: usize,
    distance: Distance,
    stopped: &AtomicBool,
) -> OperationResult<Arc<AtomicRefCell<VectorStorageEnum>>> {
    let storage = open_simple_dense_vector_storage_impl::<VectorElementType>(
        database,
        database_column_name,
        dim,
        distance,
        stopped,
    )?;

    Ok(Arc::new(AtomicRefCell::new(
        VectorStorageEnum::DenseSimple(storage),
    )))
}

pub fn open_simple_dense_byte_vector_storage(
    database: Arc<RwLock<DB>>,
    database_column_name: &str,
    dim: usize,
    distance: Distance,
    stopped: &AtomicBool,
) -> OperationResult<Arc<AtomicRefCell<VectorStorageEnum>>> {
    let storage = open_simple_dense_vector_storage_impl(
        database,
        database_column_name,
        dim,
        distance,
        stopped,
    )?;

    Ok(Arc::new(AtomicRefCell::new(
        VectorStorageEnum::DenseSimpleByte(storage),
    )))
}

pub fn open_simple_dense_half_vector_storage(
    database: Arc<RwLock<DB>>,
    database_column_name: &str,
    dim: usize,
    distance: Distance,
    stopped: &AtomicBool,
) -> OperationResult<Arc<AtomicRefCell<VectorStorageEnum>>> {
    let storage = open_simple_dense_vector_storage_impl(
        database,
        database_column_name,
        dim,
        distance,
        stopped,
    )?;

    Ok(Arc::new(AtomicRefCell::new(
        VectorStorageEnum::DenseSimpleHalf(storage),
    )))
}

impl<T: PrimitiveVectorElement> SimpleDenseVectorStorage<T> {
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
                self.deleted_count = self.deleted_count.saturating_sub(1);
            }
        }
        was_deleted
    }

    fn update_stored(
        &mut self,
        key: PointOffsetType,
        deleted: bool,
        vector: Option<&[T]>,
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

impl<T: PrimitiveVectorElement> DenseVectorStorage<T> for SimpleDenseVectorStorage<T> {
    fn vector_dim(&self) -> usize {
        self.dim
    }

    fn get_dense(&self, key: PointOffsetType) -> &[T] {
        self.vectors.get(key)
    }
}

impl<T: PrimitiveVectorElement> VectorStorage for SimpleDenseVectorStorage<T> {
    fn distance(&self) -> Distance {
        self.distance
    }

    fn datatype(&self) -> VectorStorageDatatype {
        T::datatype()
    }

    fn is_on_disk(&self) -> bool {
        false
    }

    fn total_vector_count(&self) -> usize {
        self.vectors.len()
    }

    fn available_size_in_bytes(&self) -> usize {
        self.available_vector_count() * self.vector_dim() * std::mem::size_of::<T>()
    }

    fn get_vector(&self, key: PointOffsetType) -> CowVector {
        CowVector::from(T::slice_to_float_cow(self.vectors.get(key).into()))
    }

    fn insert_vector(&mut self, key: PointOffsetType, vector: VectorRef) -> OperationResult<()> {
        let vector: &[VectorElementType] = vector.try_into()?;
        let vector = T::slice_from_float_cow(Cow::from(vector));
        self.vectors.insert(key, vector.as_ref())?;
        self.set_deleted(key, false);
        self.update_stored(key, false, Some(vector.as_ref()))?;
        Ok(())
    }

    fn update_from(
        &mut self,
        other: &VectorStorageEnum,
        other_ids: &mut impl Iterator<Item = PointOffsetType>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        let start_index = self.vectors.len() as PointOffsetType;
        for point_id in other_ids {
            check_process_stopped(stopped)?;
            // Do not perform preprocessing - vectors should be already processed
            let other_vector = other.get_vector(point_id);
            let other_vector = T::slice_from_float_cow(Cow::try_from(other_vector)?);
            let other_deleted = other.is_deleted_vector(point_id);
            let new_id = self.vectors.push(other_vector.as_ref())?;
            self.set_deleted(new_id, other_deleted);
            self.update_stored(new_id, other_deleted, Some(other_vector.as_ref()))?;
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
