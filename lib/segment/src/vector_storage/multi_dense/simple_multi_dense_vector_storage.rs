use std::ops::Range;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use atomic_refcell::AtomicRefCell;
use bitvec::prelude::{BitSlice, BitVec};
use common::types::PointOffsetType;
use parking_lot::RwLock;
use rocksdb::DB;

use crate::common::operation_error::{check_process_stopped, OperationError, OperationResult};
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::common::Flusher;
use crate::data_types::named_vectors::{CowMultiVector, CowVector};
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{
    TypedMultiDenseVector, TypedMultiDenseVectorRef, VectorElementType, VectorRef,
};
use crate::types::{Distance, MultiVectorConfig, VectorStorageDatatype};
use crate::vector_storage::bitvec::bitvec_set_deleted;
use crate::vector_storage::chunked_vectors::ChunkedVectors;
use crate::vector_storage::common::StoredRecord;
use crate::vector_storage::{MultiVectorStorage, VectorStorage, VectorStorageEnum};

type StoredMultiDenseVector<T> = StoredRecord<TypedMultiDenseVector<T>>;

#[derive(Clone, Default)]
struct MultiVectorMetadata {
    id: PointOffsetType,
    start: PointOffsetType,
    size: usize,
    capacity: usize,
}

/// In-memory vector storage with on-update persistence using `store`
pub struct SimpleMultiDenseVectorStorage<T: PrimitiveVectorElement> {
    dim: usize,
    distance: Distance,
    multi_vector_config: MultiVectorConfig,
    /// Keep vectors in memory
    vectors: ChunkedVectors<T>,
    vectors_metadata: Vec<MultiVectorMetadata>,
    db_wrapper: DatabaseColumnWrapper,
    update_buffer: StoredMultiDenseVector<T>,
    /// BitVec for deleted flags. Grows dynamically upto last set flag.
    deleted: BitVec,
    /// Current number of deleted vectors.
    deleted_count: usize,
}

pub fn open_simple_multi_dense_vector_storage(
    database: Arc<RwLock<DB>>,
    database_column_name: &str,
    dim: usize,
    distance: Distance,
    multi_vector_config: MultiVectorConfig,
    stopped: &AtomicBool,
) -> OperationResult<Arc<AtomicRefCell<VectorStorageEnum>>> {
    let storage = open_simple_multi_dense_vector_storage_impl(
        database,
        database_column_name,
        dim,
        distance,
        multi_vector_config,
        stopped,
    )?;
    Ok(Arc::new(AtomicRefCell::new(
        VectorStorageEnum::MultiDenseSimple(storage),
    )))
}

pub fn open_simple_multi_dense_vector_storage_byte(
    database: Arc<RwLock<DB>>,
    database_column_name: &str,
    dim: usize,
    distance: Distance,
    multi_vector_config: MultiVectorConfig,
    stopped: &AtomicBool,
) -> OperationResult<Arc<AtomicRefCell<VectorStorageEnum>>> {
    let storage = open_simple_multi_dense_vector_storage_impl(
        database,
        database_column_name,
        dim,
        distance,
        multi_vector_config,
        stopped,
    )?;
    Ok(Arc::new(AtomicRefCell::new(
        VectorStorageEnum::MultiDenseSimpleByte(storage),
    )))
}

pub fn open_simple_multi_dense_vector_storage_half(
    database: Arc<RwLock<DB>>,
    database_column_name: &str,
    dim: usize,
    distance: Distance,
    multi_vector_config: MultiVectorConfig,
    stopped: &AtomicBool,
) -> OperationResult<Arc<AtomicRefCell<VectorStorageEnum>>> {
    let storage = open_simple_multi_dense_vector_storage_impl(
        database,
        database_column_name,
        dim,
        distance,
        multi_vector_config,
        stopped,
    )?;
    Ok(Arc::new(AtomicRefCell::new(
        VectorStorageEnum::MultiDenseSimpleHalf(storage),
    )))
}

fn open_simple_multi_dense_vector_storage_impl<T: PrimitiveVectorElement>(
    database: Arc<RwLock<DB>>,
    database_column_name: &str,
    dim: usize,
    distance: Distance,
    multi_vector_config: MultiVectorConfig,
    stopped: &AtomicBool,
) -> OperationResult<SimpleMultiDenseVectorStorage<T>> {
    let mut vectors = ChunkedVectors::new(dim);
    let mut vectors_metadata = Vec::<MultiVectorMetadata>::new();
    let (mut deleted, mut deleted_count) = (BitVec::new(), 0);
    let db_wrapper = DatabaseColumnWrapper::new(database, database_column_name);
    db_wrapper.lock_db().iter()?;
    for (key, value) in db_wrapper.lock_db().iter()? {
        let point_id: PointOffsetType = bincode::deserialize(&key)
            .map_err(|_| OperationError::service_error("cannot deserialize point id from db"))?;
        let stored_record: StoredMultiDenseVector<T> = bincode::deserialize(&value)
            .map_err(|_| OperationError::service_error("cannot deserialize record from db"))?;

        // Propagate deleted flag
        if stored_record.deleted {
            bitvec_set_deleted(&mut deleted, point_id, true);
            deleted_count += 1;
        }
        let point_id_usize = point_id as usize;
        if point_id_usize >= vectors_metadata.len() {
            vectors_metadata.resize(point_id_usize + 1, Default::default());
        }

        let metadata = &mut vectors_metadata[point_id_usize];
        metadata.size = stored_record.vector.flattened_vectors.len();
        metadata.capacity = metadata.size;
        metadata.id = point_id;

        metadata.start = vectors.len() as PointOffsetType;
        let left_keys = vectors.get_chunk_left_keys(metadata.start);
        if stored_record.vector.len() > left_keys {
            metadata.start += left_keys as PointOffsetType;
        }
        vectors.insert_many(
            metadata.start,
            &stored_record.vector.flattened_vectors,
            stored_record.vector.len(),
        )?;

        check_process_stopped(stopped)?;
    }

    Ok(SimpleMultiDenseVectorStorage {
        dim,
        distance,
        multi_vector_config,
        vectors,
        vectors_metadata,
        db_wrapper,
        update_buffer: StoredMultiDenseVector {
            deleted: false,
            vector: TypedMultiDenseVector::placeholder(dim),
        },
        deleted,
        deleted_count,
    })
}

impl<T: PrimitiveVectorElement> SimpleMultiDenseVectorStorage<T> {
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
        vector: Option<TypedMultiDenseVectorRef<T>>,
    ) -> OperationResult<()> {
        // Write vector state to buffer record
        let record = &mut self.update_buffer;
        record.deleted = deleted;
        if let Some(vector) = vector {
            record.vector.dim = vector.dim;
            record.vector.flattened_vectors.clear();
            record
                .vector
                .flattened_vectors
                .extend_from_slice(vector.flattened_vectors);
        } else {
            // reset buffer record
            record.vector.flattened_vectors.clear();
        }

        // Store updated record
        self.db_wrapper.put(
            bincode::serialize(&key).unwrap(),
            bincode::serialize(&record).unwrap(),
        )?;

        Ok(())
    }

    fn insert_vector_impl(
        &mut self,
        key: PointOffsetType,
        vector: VectorRef,
        is_deleted: bool,
    ) -> OperationResult<()> {
        let multi_vector: TypedMultiDenseVectorRef<VectorElementType> = vector.try_into()?;
        let multi_vector = T::from_float_multivector(CowMultiVector::Borrowed(multi_vector));
        let multi_vector = multi_vector.as_vec_ref();
        assert_eq!(multi_vector.dim, self.dim);

        let key_usize = key as usize;
        if key_usize >= self.vectors_metadata.len() {
            self.vectors_metadata
                .resize(key_usize + 1, Default::default());
        }
        let metadata = &mut self.vectors_metadata[key_usize];
        metadata.id = key;
        metadata.size = multi_vector.len();

        if multi_vector.len() > metadata.capacity {
            metadata.capacity = metadata.size;
            metadata.start = self.vectors.len() as PointOffsetType;
            let left_keys = self.vectors.get_chunk_left_keys(metadata.start);
            if multi_vector.len() > left_keys {
                metadata.start += left_keys as PointOffsetType;
            }
            self.vectors.insert_many(
                metadata.start,
                multi_vector.flattened_vectors,
                multi_vector.len(),
            )?;
        } else {
            self.vectors.insert_many(
                metadata.start,
                multi_vector.flattened_vectors,
                multi_vector.len(),
            )?;
        }

        self.set_deleted(key, is_deleted);
        self.update_stored(key, is_deleted, Some(multi_vector))?;
        Ok(())
    }
}

impl<T: PrimitiveVectorElement> MultiVectorStorage<T> for SimpleMultiDenseVectorStorage<T> {
    fn get_multi(&self, key: PointOffsetType) -> TypedMultiDenseVectorRef<T> {
        let metadata = &self.vectors_metadata[key as usize];
        TypedMultiDenseVectorRef {
            flattened_vectors: self.vectors.get_many(metadata.start, metadata.size),
            dim: self.dim,
        }
    }

    fn iterate_inner_vectors(&self) -> impl Iterator<Item = &[T]> + Clone + Send {
        (0..self.total_vector_count()).flat_map(|key| {
            let metadata = &self.vectors_metadata[key];
            (0..metadata.size).map(|i| self.vectors.get(metadata.start as usize + i))
        })
    }

    fn multi_vector_config(&self) -> &MultiVectorConfig {
        &self.multi_vector_config
    }
}

impl<T: PrimitiveVectorElement> VectorStorage for SimpleMultiDenseVectorStorage<T> {
    fn vector_dim(&self) -> usize {
        self.dim
    }

    fn distance(&self) -> Distance {
        self.distance
    }

    fn datatype(&self) -> VectorStorageDatatype {
        VectorStorageDatatype::Float32
    }

    fn is_on_disk(&self) -> bool {
        false
    }

    fn total_vector_count(&self) -> usize {
        self.vectors_metadata.len()
    }

    fn get_vector(&self, key: PointOffsetType) -> CowVector {
        let multi_dense_vector = self.get_multi(key);
        CowVector::MultiDense(T::into_float_multivector(CowMultiVector::Borrowed(
            multi_dense_vector,
        )))
    }

    fn insert_vector(&mut self, key: PointOffsetType, vector: VectorRef) -> OperationResult<()> {
        self.insert_vector_impl(key, vector, false)
    }

    fn update_from(
        &mut self,
        other: &VectorStorageEnum,
        other_ids: &mut impl Iterator<Item = PointOffsetType>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        let start_index = self.vectors_metadata.len() as PointOffsetType;
        for point_id in other_ids {
            check_process_stopped(stopped)?;
            // Do not perform preprocessing - vectors should be already processed
            let other_deleted = other.is_deleted_vector(point_id);
            let other_vector = other.get_vector(point_id);
            let other_vector: VectorRef = other_vector.as_vec_ref();
            let new_id = self.vectors_metadata.len() as PointOffsetType;
            self.insert_vector_impl(new_id, other_vector, other_deleted)?;
        }
        let end_index = self.vectors_metadata.len() as PointOffsetType;
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
