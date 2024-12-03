use std::fmt;
use std::ops::Range;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

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
use crate::vector_storage::chunked_vector_storage::VectorOffsetType;
use crate::vector_storage::chunked_vectors::ChunkedVectors;
use crate::vector_storage::common::{StoredRecord, CHUNK_SIZE, VECTOR_READ_BATCH_SIZE};
use crate::vector_storage::{MultiVectorStorage, VectorStorage, VectorStorageEnum};

type StoredMultiDenseVector<T> = StoredRecord<TypedMultiDenseVector<T>>;

/// All fields are counting vectors and not dimensions.
#[derive(Debug, Clone, Default)]
struct MultiVectorMetadata {
    id: VectorOffsetType,
    start: VectorOffsetType,
    inner_vectors_count: usize,
    inner_vector_capacity: usize,
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
    /// BitVec for deleted flags. Grows dynamically upto last set flag.
    deleted: BitVec,
    /// Current number of deleted vectors.
    deleted_count: usize,
}

impl<T: fmt::Debug + PrimitiveVectorElement> fmt::Debug for SimpleMultiDenseVectorStorage<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SimpleMultiDenseVectorStorage")
            .field("dim", &self.dim)
            .field("distance", &self.distance)
            .field("multi_vector_config", &self.multi_vector_config)
            .field("vectors", &self.vectors)
            .field("vectors_metadata", &self.vectors_metadata)
            .field("db_wrapper", &self.db_wrapper)
            .field("deleted_count", &self.deleted_count)
            .finish_non_exhaustive()
    }
}

pub fn open_simple_multi_dense_vector_storage(
    database: Arc<RwLock<DB>>,
    database_column_name: &str,
    dim: usize,
    distance: Distance,
    multi_vector_config: MultiVectorConfig,
    stopped: &AtomicBool,
) -> OperationResult<VectorStorageEnum> {
    let storage = open_simple_multi_dense_vector_storage_impl(
        database,
        database_column_name,
        dim,
        distance,
        multi_vector_config,
        stopped,
    )?;
    Ok(VectorStorageEnum::MultiDenseSimple(storage))
}

pub fn open_simple_multi_dense_vector_storage_byte(
    database: Arc<RwLock<DB>>,
    database_column_name: &str,
    dim: usize,
    distance: Distance,
    multi_vector_config: MultiVectorConfig,
    stopped: &AtomicBool,
) -> OperationResult<VectorStorageEnum> {
    let storage = open_simple_multi_dense_vector_storage_impl(
        database,
        database_column_name,
        dim,
        distance,
        multi_vector_config,
        stopped,
    )?;
    Ok(VectorStorageEnum::MultiDenseSimpleByte(storage))
}

pub fn open_simple_multi_dense_vector_storage_half(
    database: Arc<RwLock<DB>>,
    database_column_name: &str,
    dim: usize,
    distance: Distance,
    multi_vector_config: MultiVectorConfig,
    stopped: &AtomicBool,
) -> OperationResult<VectorStorageEnum> {
    let storage = open_simple_multi_dense_vector_storage_impl(
        database,
        database_column_name,
        dim,
        distance,
        multi_vector_config,
        stopped,
    )?;
    Ok(VectorStorageEnum::MultiDenseSimpleHalf(storage))
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
        metadata.inner_vectors_count = stored_record.vector.vectors_count();
        metadata.inner_vector_capacity = metadata.inner_vectors_count;
        metadata.id = point_id as VectorOffsetType;

        metadata.start = vectors.len();
        let left_keys = vectors.get_chunk_left_keys(metadata.start);
        if stored_record.vector.vectors_count() > left_keys {
            metadata.start += left_keys;
        }
        vectors.insert_many(
            metadata.start,
            &stored_record.vector.flattened_vectors,
            stored_record.vector.vectors_count(),
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
        deleted,
        deleted_count,
    })
}

impl<T: PrimitiveVectorElement> SimpleMultiDenseVectorStorage<T> {
    /// Set deleted flag for given key. Returns previous deleted state.
    #[inline]
    fn set_deleted(&mut self, key: PointOffsetType, deleted: bool) -> bool {
        if !deleted && key as usize >= self.vectors.len() {
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
        &self,
        key: PointOffsetType,
        deleted: bool,
        vector: Option<TypedMultiDenseVectorRef<T>>,
    ) -> OperationResult<()> {
        let mut record = StoredMultiDenseVector {
            deleted,
            vector: TypedMultiDenseVector::placeholder(self.dim),
        };
        if let Some(vector) = vector {
            record.vector.dim = vector.dim;
            record.vector.flattened_vectors.clear();
            record
                .vector
                .flattened_vectors
                .extend_from_slice(vector.flattened_vectors);
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
        let multivector_size_in_bytes = std::mem::size_of_val(multi_vector.flattened_vectors);
        if multivector_size_in_bytes >= CHUNK_SIZE {
            return Err(OperationError::service_error(format!("Cannot insert multi vector of size {multivector_size_in_bytes} to the vector storage. It's too large, maximum size is {CHUNK_SIZE}.")));
        }

        let key_usize = key as usize;
        if key_usize >= self.vectors_metadata.len() {
            self.vectors_metadata
                .resize(key_usize + 1, Default::default());
        }
        let metadata = &mut self.vectors_metadata[key_usize];
        metadata.id = key as VectorOffsetType;
        metadata.inner_vectors_count = multi_vector.vectors_count();

        if multi_vector.vectors_count() > metadata.inner_vector_capacity {
            metadata.inner_vector_capacity = metadata.inner_vectors_count;
            metadata.start = self.vectors.len();
            let left_keys = self.vectors.get_chunk_left_keys(metadata.start);
            if multi_vector.vectors_count() > left_keys {
                metadata.start += left_keys;
            }
            self.vectors.insert_many(
                metadata.start,
                multi_vector.flattened_vectors,
                multi_vector.vectors_count(),
            )?;
        } else {
            self.vectors.insert_many(
                metadata.start,
                multi_vector.flattened_vectors,
                multi_vector.vectors_count(),
            )?;
        }

        self.set_deleted(key, is_deleted);
        self.update_stored(key, is_deleted, Some(multi_vector))?;
        Ok(())
    }
}

impl<T: PrimitiveVectorElement> MultiVectorStorage<T> for SimpleMultiDenseVectorStorage<T> {
    fn vector_dim(&self) -> usize {
        self.dim
    }

    /// Panics if key is out of bounds
    fn get_multi(&self, key: PointOffsetType) -> TypedMultiDenseVectorRef<T> {
        self.get_multi_opt(key).expect("vector not found")
    }

    /// None if key is out of bounds
    fn get_multi_opt(&self, key: PointOffsetType) -> Option<TypedMultiDenseVectorRef<T>> {
        self.vectors_metadata.get(key as usize).map(|metadata| {
            let flattened_vectors = self
                .vectors
                .get_many(metadata.start, metadata.inner_vectors_count)
                .unwrap_or_else(|| panic!("Vectors does not contain data for {metadata:?}"));
            TypedMultiDenseVectorRef {
                flattened_vectors,
                dim: self.dim,
            }
        })
    }

    fn get_batch_multi<'a>(
        &'a self,
        keys: &[PointOffsetType],
        vectors: &mut [TypedMultiDenseVectorRef<'a, T>],
    ) {
        debug_assert_eq!(keys.len(), vectors.len());
        debug_assert!(keys.len() <= VECTOR_READ_BATCH_SIZE);

        for (i, key) in keys.iter().enumerate() {
            vectors[i] = self.get_multi(*key);
        }
    }

    fn iterate_inner_vectors(&self) -> impl Iterator<Item = &[T]> + Clone + Send {
        (0..self.total_vector_count()).flat_map(|key| {
            let metadata = &self.vectors_metadata[key];
            (0..metadata.inner_vectors_count).map(|i| self.vectors.get(metadata.start + i))
        })
    }

    fn multi_vector_config(&self) -> &MultiVectorConfig {
        &self.multi_vector_config
    }
}

impl<T: PrimitiveVectorElement> VectorStorage for SimpleMultiDenseVectorStorage<T> {
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
        self.insert_vector_impl(key, vector, false)
    }

    fn update_from<'a>(
        &mut self,
        other_ids: &'a mut impl Iterator<Item = (CowVector<'a>, bool)>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        let start_index = self.vectors_metadata.len() as PointOffsetType;
        for (other_vector, other_deleted) in other_ids {
            check_process_stopped(stopped)?;
            // Do not perform preprocessing - vectors should be already processed
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
