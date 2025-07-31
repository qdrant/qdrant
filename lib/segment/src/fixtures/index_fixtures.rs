use std::borrow::Cow;
use std::marker::PhantomData;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;

use bitvec::prelude::{BitSlice, BitVec};
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use rand::Rng;

use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::data_types::named_vectors::CowVector;
use crate::data_types::vectors::{DenseVector, VectorElementType, VectorRef};
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::spaces::metric::Metric;
use crate::types::{Distance, VectorStorageDatatype};
use crate::vector_storage::dense::volatile_dense_vector_storage::new_volatile_dense_vector_storage;
use crate::vector_storage::{VectorStorage, VectorStorageEnum};

pub fn random_vector<R: Rng + ?Sized>(rnd_gen: &mut R, size: usize) -> DenseVector {
    (0..size).map(|_| rnd_gen.random_range(-1.0..1.0)).collect()
}

pub struct TestRawScorerProducer<TMetric: Metric<VectorElementType>> {
    storage: VectorStorageEnum,
    deleted_points: BitVec,
    metric: PhantomData<TMetric>,
}

impl<TMetric: Metric<VectorElementType>> VectorStorage for TestRawScorerProducer<TMetric> {
    fn distance(&self) -> Distance {
        self.storage.distance()
    }

    fn datatype(&self) -> VectorStorageDatatype {
        self.storage.datatype()
    }

    fn is_on_disk(&self) -> bool {
        self.storage.is_on_disk()
    }

    fn total_vector_count(&self) -> usize {
        self.storage.total_vector_count()
    }

    fn get_vector(&self, key: PointOffsetType) -> CowVector<'_> {
        self.storage.get_vector(key)
    }

    fn get_vector_sequential(&self, key: PointOffsetType) -> CowVector<'_> {
        self.storage.get_vector_sequential(key)
    }

    fn get_vector_opt(&self, key: PointOffsetType) -> Option<CowVector<'_>> {
        self.storage.get_vector_opt(key)
    }

    fn insert_vector(
        &mut self,
        key: PointOffsetType,
        vector: VectorRef,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.storage.insert_vector(key, vector, hw_counter)
    }

    fn update_from<'a>(
        &mut self,
        other_vectors: &'a mut impl Iterator<Item = (CowVector<'a>, bool)>,
        stopped: &AtomicBool,
    ) -> OperationResult<Range<PointOffsetType>> {
        self.storage.update_from(other_vectors, stopped)
    }

    fn flusher(&self) -> Flusher {
        self.storage.flusher()
    }

    fn files(&self) -> Vec<PathBuf> {
        self.storage.files()
    }

    fn delete_vector(&mut self, key: PointOffsetType) -> OperationResult<bool> {
        self.storage.delete_vector(key)
    }

    fn is_deleted_vector(&self, key: PointOffsetType) -> bool {
        self.storage.is_deleted_vector(key)
    }

    fn deleted_vector_count(&self) -> usize {
        self.storage.deleted_vector_count()
    }

    fn deleted_vector_bitslice(&self) -> &BitSlice {
        self.storage.deleted_vector_bitslice()
    }
}

impl<TMetric> TestRawScorerProducer<TMetric>
where
    TMetric: Metric<VectorElementType>,
{
    pub fn new<R>(dim: usize, num_vectors: usize, rng: &mut R) -> Self
    where
        R: Rng + ?Sized,
    {
        let mut storage = new_volatile_dense_vector_storage(dim, TMetric::distance());
        let hw_counter = HardwareCounterCell::new();
        for offset in 0..num_vectors as PointOffsetType {
            let rnd_vec = random_vector(rng, dim);
            let rnd_vec = TMetric::preprocess(rnd_vec);
            storage
                .insert_vector(offset, VectorRef::from(&rnd_vec), &hw_counter)
                .unwrap();
        }

        TestRawScorerProducer {
            storage,
            deleted_points: BitVec::repeat(false, num_vectors),
            metric: PhantomData,
        }
    }

    pub fn get_vector(&self, key: PointOffsetType) -> Cow<[VectorElementType]> {
        match self.storage.get_vector(key) {
            CowVector::Dense(cow) => cow,
            _ => unreachable!("Expected vector storage to be dense"),
        }
    }

    pub fn get_scorer(&self, query: DenseVector) -> FilteredScorer<'_> {
        let query = TMetric::preprocess(query).into();
        FilteredScorer::new_for_test(query, &self.storage, &self.deleted_points)
    }
}
