use std::borrow::Cow;

use bitvec::prelude::BitVec;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use rand::Rng;

use crate::data_types::named_vectors::CowVector;
use crate::data_types::vectors::{DenseVector, QueryVector, VectorElementType, VectorRef};
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::types::Distance;
use crate::vector_storage::dense::volatile_dense_vector_storage::new_volatile_dense_vector_storage;
use crate::vector_storage::{VectorStorage, VectorStorageEnum};

pub fn random_vector<R: Rng + ?Sized>(rnd_gen: &mut R, size: usize) -> DenseVector {
    (0..size).map(|_| rnd_gen.random_range(-1.0..1.0)).collect()
}

pub struct TestRawScorerProducer {
    storage: VectorStorageEnum,
    deleted_points: BitVec,
}

impl TestRawScorerProducer {
    pub fn new<R>(dim: usize, distance: Distance, num_vectors: usize, rng: &mut R) -> Self
    where
        R: Rng + ?Sized,
    {
        let mut storage = new_volatile_dense_vector_storage(dim, distance);
        let hw_counter = HardwareCounterCell::new();
        for offset in 0..num_vectors as PointOffsetType {
            let rnd_vec = random_vector(rng, dim);
            let rnd_vec = distance.preprocess_vector::<VectorElementType>(rnd_vec);
            storage
                .insert_vector(offset, VectorRef::from(&rnd_vec), &hw_counter)
                .unwrap();
        }

        TestRawScorerProducer {
            storage,
            deleted_points: BitVec::repeat(false, num_vectors),
        }
    }

    pub fn storage(&self) -> &VectorStorageEnum {
        &self.storage
    }

    pub fn get_vector(&self, key: PointOffsetType) -> Cow<[VectorElementType]> {
        match self.storage.get_vector(key) {
            CowVector::Dense(cow) => cow,
            _ => unreachable!("Expected vector storage to be dense"),
        }
    }

    pub fn get_scorer(&self, query: impl Into<QueryVector>) -> FilteredScorer<'_> {
        FilteredScorer::new_for_test(query.into(), &self.storage, &self.deleted_points)
    }

    pub fn internal_scorer(&self, point_id: PointOffsetType) -> FilteredScorer<'_> {
        FilteredScorer::new_internal(
            point_id,
            &self.vector_storage,
            None,
            None,
            &self.deleted_points,
            HardwareCounterCell::new(),
        )
        .unwrap()
    }
}
