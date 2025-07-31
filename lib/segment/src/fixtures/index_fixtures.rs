use std::sync::atomic::AtomicBool;

use bitvec::prelude::BitVec;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use rand::Rng;

use crate::data_types::vectors::{DenseVector, QueryVector, VectorElementType, VectorRef};
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::types::{Distance, ScalarQuantizationConfig};
use crate::vector_storage::dense::volatile_dense_vector_storage::new_volatile_dense_vector_storage;
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectors;
use crate::vector_storage::{VectorStorage, VectorStorageEnum};

pub fn random_vector<R: Rng + ?Sized>(rnd_gen: &mut R, size: usize) -> DenseVector {
    (0..size).map(|_| rnd_gen.random_range(-1.0..1.0)).collect()
}

pub struct TestRawScorerProducer {
    storage: VectorStorageEnum,
    deleted_points: BitVec,
    quantized_vectors: Option<QuantizedVectors>,
}

impl TestRawScorerProducer {
    pub fn new<R: Rng + ?Sized>(
        dim: usize,
        distance: Distance,
        num_vectors: usize,
        use_quantization: bool,
        rng: &mut R,
    ) -> Self {
        let mut storage = new_volatile_dense_vector_storage(dim, distance);
        let hw_counter = HardwareCounterCell::new();
        for offset in 0..num_vectors as PointOffsetType {
            let rnd_vec = random_vector(rng, dim);
            let rnd_vec = distance.preprocess_vector::<VectorElementType>(rnd_vec);
            storage
                .insert_vector(offset, VectorRef::from(&rnd_vec), &hw_counter)
                .unwrap();
        }

        let quantized_vectors = use_quantization.then(|| {
            QuantizedVectors::create(
                &storage,
                &ScalarQuantizationConfig {
                    r#type: Default::default(),
                    quantile: None,
                    always_ram: Some(true),
                }
                .into(),
                // NOTE: In general case, we should keep the temporary directory
                // as long as the QuantizedVectors instance is alive. But as for
                // now, for this configuration, QuantizedVectors does not touch
                // the file system (except during the creation), so we can drop
                // the directory immediately.
                tempfile::tempdir().unwrap().path(),
                1,
                &AtomicBool::new(false),
            )
            .unwrap()
        });

        TestRawScorerProducer {
            storage,
            deleted_points: BitVec::repeat(false, num_vectors),
            quantized_vectors,
        }
    }

    pub fn storage(&self) -> &VectorStorageEnum {
        &self.storage
    }

    pub fn quantized_vectors(&self) -> Option<&QuantizedVectors> {
        self.quantized_vectors.as_ref()
    }

    pub fn scorer(&self, query: impl Into<QueryVector>) -> FilteredScorer<'_> {
        FilteredScorer::new(
            query.into(),
            &self.storage,
            self.quantized_vectors.as_ref(),
            None,
            &self.deleted_points,
            HardwareCounterCell::new(),
        )
        .unwrap()
    }

    pub fn internal_scorer(&self, point_id: PointOffsetType) -> FilteredScorer<'_> {
        FilteredScorer::new_internal(
            point_id,
            &self.storage,
            self.quantized_vectors.as_ref(),
            None,
            &self.deleted_points,
            HardwareCounterCell::new(),
        )
        .unwrap()
    }
}
