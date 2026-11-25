//! Quantized vectors to test [`IndexBuildVectors::Quantized`] and what reads them.

use std::sync::atomic::AtomicBool;

use common::bitvec::BitVec;
use common::types::PointOffsetType;
use rand::rngs::StdRng;
use rand::{RngExt as _, SeedableRng as _};
use rand_distr::StandardNormal;
use tempfile::TempDir;

use super::IndexBuildVectors;
use crate::data_types::vectors::{VectorElementType, VectorRef};
use crate::index::hnsw_index::point_scorer::FilteredScorer;
use crate::types::{
    BinaryQuantization, BinaryQuantizationConfig, BinaryQuantizationEncoding, Distance,
    QuantizationConfig, ScalarQuantizationConfig, TurboQuantBitSize, TurboQuantQuantizationConfig,
    TurboQuantization,
};
use crate::vector_storage::dense::volatile_dense_vector_storage::new_volatile_dense_vector_storage;
use crate::vector_storage::quantized::quantized_vectors::{
    QuantizedVectors, QuantizedVectorsStorageType,
};
use crate::vector_storage::{VectorStorage as _, VectorStorageEnum};

pub(crate) const POINTS: usize = 200;
/// Exact copies of earlier points, and as many near-copies, whose codes differ at most in what
/// some scores ignore.
pub(crate) const COPIES: usize = 20;
/// Not a multiple of the words, chunks, or padding of any code.
pub(crate) const DIM: usize = 37;

#[expect(deprecated, reason = "always_ram")]
pub(crate) fn scalar() -> QuantizationConfig {
    ScalarQuantizationConfig {
        r#type: Default::default(),
        quantile: None,
        always_ram: None,
        memory: None,
    }
    .into()
}

#[expect(deprecated, reason = "always_ram")]
pub(crate) fn binary(encoding: BinaryQuantizationEncoding) -> QuantizationConfig {
    QuantizationConfig::Binary(BinaryQuantization {
        binary: BinaryQuantizationConfig {
            always_ram: None,
            memory: None,
            encoding: Some(encoding),
            query_encoding: None,
        },
    })
}

#[expect(deprecated, reason = "always_ram")]
pub(crate) fn turbo(bits: TurboQuantBitSize) -> QuantizationConfig {
    QuantizationConfig::Turbo(TurboQuantization {
        turbo: TurboQuantQuantizationConfig {
            always_ram: None,
            memory: None,
            bits: Some(bits),
        },
    })
}

pub(crate) struct Fixture {
    pub vectors: Vec<Vec<f32>>,
    pub storage: VectorStorageEnum,
    pub quantized: QuantizedVectors,
    pub points: Vec<PointOffsetType>,
    deleted: BitVec,
    _dir: TempDir,
}

impl Fixture {
    /// Gaussian vectors, then [`COPIES`] copies of the first ones and as many near-copies of the
    /// next ones, with a NaN in the vector of point 3 if `nan`, quantized with `config`.
    pub fn new(config: &QuantizationConfig, distance: Distance, nan: bool) -> Self {
        let mut rng = StdRng::seed_from_u64(1);
        let mut vectors: Vec<Vec<f32>> = (0..POINTS - 2 * COPIES)
            .map(|_| (0..DIM).map(|_| rng.sample(StandardNormal)).collect())
            .collect();
        vectors.extend_from_within(..COPIES);
        for i in COPIES..2 * COPIES {
            let near = vectors[i]
                .iter()
                .map(|x| x + 1e-3 * rng.sample::<f32, _>(StandardNormal));
            vectors.push(near.collect());
        }
        let mut vectors: Vec<_> = (vectors.into_iter())
            .map(|vector| distance.preprocess_vector::<VectorElementType>(vector))
            .collect();
        if nan {
            vectors[3][5] = f32::NAN;
        }
        let mut storage = new_volatile_dense_vector_storage(DIM, distance);
        for (id, vector) in vectors.iter().enumerate() {
            let id = id as PointOffsetType;
            storage.insert_vector(id, VectorRef::from(vector)).unwrap();
        }
        let dir = tempfile::tempdir().unwrap();
        let quantized = QuantizedVectors::create(
            &storage,
            config,
            QuantizedVectorsStorageType::Immutable,
            dir.path(),
            1,
            &AtomicBool::new(false),
        )
        .unwrap();
        Self {
            vectors,
            storage,
            quantized,
            points: (0..POINTS as PointOffsetType).collect(),
            deleted: BitVec::repeat(false, POINTS),
            _dir: dir,
        }
    }

    pub fn source(&self) -> IndexBuildVectors<'_> {
        IndexBuildVectors::Quantized(&self.quantized)
    }

    /// The scorer of HNSW, which scores the quantized vectors.
    pub fn scorer(&self) -> FilteredScorer<'_> {
        let quantized = Some(&self.quantized);
        FilteredScorer::new_internal(0, &self.storage, quantized, None, &self.deleted).unwrap()
    }

    /// The decoded vectors of all points.
    pub fn decoded(&self) -> Vec<Vec<f32>> {
        let mut decoded = vec![Vec::new(); POINTS];
        (self.source().f32_reader().unwrap())
            .for_each_in_batch(&self.points, &mut |i, vector, _| {
                decoded[i] = vector.to_vec()
            })
            .unwrap();
        decoded
    }
}
