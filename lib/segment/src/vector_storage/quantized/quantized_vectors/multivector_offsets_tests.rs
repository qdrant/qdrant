//! Loading quantized multivector storage must reject offsets that address inner vectors past
//! the end of the quantized storage, or that miss points, such as from a crafted snapshot.

// Deprecated `always_ram` fields must still be set in struct literals
#![allow(deprecated)]

use std::path::Path;
use std::sync::atomic::AtomicBool;

use common::ambient;
use common::generic_consts::Random;
use common::types::PointOffsetType;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use rstest::rstest;

use super::{QuantizedVectorStorage, QuantizedVectors, QuantizedVectorsStorageType};
use crate::common::operation_error::OperationError;
use crate::data_types::vectors::{QueryVector, VectorRef};
use crate::fixtures::payload_fixtures::random_multi_vector;
use crate::types::{
    BinaryQuantizationConfig, CompressionRatio, Distance, Memory, MultiVectorConfig,
    ProductQuantizationConfig, QuantizationConfig, ScalarQuantizationConfig, ScalarType,
    TurboQuantQuantizationConfig, TurboQuantization,
};
use crate::vector_storage::multi_dense::volatile_multi_dense_vector_storage::new_volatile_multi_dense_vector_storage;
use crate::vector_storage::quantized::quantized_multivector_storage::MultivectorOffset;
use crate::vector_storage::{VectorStorage, VectorStorageEnum, VectorStorageRead};

const DIMS: usize = 16;
const NUM_POINTS: usize = 100;
const SEED: u64 = 0x0ff5_e7a5;

#[derive(Clone, Copy, Debug)]
enum Method {
    Scalar,
    Product,
    Binary,
    Turbo,
}

fn config(method: Method, memory: Memory) -> QuantizationConfig {
    match method {
        Method::Scalar => ScalarQuantizationConfig {
            memory: Some(memory),
            r#type: ScalarType::Int8,
            quantile: None,
            always_ram: None,
        }
        .into(),
        Method::Product => ProductQuantizationConfig {
            memory: Some(memory),
            compression: CompressionRatio::X4,
            always_ram: None,
        }
        .into(),
        Method::Binary => BinaryQuantizationConfig {
            memory: Some(memory),
            always_ram: None,
            encoding: None,
            query_encoding: None,
        }
        .into(),
        Method::Turbo => QuantizationConfig::Turbo(TurboQuantization {
            turbo: TurboQuantQuantizationConfig {
                memory: Some(memory),
                always_ram: None,
                bits: None,
            },
        }),
    }
}

/// Whether `storage` is the variant expected for `method` and `memory`, so each case covers the
/// offsets loader it is meant to.
fn is_expected_variant(storage: &QuantizedVectorStorage, method: Method, memory: Memory) -> bool {
    match (method, memory) {
        (Method::Scalar, Memory::Pinned) => {
            matches!(storage, QuantizedVectorStorage::ScalarRamMulti(_))
        }
        (Method::Scalar, _) => matches!(storage, QuantizedVectorStorage::ScalarMmapMulti(_)),
        (Method::Product, Memory::Pinned) => {
            matches!(storage, QuantizedVectorStorage::PQRamMulti(_))
        }
        (Method::Product, _) => matches!(storage, QuantizedVectorStorage::PQMmapMulti(_)),
        (Method::Binary, Memory::Pinned) => {
            matches!(storage, QuantizedVectorStorage::BinaryRamMulti(_))
        }
        (Method::Binary, _) => matches!(storage, QuantizedVectorStorage::BinaryMmapMulti(_)),
        (Method::Turbo, Memory::Pinned) => {
            matches!(storage, QuantizedVectorStorage::TQRamMulti(_))
        }
        (Method::Turbo, _) => matches!(storage, QuantizedVectorStorage::TQMmapMulti(_)),
    }
}

/// Multivector storage with 1 to 4 inner vectors per point, some points deleted.
fn build_storage(rng: &mut StdRng) -> VectorStorageEnum {
    let mut storage =
        new_volatile_multi_dense_vector_storage(DIMS, Distance::Dot, MultiVectorConfig::default());
    for id in 0..NUM_POINTS as PointOffsetType {
        let count = rng.random_range(1..=4);
        let multi = random_multi_vector(rng, DIMS, count);
        storage.insert_vector(id, VectorRef::from(&multi)).unwrap();
    }
    for id in (0..NUM_POINTS as PointOffsetType).step_by(7) {
        storage.delete_vector(id).unwrap();
    }
    storage
}

fn read_offsets(path: &Path) -> Vec<MultivectorOffset> {
    bytemuck::pod_collect_to_vec(&fs_err::read(path).unwrap())
}

fn write_offsets(path: &Path, offsets: &[MultivectorOffset]) {
    fs_err::write(path, bytemuck::cast_slice(offsets)).unwrap();
}

#[derive(Clone, Copy, Debug)]
enum Corruption {
    /// Every offset points far past the inner vectors, as in the advisory
    AllFarPastEnd,
    /// One offset ends one inner vector past the end
    OnePastEnd,
    /// One offset whose end overflows `u32`
    OverflowingEnd,
    /// The file only holds the offset of the first point
    Truncated,
}

impl Corruption {
    fn apply(self, path: &Path) {
        let mut offsets = read_offsets(path);
        let inner_vectors_count = offsets.iter().map(|offset| offset.count).sum::<u32>();
        match self {
            Corruption::AllFarPastEnd => {
                offsets.fill(MultivectorOffset {
                    start: 0xFFFF_FFF0,
                    count: 2,
                });
                write_offsets(path, &offsets);
            }
            Corruption::OnePastEnd => {
                let offset = &mut offsets[NUM_POINTS / 2];
                offset.start = inner_vectors_count - offset.count + 1;
                write_offsets(path, &offsets);
            }
            Corruption::OverflowingEnd => {
                offsets[NUM_POINTS / 2] = MultivectorOffset {
                    start: u32::MAX,
                    count: 2,
                };
                write_offsets(path, &offsets);
            }
            Corruption::Truncated => {
                let file = fs_err::OpenOptions::new().write(true).open(path).unwrap();
                file.set_len(size_of::<MultivectorOffset>() as u64).unwrap();
            }
        }
    }
}

#[rstest]
fn load_validates_multivector_offsets(
    #[values(Method::Scalar, Method::Product, Method::Binary, Method::Turbo)] method: Method,
    #[values(Memory::Pinned, Memory::Cached, Memory::Cold)] memory: Memory,
) {
    let _scope = ambient::test_guard();
    let dir = tempfile::Builder::new()
        .prefix("quant-multi-offsets")
        .tempdir()
        .unwrap();
    let mut rng = StdRng::seed_from_u64(SEED);
    let storage = build_storage(&mut rng);
    let config = config(method, memory);
    let stopped = AtomicBool::new(false);

    let created = QuantizedVectors::create(
        &storage,
        &config,
        QuantizedVectorsStorageType::Immutable,
        dir.path(),
        1,
        &stopped,
    )
    .unwrap();
    assert!(
        is_expected_variant(&created.storage_impl, method, memory),
        "unexpected storage variant for {method:?} {memory:?}",
    );

    // Offsets written by Qdrant itself load, and the storage scores as before
    let loaded = QuantizedVectors::load(&config, &storage, dir.path(), &stopped)
        .unwrap()
        .unwrap();
    assert!(is_expected_variant(&loaded.storage_impl, method, memory));
    {
        let query_id = rng.random_range(0..NUM_POINTS as PointOffsetType);
        let query = QueryVector::Nearest(storage.get_vector::<Random>(query_id).to_owned());
        let created_scorer = created.raw_scorer(query.clone()).unwrap();
        let loaded_scorer = loaded.raw_scorer(query).unwrap();
        for id in 0..NUM_POINTS as PointOffsetType {
            assert_eq!(
                created_scorer.score_point(id),
                loaded_scorer.score_point(id),
                "score mismatch at point {id}",
            );
        }
    }
    drop((created, loaded));

    let offsets_path =
        QuantizedVectors::get_offsets_path(dir.path(), QuantizedVectorsStorageType::Immutable);
    let original = fs_err::read(&offsets_path).unwrap();

    for corruption in [
        Corruption::AllFarPastEnd,
        Corruption::OnePastEnd,
        Corruption::OverflowingEnd,
        Corruption::Truncated,
    ] {
        fs_err::write(&offsets_path, &original).unwrap();
        corruption.apply(&offsets_path);

        let result = QuantizedVectors::load(&config, &storage, dir.path(), &stopped);
        let Err(err) = result else {
            panic!("{method:?} {memory:?} loaded offsets corrupted with {corruption:?}");
        };
        assert!(
            matches!(err, OperationError::InconsistentStorage { .. }),
            "{method:?} {memory:?} {corruption:?}: unexpected error {err}",
        );
    }
}
