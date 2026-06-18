//! Unit tests for GPU vector storage, covering f32, f16, u8 dense/multivectors with SQ, BQ, PQ.

use common::counter::hardware_counter::HardwareCounterCell;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use rstest::rstest;

use super::*;
use crate::data_types::vectors::{MultiDenseVectorInternal, QueryVector, VectorRef};
use crate::fixtures::index_fixtures::random_vector;
use crate::fixtures::payload_fixtures::random_dense_byte_vector;
use crate::index::hnsw_index::gpu::shader_builder::ShaderBuilder;
use crate::types::{
    BinaryQuantization, BinaryQuantizationConfig, BinaryQuantizationEncoding, Distance,
    ProductQuantization, ProductQuantizationConfig, QuantizationConfig, ScalarQuantization,
    ScalarQuantizationConfig, TurboQuantBitSize, TurboQuantQuantizationConfig, TurboQuantization,
};
use crate::vector_storage::dense::volatile_dense_vector_storage::{
    new_volatile_dense_byte_vector_storage, new_volatile_dense_half_vector_storage,
    new_volatile_dense_vector_storage,
};
use crate::vector_storage::multi_dense::volatile_multi_dense_vector_storage::{
    new_volatile_multi_dense_vector_storage, new_volatile_multi_dense_vector_storage_byte,
    new_volatile_multi_dense_vector_storage_half,
};
use crate::vector_storage::quantized::quantized_vectors::QuantizedVectorsStorageType;
use crate::vector_storage::turbo::multi::open_appendable_turbo_multi_vector_storage;
use crate::vector_storage::turbo::open_appendable_turbo_vector_storage;
use crate::vector_storage::{DEFAULT_STOPPED, RawScorer, VectorStorage, new_raw_scorer_for_test};

#[derive(Debug, Clone, Copy)]
enum TestElementType {
    Float32,
    Float16,
    Uint8,
}

#[derive(Debug, Clone, Copy)]
enum TestStorageType {
    Dense(TestElementType),
    Multi(TestElementType),
}

impl TestStorageType {
    pub fn element_type(&self) -> TestElementType {
        match self {
            TestStorageType::Dense(element_type) => *element_type,
            TestStorageType::Multi(element_type) => *element_type,
        }
    }
}

#[rstest]
#[case::cosine_f32(
    Distance::Cosine,
    TestStorageType::Dense(TestElementType::Float32),
    273,
    2057
)]
#[case::dot_f32(
    Distance::Dot,
    TestStorageType::Dense(TestElementType::Float32),
    256,
    512
)]
#[case::euclid_f32(
    Distance::Euclid,
    TestStorageType::Dense(TestElementType::Float32),
    273,
    2057
)]
#[case::manhattan_f32(
    Distance::Manhattan,
    TestStorageType::Dense(TestElementType::Float32),
    273,
    2057
)]
#[case::small_dimension(
    Distance::Cosine,
    TestStorageType::Dense(TestElementType::Float32),
    17,
    2057
)]
#[case::cosine_f16(
    Distance::Cosine,
    TestStorageType::Dense(TestElementType::Float16),
    273,
    2057
)]
#[case::cosine_u8(
    Distance::Cosine,
    TestStorageType::Dense(TestElementType::Uint8),
    273,
    2057
)]
#[case::cosine_multi_f32(
    Distance::Cosine,
    TestStorageType::Multi(TestElementType::Float32),
    67,
    2057
)]
#[case::cosine_multi_u8(
    Distance::Cosine,
    TestStorageType::Multi(TestElementType::Uint8),
    273,
    2057
)]
fn test_gpu_vector_storage_sq(
    #[case] distance: Distance,
    #[case] storage_type: TestStorageType,
    #[case] dim: usize,
    #[case] num_vectors: usize,
) {
    let _ = env_logger::builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Trace)
        .try_init();

    let quantization_config = QuantizationConfig::Scalar(ScalarQuantization {
        scalar: ScalarQuantizationConfig {
            always_ram: Some(true),
            r#type: crate::types::ScalarType::Int8,
            quantile: Some(0.99),
        },
    });

    let precision = get_precision(storage_type, dim, distance);
    log::info!(
        "Testing SQ distance {distance:?}, element type {storage_type:?}, dim {dim} with precision {precision}"
    );
    test_gpu_vector_storage_impl(
        storage_type,
        num_vectors,
        dim,
        distance,
        Some(quantization_config.clone()),
        false,
        false,
        precision,
    );
}

#[rstest]
#[case::cosine_f32_one_bit(
    Distance::Cosine,
    TestStorageType::Dense(TestElementType::Float32),
    273,
    2057,
    BinaryQuantizationEncoding::OneBit
)]
#[case::dot_f32_one_and_half_bits(
    Distance::Dot,
    TestStorageType::Dense(TestElementType::Float32),
    256,
    512,
    BinaryQuantizationEncoding::OneAndHalfBits
)]
#[case::euclid_f32(
    Distance::Euclid,
    TestStorageType::Dense(TestElementType::Float32),
    273,
    2057,
    BinaryQuantizationEncoding::OneBit
)]
#[case::manhattan_f32_two_bits(
    Distance::Manhattan,
    TestStorageType::Dense(TestElementType::Float32),
    273,
    2057,
    BinaryQuantizationEncoding::TwoBits
)]
#[case::small_dimension(
    Distance::Cosine,
    TestStorageType::Dense(TestElementType::Float32),
    17,
    2057,
    BinaryQuantizationEncoding::OneBit
)]
#[case::cosine_f16(
    Distance::Cosine,
    TestStorageType::Dense(TestElementType::Float16),
    273,
    2057,
    BinaryQuantizationEncoding::OneBit
)]
#[case::cosine_u8(
    Distance::Cosine,
    TestStorageType::Dense(TestElementType::Uint8),
    273,
    2057,
    BinaryQuantizationEncoding::OneBit
)]
#[case::cosine_multi_f32(
    Distance::Cosine,
    TestStorageType::Multi(TestElementType::Float32),
    67,
    2057,
    BinaryQuantizationEncoding::OneBit
)]
#[case::cosine_multi_u8(
    Distance::Cosine,
    TestStorageType::Multi(TestElementType::Uint8),
    273,
    2057,
    BinaryQuantizationEncoding::OneBit
)]
fn test_gpu_vector_storage_bq(
    #[case] distance: Distance,
    #[case] storage_type: TestStorageType,
    #[case] dim: usize,
    #[case] num_vectors: usize,
    #[case] encoding: BinaryQuantizationEncoding,
) {
    let _ = env_logger::builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Trace)
        .try_init();

    let quantization_config = QuantizationConfig::Binary(BinaryQuantization {
        binary: BinaryQuantizationConfig {
            always_ram: Some(true),
            encoding: Some(encoding),
            query_encoding: None,
        },
    });

    let precision = get_precision(storage_type, dim, distance);
    log::info!(
        "Testing BQ distance {distance:?}, element type {storage_type:?}, dim {dim} with precision {precision}"
    );
    test_gpu_vector_storage_impl(
        storage_type,
        num_vectors,
        dim,
        distance,
        Some(quantization_config.clone()),
        false,
        false,
        precision,
    );
}

#[rstest]
#[case::cosine_f32(
    Distance::Cosine,
    TestStorageType::Dense(TestElementType::Float32),
    17,
    2057
)]
#[case::dot_f32(
    Distance::Dot,
    TestStorageType::Dense(TestElementType::Float32),
    17,
    512
)]
#[case::euclid_f32(
    Distance::Euclid,
    TestStorageType::Dense(TestElementType::Float32),
    17,
    2057
)]
#[case::manhattan_f32(
    Distance::Manhattan,
    TestStorageType::Dense(TestElementType::Float32),
    17,
    2057
)]
#[case::large_dimension(
    Distance::Cosine,
    TestStorageType::Dense(TestElementType::Float32),
    129,
    1095
)]
#[case::cosine_f16(
    Distance::Cosine,
    TestStorageType::Dense(TestElementType::Float16),
    17,
    2057
)]
#[case::cosine_u8(
    Distance::Cosine,
    TestStorageType::Dense(TestElementType::Uint8),
    17,
    2057
)]
#[case::cosine_multi_f32(
    Distance::Cosine,
    TestStorageType::Multi(TestElementType::Float32),
    17,
    2057
)]
#[case::cosine_multi_u8(
    Distance::Cosine,
    TestStorageType::Multi(TestElementType::Uint8),
    17,
    2057
)]
fn test_gpu_vector_storage_pq(
    #[case] distance: Distance,
    #[case] storage_type: TestStorageType,
    #[case] dim: usize,
    #[case] num_vectors: usize,
) {
    let _ = env_logger::builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Trace)
        .try_init();

    let quantization_config = QuantizationConfig::Product(ProductQuantization {
        product: ProductQuantizationConfig {
            always_ram: Some(true),
            compression: crate::types::CompressionRatio::X8,
        },
    });

    let precision = get_precision(storage_type, dim, distance);
    log::info!(
        "Testing PQ distance {distance:?}, element type {storage_type:?}, dim {dim} with precision {precision}"
    );
    test_gpu_vector_storage_impl(
        storage_type,
        num_vectors,
        dim,
        distance,
        Some(quantization_config.clone()),
        false,
        false,
        precision,
    );
}

#[rstest]
#[case::cosine_f32(
    Distance::Cosine,
    TestStorageType::Dense(TestElementType::Float32),
    273,
    2057
)]
#[case::dot_f32(
    Distance::Dot,
    TestStorageType::Dense(TestElementType::Float32),
    256,
    512
)]
#[case::euclid_f32(
    Distance::Euclid,
    TestStorageType::Dense(TestElementType::Float32),
    273,
    2057
)]
#[case::manhattan_f32(
    Distance::Manhattan,
    TestStorageType::Dense(TestElementType::Float32),
    273,
    2057
)]
#[case::small_dimension(
    Distance::Cosine,
    TestStorageType::Dense(TestElementType::Float32),
    17,
    2057
)]
#[case::cosine_f16(
    Distance::Cosine,
    TestStorageType::Dense(TestElementType::Float16),
    273,
    2057
)]
#[case::cosine_u8(
    Distance::Cosine,
    TestStorageType::Dense(TestElementType::Uint8),
    273,
    2057
)]
#[case::cosine_multi_f32(
    Distance::Cosine,
    TestStorageType::Multi(TestElementType::Float32),
    67,
    2057
)]
#[case::cosine_multi_u8(
    Distance::Cosine,
    TestStorageType::Multi(TestElementType::Uint8),
    273,
    2057
)]
fn test_gpu_vector_storage(
    #[case] distance: Distance,
    #[case] storage_type: TestStorageType,
    #[case] dim: usize,
    #[case] num_vectors: usize,
) {
    let _ = env_logger::builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Trace)
        .try_init();

    let precision = get_precision(storage_type, dim, distance);
    log::info!(
        "Testing distance {distance:?}, element type {storage_type:?}, dim {dim} with precision {precision}"
    );
    test_gpu_vector_storage_impl(
        storage_type,
        num_vectors,
        dim,
        distance,
        None,
        false,
        false,
        precision,
    );
}

#[rstest]
#[case::cosine_dense(
    Distance::Cosine,
    TestStorageType::Dense(TestElementType::Float32),
    273,
    2057
)]
#[case::cosine_multi(
    Distance::Cosine,
    TestStorageType::Multi(TestElementType::Float32),
    67,
    2057
)]
fn test_gpu_vector_storage_force_half(
    #[case] distance: Distance,
    #[case] storage_type: TestStorageType,
    #[case] dim: usize,
    #[case] num_vectors: usize,
) {
    let _ = env_logger::builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Trace)
        .try_init();

    let precision = 5.0 * get_precision(storage_type, dim, distance);
    log::info!(
        "Testing distance {distance:?}, element type {storage_type:?}, dim {dim} with precision {precision}"
    );
    test_gpu_vector_storage_impl(
        storage_type,
        num_vectors,
        dim,
        distance,
        None,
        true, // force half precision
        false,
        precision,
    );
}

#[rstest]
#[case::dense_f32(
    Distance::Cosine,
    TestStorageType::Dense(TestElementType::Float32),
    273,
    2057
)]
#[case::dense_f16(
    Distance::Cosine,
    TestStorageType::Dense(TestElementType::Float16),
    273,
    2057
)]
#[case::multi_f32(
    Distance::Cosine,
    TestStorageType::Multi(TestElementType::Float32),
    67,
    2057
)]
#[case::multi_f16(
    Distance::Cosine,
    TestStorageType::Multi(TestElementType::Float16),
    67,
    2057
)]
fn test_gpu_vector_storage_without_half(
    #[case] distance: Distance,
    #[case] storage_type: TestStorageType,
    #[case] dim: usize,
    #[case] num_vectors: usize,
) {
    let _ = env_logger::builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Trace)
        .try_init();

    let precision = 5.0 * get_precision(storage_type, dim, distance);
    log::info!(
        "Testing distance {distance:?}, element type {storage_type:?}, dim {dim} with precision {precision}"
    );
    test_gpu_vector_storage_impl(
        storage_type,
        num_vectors,
        dim,
        distance,
        None,
        true, // force half precision
        true, // skip half support
        precision,
    );
}

#[rstest]
#[case::dense_f32(
    Distance::Cosine,
    TestStorageType::Dense(TestElementType::Float32),
    273,
    2057,
    false // skip_half_support
)]
#[case::dense_f32_no_half(
    Distance::Cosine,
    TestStorageType::Dense(TestElementType::Float32),
    273,
    2057,
    true
)]
#[case::multi_f32(
    Distance::Cosine,
    TestStorageType::Multi(TestElementType::Float32),
    67,
    2057,
    false
)]
#[case::multi_f32_no_half(
    Distance::Cosine,
    TestStorageType::Multi(TestElementType::Float32),
    67,
    2057,
    true
)]
fn test_gpu_vector_storage_tq_falls_back_to_half_precision(
    #[case] distance: Distance,
    #[case] storage_type: TestStorageType,
    #[case] dim: usize,
    #[case] num_vectors: usize,
    #[case] skip_half_support: bool,
) {
    let _ = env_logger::builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Trace)
        .try_init();

    // TurboQuant is not supported on GPU: `GpuVectorStorage::new` returns `Ok(None)` from
    // `new_quantized` for any TQ variant and falls back to building from the unquantized
    // storage with `force_half_precision = true` hardcoded. This test pins that fallback by
    // checking that the resulting GPU storage's element type is half precision (or f32 if the
    // device has no f16 support) — never `Uint8`, which is what SQ/PQ/BQ on GPU produce.
    let dir = tempfile::Builder::new().prefix("db_dir").tempdir().unwrap();
    let storage = create_vector_storage(storage_type, num_vectors, dim, distance);

    let tq_config = QuantizationConfig::Turbo(TurboQuantization {
        turbo: TurboQuantQuantizationConfig {
            always_ram: Some(true),
            bits: Some(TurboQuantBitSize::Bits4),
        },
    });
    let tq_vectors = QuantizedVectors::create(
        &storage,
        &tq_config,
        QuantizedVectorsStorageType::Immutable,
        dir.path(),
        1,
        &DEFAULT_STOPPED,
    )
    .unwrap();

    let instance = gpu::GPU_TEST_INSTANCE.clone();
    let device = gpu::Device::new_with_params(
        instance.clone(),
        &instance.physical_devices()[0],
        0,
        skip_half_support,
    )
    .unwrap();

    // `force_half_precision = false` from the caller — any half precision in the result must
    // come from the TQ fallback inside `new`, not from this argument.
    let gpu_via_tq = GpuVectorStorage::new(
        device.clone(),
        &storage,
        Some(&tq_vectors),
        false,
        &DEFAULT_STOPPED,
    )
    .unwrap();

    let expected = if device.has_half_precision() {
        VectorStorageDatatype::Float16
    } else {
        VectorStorageDatatype::Float32
    };
    assert_eq!(
        gpu_via_tq.element_type, expected,
        "TurboQuant on GPU did not pick the half-precision float type",
    );
}

/// A primary `VectorStorageEnum::DenseTurbo` storage built on GPU dequantizes
/// each encoded vector back to float and uploads it as a regular dense storage.
/// This pins that path by comparing GPU scores of the TurboQuant storage against
/// GPU scores of a plain `f32` storage holding the *same* dequantized vectors —
/// the two must be numerically identical.
#[rstest]
#[case::cosine(Distance::Cosine, 273, 1037)]
#[case::dot(Distance::Dot, 256, 512)]
#[case::euclid(Distance::Euclid, 273, 1037)]
#[case::manhattan(Distance::Manhattan, 17, 1037)]
fn test_gpu_vector_storage_turbo_dense(
    #[case] distance: Distance,
    #[case] dim: usize,
    #[case] num_vectors: usize,
) {
    let _ = env_logger::builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Trace)
        .try_init();

    let dir = tempfile::Builder::new().prefix("tq_dir").tempdir().unwrap();
    let mut turbo = open_appendable_turbo_vector_storage(dir.path(), dim, distance, true).unwrap();

    let mut rnd = StdRng::seed_from_u64(42);
    for i in 0..num_vectors {
        let vec = distance.preprocess_vector::<VectorElementType>(random_vector(&mut rnd, dim));
        turbo
            .insert_vector(
                i as PointOffsetType,
                VectorRef::from(&vec),
                &HardwareCounterCell::new(),
            )
            .unwrap();
    }

    // Reference plain-`f32` storage with the dequantized vectors the GPU uploads.
    let mut reference = new_volatile_dense_vector_storage(dim, distance);
    for i in 0..num_vectors {
        let dequantized = turbo.get_dense_for_requantization(i as PointOffsetType, false);
        reference
            .insert_vector(
                i as PointOffsetType,
                VectorRef::from(&dequantized),
                &HardwareCounterCell::new(),
            )
            .unwrap();
    }

    let turbo_storage = VectorStorageEnum::DenseTurbo(Box::new(turbo));

    let instance = gpu::GPU_TEST_INSTANCE.clone();
    let device =
        gpu::Device::new_with_params(instance.clone(), &instance.physical_devices()[0], 0, false)
            .unwrap();

    // `force_half_precision = false` keeps both storages on `f32` so the only
    // difference under test is the dequantize-and-upload path, not f16 rounding.
    let gpu_turbo = GpuVectorStorage::new(
        device.clone(),
        &turbo_storage,
        None,
        false,
        &DEFAULT_STOPPED,
    )
    .unwrap();
    let gpu_reference =
        GpuVectorStorage::new(device.clone(), &reference, None, false, &DEFAULT_STOPPED).unwrap();

    assert_eq!(gpu_turbo.element_type, VectorStorageDatatype::Float32);
    assert_eq!(gpu_turbo.num_vectors(), num_vectors);

    let turbo_scores = run_gpu_scoring(device.clone(), &gpu_turbo, num_vectors);
    let reference_scores = run_gpu_scoring(device.clone(), &gpu_reference, num_vectors);

    for (turbo_score, reference_score) in turbo_scores.iter().zip(reference_scores.iter()) {
        assert!(
            (turbo_score - reference_score).abs() < 1e-4,
            "turbo GPU score {turbo_score} != reference GPU score {reference_score}",
        );
    }
}

/// Multivector counterpart of [`test_gpu_vector_storage_turbo_dense`].
#[rstest]
#[case::cosine(Distance::Cosine, 67, 1037)]
#[case::dot(Distance::Dot, 67, 512)]
fn test_gpu_vector_storage_turbo_multi(
    #[case] distance: Distance,
    #[case] dim: usize,
    #[case] num_points: usize,
) {
    let _ = env_logger::builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Trace)
        .try_init();

    let multi_config = Default::default();
    let dir = tempfile::Builder::new().prefix("tq_dir").tempdir().unwrap();
    let mut turbo =
        open_appendable_turbo_multi_vector_storage(dir.path(), dim, distance, multi_config, true)
            .unwrap();

    let mut rnd = StdRng::seed_from_u64(42);
    for i in 0..num_points {
        let inner_count = 1 + rnd.random::<u8>() % 3;
        let mut flattened = vec![];
        for _ in 0..inner_count {
            flattened.extend(
                distance.preprocess_vector::<VectorElementType>(random_vector(&mut rnd, dim)),
            );
        }
        let multivector = MultiDenseVectorInternal::new(flattened, dim);
        turbo
            .insert_vector(
                i as PointOffsetType,
                VectorRef::from(&multivector),
                &HardwareCounterCell::new(),
            )
            .unwrap();
    }

    // Reference multivector storage with the dequantized inner vectors.
    let mut reference = new_volatile_multi_dense_vector_storage(dim, distance, multi_config);
    for i in 0..num_points {
        let inner = turbo.get_inner_dense_for_requantization(i as PointOffsetType, false);
        let flattened: Vec<VectorElementType> = inner.into_iter().flatten().collect();
        let multivector = MultiDenseVectorInternal::new(flattened, dim);
        reference
            .insert_vector(
                i as PointOffsetType,
                VectorRef::from(&multivector),
                &HardwareCounterCell::new(),
            )
            .unwrap();
    }

    let turbo_storage = VectorStorageEnum::MultiDenseTurbo(Box::new(turbo));

    let instance = gpu::GPU_TEST_INSTANCE.clone();
    let device =
        gpu::Device::new_with_params(instance.clone(), &instance.physical_devices()[0], 0, false)
            .unwrap();

    let gpu_turbo = GpuVectorStorage::new(
        device.clone(),
        &turbo_storage,
        None,
        false,
        &DEFAULT_STOPPED,
    )
    .unwrap();
    let gpu_reference =
        GpuVectorStorage::new(device.clone(), &reference, None, false, &DEFAULT_STOPPED).unwrap();

    assert_eq!(gpu_turbo.element_type, VectorStorageDatatype::Float32);
    assert_eq!(gpu_turbo.num_vectors(), num_points);

    let turbo_scores = run_gpu_scoring(device.clone(), &gpu_turbo, num_points);
    let reference_scores = run_gpu_scoring(device.clone(), &gpu_reference, num_points);

    for (turbo_score, reference_score) in turbo_scores.iter().zip(reference_scores.iter()) {
        assert!(
            (turbo_score - reference_score).abs() < 1e-4,
            "turbo GPU score {turbo_score} != reference GPU score {reference_score}",
        );
    }
}

fn get_precision(storage_type: TestStorageType, dim: usize, distance: Distance) -> f32 {
    let distance_persision = match distance {
        Distance::Cosine => 0.01,
        Distance::Dot => 0.01,
        Distance::Euclid => dim as f32 * 0.001,
        Distance::Manhattan => dim as f32 * 0.001,
    };
    match storage_type.element_type() {
        TestElementType::Float32 => distance_persision,
        TestElementType::Float16 => distance_persision * 5.0,
        TestElementType::Uint8 => distance_persision * 10.0,
    }
}

fn create_vector_storage(
    storage_type: TestStorageType,
    num_vectors: usize,
    dim: usize,
    distance: Distance,
) -> VectorStorageEnum {
    match storage_type {
        TestStorageType::Dense(TestElementType::Float32) => {
            create_vector_storage_f32(num_vectors, dim, distance)
        }
        TestStorageType::Dense(TestElementType::Float16) => {
            create_vector_storage_f16(num_vectors, dim, distance)
        }
        TestStorageType::Dense(TestElementType::Uint8) => {
            create_vector_storage_u8(num_vectors, dim, distance)
        }
        TestStorageType::Multi(TestElementType::Float32) => {
            create_vector_storage_f32_multi(num_vectors, dim, distance)
        }
        TestStorageType::Multi(TestElementType::Float16) => {
            create_vector_storage_f16_multi(num_vectors, dim, distance)
        }
        TestStorageType::Multi(TestElementType::Uint8) => {
            create_vector_storage_u8_multi(num_vectors, dim, distance)
        }
    }
}

fn create_vector_storage_f32(
    num_vectors: usize,
    dim: usize,
    distance: Distance,
) -> VectorStorageEnum {
    let mut rnd = StdRng::seed_from_u64(42);
    let mut vector_storage = new_volatile_dense_vector_storage(dim, distance);
    for i in 0..num_vectors {
        let vec = random_vector(&mut rnd, dim);
        let vec = distance.preprocess_vector::<VectorElementType>(vec);
        let vec_ref = VectorRef::from(&vec);
        vector_storage
            .insert_vector(i as PointOffsetType, vec_ref, &HardwareCounterCell::new())
            .unwrap();
    }
    vector_storage
}

fn create_vector_storage_f16(
    num_vectors: usize,
    dim: usize,
    distance: Distance,
) -> VectorStorageEnum {
    let mut rnd = StdRng::seed_from_u64(42);
    let mut vector_storage = new_volatile_dense_half_vector_storage(dim, distance);
    for i in 0..num_vectors {
        let vec = random_vector(&mut rnd, dim);
        let vec = distance.preprocess_vector::<VectorElementTypeHalf>(vec);
        let vec_ref = VectorRef::from(&vec);
        vector_storage
            .insert_vector(i as PointOffsetType, vec_ref, &HardwareCounterCell::new())
            .unwrap();
    }
    vector_storage
}

fn create_vector_storage_u8(
    num_vectors: usize,
    dim: usize,
    distance: Distance,
) -> VectorStorageEnum {
    let mut rnd = StdRng::seed_from_u64(42);
    let mut vector_storage = new_volatile_dense_byte_vector_storage(dim, distance);
    for i in 0..num_vectors {
        let vec = random_dense_byte_vector(&mut rnd, dim);
        let vec = distance.preprocess_vector::<VectorElementTypeByte>(vec);
        let vec_ref = VectorRef::from(&vec);
        vector_storage
            .insert_vector(i as PointOffsetType, vec_ref, &HardwareCounterCell::new())
            .unwrap();
    }
    vector_storage
}

fn create_vector_storage_f32_multi(
    num_vectors: usize,
    dim: usize,
    distance: Distance,
) -> VectorStorageEnum {
    let mut rnd = StdRng::seed_from_u64(42);
    let multivector_config = Default::default();
    let mut vector_storage =
        new_volatile_multi_dense_vector_storage(dim, distance, multivector_config);
    for i in 0..num_vectors {
        let mut vectors = vec![];
        let num_vectors_per_points = 1 + rnd.random::<u8>() % 3;
        for _ in 0..num_vectors_per_points {
            let vec = random_vector(&mut rnd, dim);
            let vec = distance.preprocess_vector::<VectorElementType>(vec);
            vectors.extend(vec);
        }
        let multivector = MultiDenseVectorInternal::new(vectors, dim);
        let vec_ref = VectorRef::from(&multivector);
        vector_storage
            .insert_vector(i as PointOffsetType, vec_ref, &HardwareCounterCell::new())
            .unwrap();
    }
    vector_storage
}

fn create_vector_storage_f16_multi(
    num_vectors: usize,
    dim: usize,
    distance: Distance,
) -> VectorStorageEnum {
    let mut rnd = StdRng::seed_from_u64(42);
    let multivector_config = Default::default();
    let mut vector_storage =
        new_volatile_multi_dense_vector_storage_half(dim, distance, multivector_config);
    for i in 0..num_vectors {
        let mut vectors = vec![];
        let num_vectors_per_points = 1 + rnd.random::<u8>() % 3;
        for _ in 0..num_vectors_per_points {
            let vec = random_vector(&mut rnd, dim);
            let vec = distance.preprocess_vector::<VectorElementTypeHalf>(vec);
            vectors.extend(vec);
        }
        let multivector = MultiDenseVectorInternal::new(vectors, dim);
        let vec_ref = VectorRef::from(&multivector);
        vector_storage
            .insert_vector(i as PointOffsetType, vec_ref, &HardwareCounterCell::new())
            .unwrap();
    }
    vector_storage
}

fn create_vector_storage_u8_multi(
    num_vectors: usize,
    dim: usize,
    distance: Distance,
) -> VectorStorageEnum {
    let mut rnd = StdRng::seed_from_u64(42);
    let multivector_config = Default::default();
    let mut vector_storage =
        new_volatile_multi_dense_vector_storage_byte(dim, distance, multivector_config);
    for i in 0..num_vectors {
        let mut vectors = vec![];
        let num_vectors_per_points = 1 + rnd.random::<u8>() % 3;
        for _ in 0..num_vectors_per_points {
            let vec = random_dense_byte_vector(&mut rnd, dim);
            let vec = distance.preprocess_vector::<VectorElementTypeByte>(vec);
            vectors.extend(vec);
        }
        let multivector = MultiDenseVectorInternal::new(vectors, dim);
        let vec_ref = VectorRef::from(&multivector);
        vector_storage
            .insert_vector(i as PointOffsetType, vec_ref, &HardwareCounterCell::new())
            .unwrap();
    }
    vector_storage
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
fn test_gpu_vector_storage_impl(
    storage_type: TestStorageType,
    num_vectors: usize,
    dim: usize,
    distance: Distance,
    quantization_config: Option<QuantizationConfig>,
    force_half_precision: bool,
    skip_half_support: bool,
    precision: f32,
) {
    let test_point_id: PointOffsetType = 0;

    let dir = tempfile::Builder::new().prefix("db_dir").tempdir().unwrap();
    let storage = create_vector_storage(storage_type, num_vectors, dim, distance);

    let quantized_vectors = quantization_config.as_ref().map(|quantization_config| {
        QuantizedVectors::create(
            &storage,
            quantization_config,
            QuantizedVectorsStorageType::Immutable,
            dir.path(),
            1,
            &DEFAULT_STOPPED,
        )
        .unwrap()
    });

    let instance = gpu::GPU_TEST_INSTANCE.clone();
    let device = gpu::Device::new_with_params(
        instance.clone(),
        &instance.physical_devices()[0],
        0,
        skip_half_support,
    )
    .unwrap();

    let gpu_vector_storage = GpuVectorStorage::new(
        device.clone(),
        &storage,
        quantized_vectors.as_ref(),
        force_half_precision,
        &DEFAULT_STOPPED,
    )
    .unwrap();

    assert_eq!(gpu_vector_storage.num_vectors(), num_vectors);
    assert_eq!(
        gpu_vector_storage.element_type,
        if let Some(_quantization_config) = quantization_config.as_ref() {
            VectorStorageDatatype::Uint8
        } else {
            match storage_type.element_type() {
                TestElementType::Float32 => {
                    if force_half_precision && device.has_half_precision() {
                        VectorStorageDatatype::Float16
                    } else {
                        VectorStorageDatatype::Float32
                    }
                }
                TestElementType::Float16 => {
                    if device.has_half_precision() {
                        VectorStorageDatatype::Float16
                    } else {
                        VectorStorageDatatype::Float32
                    }
                }
                TestElementType::Uint8 => VectorStorageDatatype::Uint8,
            }
        }
    );

    let gpu_scores = run_gpu_scoring(device.clone(), &gpu_vector_storage, num_vectors);

    let query = QueryVector::Nearest(storage.get_vector::<Random>(test_point_id).to_owned());

    let hardware_counter = HardwareCounterCell::new();
    let scorer: Box<dyn RawScorer> = if let Some(quantized_vectors) = quantized_vectors.as_ref() {
        quantized_vectors
            .raw_scorer(query, hardware_counter)
            .unwrap()
    } else {
        new_raw_scorer_for_test(query, &storage).unwrap()
    };

    for (point_id, gpu_score) in gpu_scores.iter().enumerate() {
        let score = scorer.score_internal(
            test_point_id as PointOffsetType,
            point_id as PointOffsetType,
        );
        assert!((score - gpu_score).abs() < precision);
    }
}

/// Dispatch the `test_vector_storage.comp` shader, scoring every vector against
/// target `0`, and download the resulting per-vector scores.
fn run_gpu_scoring(
    device: Arc<gpu::Device>,
    gpu_vector_storage: &GpuVectorStorage,
    num_vectors: usize,
) -> Vec<f32> {
    let scores_buffer = gpu::Buffer::new(
        device.clone(),
        "Scores buffer",
        gpu::BufferType::Storage,
        num_vectors * std::mem::size_of::<f32>(),
    )
    .unwrap();

    let descriptor_set_layout = gpu::DescriptorSetLayout::builder()
        .add_storage_buffer(0)
        .build(device.clone())
        .unwrap();

    let descriptor_set = gpu::DescriptorSet::builder(descriptor_set_layout.clone())
        .add_storage_buffer(0, scores_buffer.clone())
        .build()
        .unwrap();

    let shader = ShaderBuilder::new(device.clone())
        .with_shader_code(include_str!("../shaders/tests/test_vector_storage.comp"))
        .with_parameters(gpu_vector_storage)
        .build("tests/test_vector_storage.comp")
        .unwrap();

    let pipeline = gpu::Pipeline::builder()
        .add_descriptor_set_layout(0, descriptor_set_layout.clone())
        .add_descriptor_set_layout(1, gpu_vector_storage.descriptor_set_layout.clone())
        .add_shader(shader.clone())
        .build(device.clone())
        .unwrap();

    let mut context = gpu::Context::new(device.clone()).unwrap();
    context
        .bind_pipeline(
            pipeline,
            &[descriptor_set, gpu_vector_storage.descriptor_set.clone()],
        )
        .unwrap();
    context.dispatch(num_vectors, 1, 1).unwrap();

    let timer = std::time::Instant::now();
    context.run().unwrap();
    context.wait_finish(GPU_TIMEOUT).unwrap();
    log::trace!("GPU scoring time = {:?}", timer.elapsed());

    let staging_buffer = gpu::Buffer::new(
        device.clone(),
        "Scores staging buffer",
        gpu::BufferType::GpuToCpu,
        num_vectors * std::mem::size_of::<f32>(),
    )
    .unwrap();
    context
        .copy_gpu_buffer(
            scores_buffer,
            staging_buffer.clone(),
            0,
            0,
            num_vectors * std::mem::size_of::<f32>(),
        )
        .unwrap();
    context.run().unwrap();
    context.wait_finish(GPU_TIMEOUT).unwrap();

    staging_buffer.download_vec(0, num_vectors).unwrap()
}
