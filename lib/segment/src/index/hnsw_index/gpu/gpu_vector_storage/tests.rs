//! Unit tests for GPU vector storage, covering f32, f16, u8 dense/multivectors with SQ, BQ, PQ.

use std::path::Path;

use bitvec::vec::BitVec;
use parking_lot::RwLock;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rocksdb::DB;
use rstest::rstest;

use super::*;
use crate::common::rocksdb_wrapper::{open_db, DB_VECTOR_CF};
use crate::data_types::vectors::{MultiDenseVectorInternal, QueryVector, VectorRef};
use crate::fixtures::index_fixtures::random_vector;
use crate::fixtures::payload_fixtures::random_dense_byte_vector;
use crate::index::hnsw_index::gpu::shader_builder::ShaderBuilder;
use crate::spaces::metric::Metric;
use crate::spaces::simple::{CosineMetric, DotProductMetric, EuclidMetric, ManhattanMetric};
use crate::types::{
    BinaryQuantization, BinaryQuantizationConfig, Distance, ProductQuantization,
    ProductQuantizationConfig, QuantizationConfig, ScalarQuantization, ScalarQuantizationConfig,
};
use crate::vector_storage::dense::simple_dense_vector_storage::{
    open_simple_dense_byte_vector_storage, open_simple_dense_half_vector_storage,
    open_simple_dense_vector_storage,
};
use crate::vector_storage::multi_dense::simple_multi_dense_vector_storage::{
    open_simple_multi_dense_vector_storage, open_simple_multi_dense_vector_storage_byte,
    open_simple_multi_dense_vector_storage_half,
};
use crate::vector_storage::{new_raw_scorer, RawScorer};

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
fn test_gpu_vector_storage_sq(
    #[values(Distance::Cosine, Distance::Dot, Distance::Euclid, Distance::Manhattan)]
    distance: Distance,
    #[values(
        TestStorageType::Dense(TestElementType::Float32),
        TestStorageType::Dense(TestElementType::Float16),
        TestStorageType::Dense(TestElementType::Uint8),
        TestStorageType::Multi(TestElementType::Float32),
        TestStorageType::Multi(TestElementType::Float16),
        TestStorageType::Multi(TestElementType::Uint8)
    )]
    storage_type: TestStorageType,
    #[values(
        15,
        512,
        256 + 17,
    )]
    dim: usize,
    #[values(2048 + 17)] num_vectors: usize,
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
        "Testing SQ distance {:?}, element type {:?}, dim {} with precision {}",
        distance,
        storage_type,
        dim,
        precision
    );
    test_gpu_vector_storage_impl(
        storage_type,
        num_vectors,
        dim,
        distance,
        Some(quantization_config.clone()),
        false,
        precision,
    );
}

#[rstest]
fn test_gpu_vector_storage_bq(
    #[values(Distance::Cosine, Distance::Dot, Distance::Euclid, Distance::Manhattan)]
    distance: Distance,
    #[values(
        TestStorageType::Dense(TestElementType::Float32),
        TestStorageType::Dense(TestElementType::Float16),
        TestStorageType::Dense(TestElementType::Uint8),
        TestStorageType::Multi(TestElementType::Float32),
        TestStorageType::Multi(TestElementType::Float16),
        TestStorageType::Multi(TestElementType::Uint8)
    )]
    storage_type: TestStorageType,
    #[values(15, 1536, 256 + 17)] dim: usize,
    #[values(2048 + 17)] num_vectors: usize,
) {
    let _ = env_logger::builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Trace)
        .try_init();

    let quantization_config = QuantizationConfig::Binary(BinaryQuantization {
        binary: BinaryQuantizationConfig {
            always_ram: Some(true),
        },
    });

    let precision = get_precision(storage_type, dim, distance);
    log::info!(
        "Testing BQ distance {:?}, element type {:?}, dim {} with precision {}",
        distance,
        storage_type,
        dim,
        precision
    );
    test_gpu_vector_storage_impl(
        storage_type,
        num_vectors,
        dim,
        distance,
        Some(quantization_config.clone()),
        false,
        precision,
    );
}

#[rstest]
fn test_gpu_vector_storage_pq(
    #[values(Distance::Cosine, Distance::Dot, Distance::Euclid, Distance::Manhattan)]
    distance: Distance,
    #[values(
        TestStorageType::Dense(TestElementType::Float32),
        TestStorageType::Dense(TestElementType::Float16),
        TestStorageType::Multi(TestElementType::Float32),
        TestStorageType::Multi(TestElementType::Float16)
    )]
    storage_type: TestStorageType,
    #[values(15, 256 + 17)] dim: usize,
    #[values(512 + 17)] num_vectors: usize,
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
        "Testing PQ distance {:?}, element type {:?}, dim {} with precision {}",
        distance,
        storage_type,
        dim,
        precision
    );
    test_gpu_vector_storage_impl(
        storage_type,
        num_vectors,
        dim,
        distance,
        Some(quantization_config.clone()),
        false,
        precision,
    );
}

#[rstest]
fn test_gpu_vector_storage(
    #[values(Distance::Cosine, Distance::Dot, Distance::Euclid, Distance::Manhattan)]
    distance: Distance,
    #[values(
        TestStorageType::Dense(TestElementType::Float32),
        TestStorageType::Dense(TestElementType::Float16),
        TestStorageType::Dense(TestElementType::Uint8),
        TestStorageType::Multi(TestElementType::Float32),
        TestStorageType::Multi(TestElementType::Float16),
        TestStorageType::Multi(TestElementType::Uint8)
    )]
    storage_type: TestStorageType,
    #[values(
        15,
        512,
        256 + 17,
    )]
    dim: usize,
    #[values(2048 + 17)] num_vectors: usize,
) {
    let _ = env_logger::builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Trace)
        .try_init();

    let precision = get_precision(storage_type, dim, distance);
    log::info!(
        "Testing distance {:?}, element type {:?}, dim {} with precision {}",
        distance,
        storage_type,
        dim,
        precision
    );
    test_gpu_vector_storage_impl(
        storage_type,
        num_vectors,
        dim,
        distance,
        None,
        false,
        precision,
    );
}

#[rstest]
fn test_gpu_vector_storage_force_half(
    #[values(Distance::Cosine, Distance::Dot, Distance::Euclid, Distance::Manhattan)]
    distance: Distance,
    #[values(
        TestStorageType::Dense(TestElementType::Float32),
        TestStorageType::Multi(TestElementType::Float32)
    )]
    storage_type: TestStorageType,
    #[values(15, 256 + 17)] dim: usize,
    #[values(2048 + 17)] num_vectors: usize,
) {
    let _ = env_logger::builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Trace)
        .try_init();

    let precision = 5.0 * get_precision(storage_type, dim, distance);
    log::info!(
        "Testing distance {:?}, element type {:?}, dim {} with precision {}",
        distance,
        storage_type,
        dim,
        precision
    );
    test_gpu_vector_storage_impl(
        storage_type,
        num_vectors,
        dim,
        distance,
        None,
        true, // force half precision
        precision,
    );
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
    path: &Path,
    storage_type: TestStorageType,
    num_vectors: usize,
    dim: usize,
    distance: Distance,
) -> VectorStorageEnum {
    let db = open_db(path, &[DB_VECTOR_CF]).unwrap();
    match storage_type {
        TestStorageType::Dense(TestElementType::Float32) => {
            create_vector_storage_f32(db, num_vectors, dim, distance)
        }
        TestStorageType::Dense(TestElementType::Float16) => {
            create_vector_storage_f16(db, num_vectors, dim, distance)
        }
        TestStorageType::Dense(TestElementType::Uint8) => {
            create_vector_storage_u8(db, num_vectors, dim, distance)
        }
        TestStorageType::Multi(TestElementType::Float32) => {
            create_vector_storage_f32_multi(db, num_vectors, dim, distance)
        }
        TestStorageType::Multi(TestElementType::Float16) => {
            create_vector_storage_f16_multi(db, num_vectors, dim, distance)
        }
        TestStorageType::Multi(TestElementType::Uint8) => {
            create_vector_storage_u8_multi(db, num_vectors, dim, distance)
        }
    }
}

fn create_vector_storage_f32(
    db: Arc<RwLock<DB>>,
    num_vectors: usize,
    dim: usize,
    distance: Distance,
) -> VectorStorageEnum {
    let mut rnd = StdRng::seed_from_u64(42);
    let mut vector_storage =
        open_simple_dense_vector_storage(db, DB_VECTOR_CF, dim, distance, &false.into()).unwrap();
    for i in 0..num_vectors {
        let vec = random_vector(&mut rnd, dim);
        let vec = match distance {
            Distance::Cosine => <CosineMetric as Metric<VectorElementType>>::preprocess(vec),
            Distance::Euclid => <EuclidMetric as Metric<VectorElementType>>::preprocess(vec),
            Distance::Dot => <DotProductMetric as Metric<VectorElementType>>::preprocess(vec),
            Distance::Manhattan => <ManhattanMetric as Metric<VectorElementType>>::preprocess(vec),
        };
        let vec_ref = VectorRef::from(&vec);
        vector_storage
            .insert_vector(i as PointOffsetType, vec_ref)
            .unwrap();
    }
    vector_storage
}

fn create_vector_storage_f16(
    db: Arc<RwLock<DB>>,
    num_vectors: usize,
    dim: usize,
    distance: Distance,
) -> VectorStorageEnum {
    let mut rnd = StdRng::seed_from_u64(42);
    let mut vector_storage =
        open_simple_dense_half_vector_storage(db, DB_VECTOR_CF, dim, distance, &false.into())
            .unwrap();
    for i in 0..num_vectors {
        let vec = random_vector(&mut rnd, dim);
        let vec = match distance {
            Distance::Cosine => <CosineMetric as Metric<VectorElementTypeHalf>>::preprocess(vec),
            Distance::Euclid => <EuclidMetric as Metric<VectorElementTypeHalf>>::preprocess(vec),
            Distance::Dot => <DotProductMetric as Metric<VectorElementTypeHalf>>::preprocess(vec),
            Distance::Manhattan => {
                <ManhattanMetric as Metric<VectorElementTypeHalf>>::preprocess(vec)
            }
        };
        let vec_ref = VectorRef::from(&vec);
        vector_storage
            .insert_vector(i as PointOffsetType, vec_ref)
            .unwrap();
    }
    vector_storage
}

fn create_vector_storage_u8(
    db: Arc<RwLock<DB>>,
    num_vectors: usize,
    dim: usize,
    distance: Distance,
) -> VectorStorageEnum {
    let mut rnd = StdRng::seed_from_u64(42);
    let mut vector_storage =
        open_simple_dense_byte_vector_storage(db, DB_VECTOR_CF, dim, distance, &false.into())
            .unwrap();
    for i in 0..num_vectors {
        let vec = random_dense_byte_vector(&mut rnd, dim);
        let vec = match distance {
            Distance::Cosine => <CosineMetric as Metric<VectorElementTypeByte>>::preprocess(vec),
            Distance::Euclid => <EuclidMetric as Metric<VectorElementTypeByte>>::preprocess(vec),
            Distance::Dot => <DotProductMetric as Metric<VectorElementTypeByte>>::preprocess(vec),
            Distance::Manhattan => {
                <ManhattanMetric as Metric<VectorElementTypeByte>>::preprocess(vec)
            }
        };
        let vec_ref = VectorRef::from(&vec);
        vector_storage
            .insert_vector(i as PointOffsetType, vec_ref)
            .unwrap();
    }
    vector_storage
}

fn create_vector_storage_f32_multi(
    db: Arc<RwLock<DB>>,
    num_vectors: usize,
    dim: usize,
    distance: Distance,
) -> VectorStorageEnum {
    let mut rnd = StdRng::seed_from_u64(42);
    let multivector_config = Default::default();
    let mut vector_storage = open_simple_multi_dense_vector_storage(
        db,
        DB_VECTOR_CF,
        dim,
        distance,
        multivector_config,
        &false.into(),
    )
    .unwrap();
    for i in 0..num_vectors {
        let mut vectors = vec![];
        let num_vectors_per_points = 1 + rnd.gen::<usize>() % 3;
        for _ in 0..num_vectors_per_points {
            let vec = random_vector(&mut rnd, dim);
            let vec = match distance {
                Distance::Cosine => <CosineMetric as Metric<VectorElementType>>::preprocess(vec),
                Distance::Euclid => <EuclidMetric as Metric<VectorElementType>>::preprocess(vec),
                Distance::Dot => <DotProductMetric as Metric<VectorElementType>>::preprocess(vec),
                Distance::Manhattan => {
                    <ManhattanMetric as Metric<VectorElementType>>::preprocess(vec)
                }
            };
            vectors.extend(vec);
        }
        let multivector = MultiDenseVectorInternal::new(vectors, dim);
        let vec_ref = VectorRef::from(&multivector);
        vector_storage
            .insert_vector(i as PointOffsetType, vec_ref)
            .unwrap();
    }
    vector_storage
}

fn create_vector_storage_f16_multi(
    db: Arc<RwLock<DB>>,
    num_vectors: usize,
    dim: usize,
    distance: Distance,
) -> VectorStorageEnum {
    let mut rnd = StdRng::seed_from_u64(42);
    let multivector_config = Default::default();
    let mut vector_storage = open_simple_multi_dense_vector_storage_half(
        db,
        DB_VECTOR_CF,
        dim,
        distance,
        multivector_config,
        &false.into(),
    )
    .unwrap();
    for i in 0..num_vectors {
        let mut vectors = vec![];
        let num_vectors_per_points = 1 + rnd.gen::<usize>() % 3;
        for _ in 0..num_vectors_per_points {
            let vec = random_vector(&mut rnd, dim);
            let vec = match distance {
                Distance::Cosine => {
                    <CosineMetric as Metric<VectorElementTypeHalf>>::preprocess(vec)
                }
                Distance::Euclid => {
                    <EuclidMetric as Metric<VectorElementTypeHalf>>::preprocess(vec)
                }
                Distance::Dot => {
                    <DotProductMetric as Metric<VectorElementTypeHalf>>::preprocess(vec)
                }
                Distance::Manhattan => {
                    <ManhattanMetric as Metric<VectorElementTypeHalf>>::preprocess(vec)
                }
            };
            vectors.extend(vec);
        }
        let multivector = MultiDenseVectorInternal::new(vectors, dim);
        let vec_ref = VectorRef::from(&multivector);
        vector_storage
            .insert_vector(i as PointOffsetType, vec_ref)
            .unwrap();
    }
    vector_storage
}

fn create_vector_storage_u8_multi(
    db: Arc<RwLock<DB>>,
    num_vectors: usize,
    dim: usize,
    distance: Distance,
) -> VectorStorageEnum {
    let mut rnd = StdRng::seed_from_u64(42);
    let multivector_config = Default::default();
    let mut vector_storage = open_simple_multi_dense_vector_storage_byte(
        db,
        DB_VECTOR_CF,
        dim,
        distance,
        multivector_config,
        &false.into(),
    )
    .unwrap();
    for i in 0..num_vectors {
        let mut vectors = vec![];
        let num_vectors_per_points = 1 + rnd.gen::<usize>() % 3;
        for _ in 0..num_vectors_per_points {
            let vec = random_dense_byte_vector(&mut rnd, dim);
            let vec = match distance {
                Distance::Cosine => {
                    <CosineMetric as Metric<VectorElementTypeByte>>::preprocess(vec)
                }
                Distance::Euclid => {
                    <EuclidMetric as Metric<VectorElementTypeByte>>::preprocess(vec)
                }
                Distance::Dot => {
                    <DotProductMetric as Metric<VectorElementTypeByte>>::preprocess(vec)
                }
                Distance::Manhattan => {
                    <ManhattanMetric as Metric<VectorElementTypeByte>>::preprocess(vec)
                }
            };
            vectors.extend(vec);
        }
        let multivector = MultiDenseVectorInternal::new(vectors, dim);
        let vec_ref = VectorRef::from(&multivector);
        vector_storage
            .insert_vector(i as PointOffsetType, vec_ref)
            .unwrap();
    }
    vector_storage
}

fn test_gpu_vector_storage_impl(
    storage_type: TestStorageType,
    num_vectors: usize,
    dim: usize,
    distance: Distance,
    quantization_config: Option<QuantizationConfig>,
    force_half_precision: bool,
    precision: f32,
) {
    let test_point_id: PointOffsetType = 0;

    let dir = tempfile::Builder::new().prefix("db_dir").tempdir().unwrap();
    let storage = create_vector_storage(dir.path(), storage_type, num_vectors, dim, distance);

    let quantized_vectors = quantization_config.as_ref().map(|quantization_config| {
        QuantizedVectors::create(&storage, quantization_config, dir.path(), 1, &false.into())
            .unwrap()
    });

    let debug_messenger = gpu::PanicIfErrorMessenger {};
    let instance = gpu::Instance::new(Some(&debug_messenger), None, false).unwrap();
    let device = gpu::Device::new(instance.clone(), &instance.physical_devices()[0]).unwrap();

    let gpu_vector_storage = GpuVectorStorage::new(
        device.clone(),
        &storage,
        quantized_vectors.as_ref(),
        force_half_precision,
        &false.into(),
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
                    if force_half_precision {
                        VectorStorageDatatype::Float16
                    } else {
                        VectorStorageDatatype::Float32
                    }
                }
                TestElementType::Float16 => VectorStorageDatatype::Float16,
                TestElementType::Uint8 => VectorStorageDatatype::Uint8,
            }
        }
    );

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
        .with_parameters(&gpu_vector_storage)
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

    let mut gpu_scores = vec![0.0f32; num_vectors];
    staging_buffer.download_slice(&mut gpu_scores, 0).unwrap();

    let stopped = false.into();
    let point_deleted = BitVec::repeat(false, num_vectors);
    let query = QueryVector::Nearest(storage.get_vector(test_point_id).to_owned());
    let scorer: Box<dyn RawScorer> = if let Some(quantized_vectors) = quantized_vectors.as_ref() {
        quantized_vectors
            .raw_scorer(query, &point_deleted, &point_deleted, &stopped)
            .unwrap()
    } else {
        new_raw_scorer(query, &storage, &point_deleted).unwrap()
    };

    for (point_id, gpu_score) in gpu_scores.iter().enumerate() {
        let score = scorer.score_internal(
            test_point_id as PointOffsetType,
            point_id as PointOffsetType,
        );
        assert!((score - gpu_score).abs() < precision);
    }

    scorer.take_hardware_counter().discard_results();
}
