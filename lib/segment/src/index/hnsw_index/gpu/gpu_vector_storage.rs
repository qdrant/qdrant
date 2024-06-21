use std::borrow::Cow;
use std::sync::Arc;

use common::types::PointOffsetType;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{VectorElementType, VectorElementTypeByte, VectorElementTypeHalf};
use crate::vector_storage::quantized::quantized_vectors::{
    QuantizedVectorStorage, QuantizedVectors,
};
use crate::vector_storage::{DenseVectorStorage, VectorStorage, VectorStorageEnum};

pub const ALIGNMENT: usize = 32 * 4;
pub const UPLOAD_CHUNK_SIZE: usize = 64 * 1024 * 1024;
pub const STORAGES_COUNT: usize = 4;

#[repr(C)]
struct GpuVectorParamsBuffer {
    dim: u32,
    count: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GpuVectorStorageElementType {
    Float32,
    Float16,
    Uint8,
    Binary,
}

pub struct GpuVectorStorage {
    pub device: Arc<gpu::Device>,
    pub vectors_buffer: Vec<Arc<gpu::Buffer>>,
    pub params_buffer: Arc<gpu::Buffer>,
    pub descriptor_set_layout: Arc<gpu::DescriptorSetLayout>,
    pub descriptor_set: Arc<gpu::DescriptorSet>,
    pub dim: usize,
    pub count: usize,
    pub element_type: GpuVectorStorageElementType,
}

impl GpuVectorStorage {
    pub fn new(
        device: Arc<gpu::Device>,
        vector_storage: &VectorStorageEnum,
        quantized_storage: Option<&QuantizedVectors>,
        force_half_precision: bool,
    ) -> OperationResult<Self> {
        if let Some(quantized_storage) = quantized_storage {
            Self::new_from_vector_quantization(
                device,
                vector_storage,
                &quantized_storage.storage_impl,
                force_half_precision,
            )
        } else {
            Self::new_from_vector_storage(device, vector_storage, force_half_precision)
        }
    }

    fn new_from_vector_quantization(
        device: Arc<gpu::Device>,
        vector_storage: &VectorStorageEnum,
        quantized_storage: &QuantizedVectorStorage,
        force_half_precision: bool,
    ) -> OperationResult<Self> {
        match quantized_storage {
            QuantizedVectorStorage::ScalarRam(_) => {
                log::warn!("GPU does not support scalar quantization, use original vector data");
                Self::new_from_vector_storage(device, vector_storage, force_half_precision)
            }
            QuantizedVectorStorage::ScalarMmap(_) => {
                log::warn!("GPU does not support scalar quantization, use original vector data");
                Self::new_from_vector_storage(device, vector_storage, force_half_precision)
            }
            QuantizedVectorStorage::PQRam(_) => {
                log::warn!("GPU does not support product quantization, use original vector data");
                Self::new_from_vector_storage(device, vector_storage, force_half_precision)
            }
            QuantizedVectorStorage::PQMmap(_) => {
                log::warn!("GPU does not support product quantization, use original vector data");
                Self::new_from_vector_storage(device, vector_storage, force_half_precision)
            }
            QuantizedVectorStorage::BinaryRam(_) => {
                let first_vector = vector_storage.get_vector(0);
                let first_dense: &[VectorElementType] = first_vector.as_vec_ref().try_into()?;
                let bits_per_read = device.subgroup_size() * std::mem::size_of::<u32>() * 8;
                let bits_count = first_dense.len().div_ceil(bits_per_read) * bits_per_read;
                let u32_count = bits_count / 32;
                Self::new_typed::<VectorElementTypeByte>(
                    device,
                    GpuVectorStorageElementType::Binary,
                    vector_storage.total_vector_count(),
                    |id| {
                        let vector = vector_storage.get_vector(id);
                        let dense: &[VectorElementType] = vector.as_vec_ref().try_into().unwrap();
                        let mut binary = vec![0u8; u32_count * std::mem::size_of::<u32>()];
                        for (i, v) in dense.iter().enumerate() {
                            if *v > 0.0 {
                                binary[i / 8] |= 1u8 << (i % 8);
                            }
                        }
                        Cow::Owned(binary)
                    },
                )
            }
            QuantizedVectorStorage::BinaryMmap(_) => {
                let first_vector = vector_storage.get_vector(0);
                let first_dense: &[VectorElementType] = first_vector.as_vec_ref().try_into()?;
                let bits_per_read = device.subgroup_size() * std::mem::size_of::<u32>() * 8;
                let bits_count = first_dense.len().div_ceil(bits_per_read) * bits_per_read;
                let u32_count = bits_count / 32;
                Self::new_typed::<VectorElementTypeByte>(
                    device,
                    GpuVectorStorageElementType::Binary,
                    vector_storage.total_vector_count(),
                    |id| {
                        let vector = vector_storage.get_vector(id);
                        let dense: &[VectorElementType] = vector.as_vec_ref().try_into().unwrap();
                        let mut binary = vec![0u8; u32_count * std::mem::size_of::<u32>()];
                        for (i, v) in dense.iter().enumerate() {
                            let bit = if *v > 0.0 { 1 } else { 0 };
                            binary[i / 8] |= bit << (i % 8);
                        }
                        Cow::Owned(binary)
                    },
                )
            }
            QuantizedVectorStorage::ScalarRamMulti(_) => Err(OperationError::service_error(
                "GPU does not support multivectors",
            )),
            QuantizedVectorStorage::ScalarMmapMulti(_) => Err(OperationError::service_error(
                "GPU does not support multivectors",
            )),
            QuantizedVectorStorage::PQRamMulti(_) => Err(OperationError::service_error(
                "GPU does not support multivectors",
            )),
            QuantizedVectorStorage::PQMmapMulti(_) => Err(OperationError::service_error(
                "GPU does not support multivectors",
            )),
            QuantizedVectorStorage::BinaryRamMulti(_) => Err(OperationError::service_error(
                "GPU does not support multivectors",
            )),
            QuantizedVectorStorage::BinaryMmapMulti(_) => Err(OperationError::service_error(
                "GPU does not support multivectors",
            )),
        }
    }

    fn new_from_vector_storage(
        device: Arc<gpu::Device>,
        vector_storage: &VectorStorageEnum,
        force_half_precision: bool,
    ) -> OperationResult<Self> {
        match vector_storage {
            VectorStorageEnum::DenseSimple(vector_storage) => {
                if force_half_precision {
                    Self::new_typed::<VectorElementTypeHalf>(
                        device,
                        GpuVectorStorageElementType::Float16,
                        vector_storage.total_vector_count(),
                        |id| {
                            VectorElementTypeHalf::slice_from_float_cow(Cow::Borrowed(
                                vector_storage.get_dense(id),
                            ))
                        },
                    )
                } else {
                    Self::new_typed::<VectorElementType>(
                        device,
                        GpuVectorStorageElementType::Float32,
                        vector_storage.total_vector_count(),
                        |id| Cow::Borrowed(vector_storage.get_dense(id)),
                    )
                }
            }
            VectorStorageEnum::DenseSimpleByte(vector_storage) => {
                Self::new_typed::<VectorElementTypeByte>(
                    device,
                    GpuVectorStorageElementType::Uint8,
                    vector_storage.total_vector_count(),
                    |id| Cow::Borrowed(vector_storage.get_dense(id)),
                )
            }
            VectorStorageEnum::DenseSimpleHalf(vector_storage) => {
                Self::new_typed::<VectorElementTypeHalf>(
                    device,
                    GpuVectorStorageElementType::Float16,
                    vector_storage.total_vector_count(),
                    |id| Cow::Borrowed(vector_storage.get_dense(id)),
                )
            }
            VectorStorageEnum::DenseMemmap(vector_storage) => {
                if force_half_precision {
                    Self::new_typed::<VectorElementTypeHalf>(
                        device,
                        GpuVectorStorageElementType::Float16,
                        vector_storage.total_vector_count(),
                        |id| {
                            VectorElementTypeHalf::slice_from_float_cow(Cow::Borrowed(
                                vector_storage.get_dense(id),
                            ))
                        },
                    )
                } else {
                    Self::new_typed::<VectorElementType>(
                        device,
                        GpuVectorStorageElementType::Float32,
                        vector_storage.total_vector_count(),
                        |id| Cow::Borrowed(vector_storage.get_dense(id)),
                    )
                }
            }
            VectorStorageEnum::DenseMemmapByte(vector_storage) => {
                Self::new_typed::<VectorElementTypeByte>(
                    device,
                    GpuVectorStorageElementType::Uint8,
                    vector_storage.total_vector_count(),
                    |id| Cow::Borrowed(vector_storage.get_dense(id)),
                )
            }
            VectorStorageEnum::DenseMemmapHalf(vector_storage) => {
                Self::new_typed::<VectorElementTypeHalf>(
                    device,
                    GpuVectorStorageElementType::Float16,
                    vector_storage.total_vector_count(),
                    |id| Cow::Borrowed(vector_storage.get_dense(id)),
                )
            }
            VectorStorageEnum::DenseAppendableMemmap(vector_storage) => {
                if force_half_precision {
                    Self::new_typed::<VectorElementTypeHalf>(
                        device,
                        GpuVectorStorageElementType::Float16,
                        vector_storage.total_vector_count(),
                        |id| {
                            VectorElementTypeHalf::slice_from_float_cow(Cow::Borrowed(
                                vector_storage.get_dense(id),
                            ))
                        },
                    )
                } else {
                    Self::new_typed::<VectorElementType>(
                        device,
                        GpuVectorStorageElementType::Float32,
                        vector_storage.total_vector_count(),
                        |id| Cow::Borrowed(vector_storage.get_dense(id)),
                    )
                }
            }
            VectorStorageEnum::DenseAppendableMemmapByte(vector_storage) => {
                Self::new_typed::<VectorElementTypeByte>(
                    device,
                    GpuVectorStorageElementType::Uint8,
                    vector_storage.total_vector_count(),
                    |id| Cow::Borrowed(vector_storage.get_dense(id)),
                )
            }
            VectorStorageEnum::DenseAppendableMemmapHalf(vector_storage) => {
                Self::new_typed::<VectorElementTypeHalf>(
                    device,
                    GpuVectorStorageElementType::Float16,
                    vector_storage.total_vector_count(),
                    |id| Cow::Borrowed(vector_storage.get_dense(id)),
                )
            }
            VectorStorageEnum::SparseSimple(_) => Err(OperationError::service_error(
                "GPU does not support sparse vectors",
            )),
            VectorStorageEnum::MultiDenseSimple(_) => Err(OperationError::service_error(
                "GPU does not support multivectors",
            )),
            VectorStorageEnum::MultiDenseSimpleByte(_) => Err(OperationError::service_error(
                "GPU does not support multivectors",
            )),
            VectorStorageEnum::MultiDenseSimpleHalf(_) => Err(OperationError::service_error(
                "GPU does not support multivectors",
            )),
            VectorStorageEnum::MultiDenseAppendableMemmap(_) => Err(OperationError::service_error(
                "GPU does not support multivectors",
            )),
            VectorStorageEnum::MultiDenseAppendableMemmapByte(_) => Err(
                OperationError::service_error("GPU does not support multivectors"),
            ),
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(_) => Err(
                OperationError::service_error("GPU does not support multivectors"),
            ),
        }
    }

    fn new_typed<'a, TElement: PrimitiveVectorElement>(
        device: Arc<gpu::Device>,
        element_type: GpuVectorStorageElementType,
        count: usize,
        get_vector: impl Fn(PointOffsetType) -> Cow<'a, [TElement]>,
    ) -> OperationResult<Self> {
        let timer = std::time::Instant::now();

        let dim = get_vector(0).len();

        let capacity = Self::get_capacity(dim);
        let upload_points_count = UPLOAD_CHUNK_SIZE / (capacity * std::mem::size_of::<TElement>());

        let points_in_storage_count = Self::get_points_in_storage_count(count);
        let vectors_buffer = (0..STORAGES_COUNT)
            .map(|_| {
                Arc::new(gpu::Buffer::new(
                    device.clone(),
                    gpu::BufferType::Storage,
                    points_in_storage_count * capacity * std::mem::size_of::<TElement>(),
                ))
            })
            .collect::<Vec<_>>();
        println!("Storage buffer size {}", vectors_buffer[0].size);
        let params_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::Uniform,
            std::mem::size_of::<GpuVectorParamsBuffer>(),
        ));

        let mut upload_context = gpu::Context::new(device.clone());
        let staging_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::CpuToGpu,
            upload_points_count * capacity * std::mem::size_of::<TElement>(),
        ));
        println!(
            "Staging buffer size {}, upload_points_count = {}",
            staging_buffer.size, upload_points_count
        );

        println!("capacity = {}, count = {}", capacity, count);
        let params = GpuVectorParamsBuffer {
            dim: capacity as u32,
            count: count as u32,
        };
        staging_buffer.upload(&params, 0);
        upload_context.copy_gpu_buffer(
            staging_buffer.clone(),
            params_buffer.clone(),
            0,
            0,
            std::mem::size_of::<GpuVectorParamsBuffer>(),
        );
        upload_context.run();
        upload_context.wait_finish();

        for (storage_index, vector_buffer) in vectors_buffer.iter().enumerate() {
            let mut gpu_offset = 0;
            let mut upload_size = 0;
            let mut upload_points = 0;
            let mut extended_vector = vec![TElement::default(); capacity];
            for point_id in 0..count {
                if point_id % STORAGES_COUNT != storage_index {
                    continue;
                }

                let vector = get_vector(point_id as PointOffsetType);
                extended_vector[..vector.len()].copy_from_slice(&vector);
                staging_buffer.upload_slice(
                    &extended_vector,
                    upload_points * capacity * std::mem::size_of::<TElement>(),
                );
                upload_size += capacity * std::mem::size_of::<TElement>();
                upload_points += 1;

                if upload_points == upload_points_count {
                    upload_context.copy_gpu_buffer(
                        staging_buffer.clone(),
                        vector_buffer.clone(),
                        0,
                        gpu_offset,
                        upload_size,
                    );
                    upload_context.run();
                    upload_context.wait_finish();

                    println!(
                        "Uploaded {} vectors, {} MB",
                        upload_points,
                        upload_size / 1024 / 1024,
                    );

                    gpu_offset += upload_size;
                    upload_size = 0;
                    upload_points = 0;
                }
            }
            if upload_points > 0 {
                upload_context.copy_gpu_buffer(
                    staging_buffer.clone(),
                    vectors_buffer[storage_index].clone(),
                    0,
                    gpu_offset,
                    upload_size,
                );
                upload_context.run();
                upload_context.wait_finish();

                println!(
                    "Uploaded {} vectors, {} MB",
                    upload_points,
                    upload_size / 1024 / 1024,
                );
            }
        }

        println!(
            "Upload vector data to GPU time = {:?}, vector data size {} MB, element type: {:?}",
            timer.elapsed(),
            STORAGES_COUNT * points_in_storage_count * capacity * std::mem::size_of::<TElement>()
                / 1024
                / 1024,
            element_type,
        );

        let mut descriptor_set_layout_builder =
            gpu::DescriptorSetLayout::builder().add_uniform_buffer(0);
        for i in 0..STORAGES_COUNT {
            descriptor_set_layout_builder = descriptor_set_layout_builder.add_storage_buffer(i + 1);
        }
        let descriptor_set_layout = descriptor_set_layout_builder.build(device.clone());

        let mut descriptor_set_builder = gpu::DescriptorSet::builder(descriptor_set_layout.clone())
            .add_uniform_buffer(0, params_buffer.clone());
        for (i, vector_buffer) in vectors_buffer.iter().enumerate() {
            descriptor_set_builder =
                descriptor_set_builder.add_storage_buffer(i + 1, vector_buffer.clone());
        }

        let descriptor_set = descriptor_set_builder.build();

        Ok(Self {
            device,
            vectors_buffer,
            params_buffer,
            descriptor_set_layout,
            descriptor_set,
            dim,
            count,
            element_type,
        })
    }

    pub fn get_capacity(dim: usize) -> usize {
        dim + (ALIGNMENT - dim % ALIGNMENT) % ALIGNMENT
    }

    pub fn get_points_in_storage_count(num_vectors: usize) -> usize {
        num_vectors + (STORAGES_COUNT - num_vectors % STORAGES_COUNT) % STORAGES_COUNT
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use bitvec::vec::BitVec;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    use super::*;
    use crate::common::rocksdb_wrapper::{open_db, DB_VECTOR_CF};
    use crate::fixtures::index_fixtures::random_vector;
    use crate::fixtures::payload_fixtures::random_dense_byte_vector;
    use crate::spaces::metric::Metric;
    use crate::spaces::simple::DotProductMetric;
    use crate::types::{
        BinaryQuantization, BinaryQuantizationConfig, Distance, QuantizationConfig,
    };
    use crate::vector_storage::dense::simple_dense_vector_storage::{
        open_simple_dense_byte_vector_storage, open_simple_dense_half_vector_storage,
        open_simple_dense_vector_storage,
    };

    enum TestElementType {
        Float32,
        Float16,
        Uint8,
    }

    fn open_vector_storage(
        path: &Path,
        dim: usize,
        element_type: TestElementType,
    ) -> VectorStorageEnum {
        let db = open_db(path, &[DB_VECTOR_CF]).unwrap();

        match element_type {
            TestElementType::Float32 => open_simple_dense_vector_storage(
                db,
                DB_VECTOR_CF,
                dim,
                Distance::Dot,
                &false.into(),
            )
            .unwrap(),
            TestElementType::Float16 => open_simple_dense_half_vector_storage(
                db,
                DB_VECTOR_CF,
                dim,
                Distance::Dot,
                &false.into(),
            )
            .unwrap(),
            TestElementType::Uint8 => open_simple_dense_byte_vector_storage(
                db,
                DB_VECTOR_CF,
                dim,
                Distance::Dot,
                &false.into(),
            )
            .unwrap(),
        }
    }

    fn test_gpu_vector_storage_scoring_impl(
        element_type: TestElementType,
        force_half_precision: bool,
    ) -> GpuVectorStorageElementType {
        let num_vectors = 2048;
        let dim = 128;
        let capacity = 128;
        let test_point_id = 0usize;

        let mut rnd = StdRng::seed_from_u64(42);
        let points = (0..num_vectors)
            .map(|_| match element_type {
                TestElementType::Float32 => random_vector(&mut rnd, dim),
                TestElementType::Float16 => random_vector(&mut rnd, dim),
                TestElementType::Uint8 => random_dense_byte_vector(&mut rnd, dim),
            })
            .collect::<Vec<_>>();

        let dir = tempfile::Builder::new().prefix("db_dir").tempdir().unwrap();
        let mut storage = open_vector_storage(dir.path(), dim, element_type);
        points.iter().enumerate().for_each(|(i, vec)| {
            storage
                .insert_vector(i as PointOffsetType, vec.into())
                .unwrap();
        });

        let debug_messenger = gpu::PanicIfErrorMessenger {};
        let instance =
            Arc::new(gpu::Instance::new("qdrant", Some(&debug_messenger), false).unwrap());
        let device =
            Arc::new(gpu::Device::new(instance.clone(), instance.vk_physical_devices[0]).unwrap());

        let gpu_vector_storage =
            GpuVectorStorage::new(device.clone(), &storage, None, force_half_precision).unwrap();

        let scores_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::Storage,
            num_vectors * std::mem::size_of::<f32>(),
        ));

        let descriptor_set_layout = gpu::DescriptorSetLayout::builder()
            .add_storage_buffer(0)
            .build(device.clone());

        let descriptor_set = gpu::DescriptorSet::builder(descriptor_set_layout.clone())
            .add_storage_buffer(0, scores_buffer.clone())
            .build();

        let shader = Arc::new(gpu::Shader::new(
            device.clone(),
            match gpu_vector_storage.element_type {
                GpuVectorStorageElementType::Float32 => {
                    println!("Float32 shader");
                    include_bytes!("./shaders/compiled/test_vector_storage_f32.spv")
                }
                GpuVectorStorageElementType::Float16 => {
                    println!("Float16 shader");
                    include_bytes!("./shaders/compiled/test_vector_storage_f16.spv")
                }
                GpuVectorStorageElementType::Uint8 => {
                    println!("Uint8 shader");
                    include_bytes!("./shaders/compiled/test_vector_storage_u8.spv")
                }
                GpuVectorStorageElementType::Binary => {
                    println!("Binary shader");
                    include_bytes!("./shaders/compiled/test_vector_storage_binary.spv")
                }
            },
        ));

        let pipeline = gpu::Pipeline::builder()
            .add_descriptor_set_layout(0, descriptor_set_layout.clone())
            .add_descriptor_set_layout(1, gpu_vector_storage.descriptor_set_layout.clone())
            .add_shader(shader.clone())
            .build(device.clone());

        let mut context = gpu::Context::new(device.clone());
        context.bind_pipeline(
            pipeline,
            &[descriptor_set, gpu_vector_storage.descriptor_set.clone()],
        );
        context.dispatch(num_vectors, 1, 1);

        let timer = std::time::Instant::now();
        context.run();
        context.wait_finish();
        println!("GPU scoring time = {:?}", timer.elapsed());

        let staging_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::GpuToCpu,
            num_vectors * std::mem::size_of::<f32>(),
        ));
        context.copy_gpu_buffer(
            scores_buffer,
            staging_buffer.clone(),
            0,
            0,
            num_vectors * std::mem::size_of::<f32>(),
        );
        context.run();
        context.wait_finish();

        let mut scores = vec![0.0f32; num_vectors];
        staging_buffer.download_slice(&mut scores, 0);

        context.copy_gpu_buffer(
            gpu_vector_storage.params_buffer.clone(),
            staging_buffer.clone(),
            0,
            0,
            std::mem::size_of::<GpuVectorParamsBuffer>(),
        );
        context.run();
        context.wait_finish();

        let mut vector_storage_params = GpuVectorParamsBuffer { dim: 0, count: 0 };
        staging_buffer.download(&mut vector_storage_params, 0);
        assert_eq!(vector_storage_params.dim, capacity as u32);
        assert_eq!(vector_storage_params.count, num_vectors as u32);

        let timer = std::time::Instant::now();
        for i in 0..num_vectors {
            let score = DotProductMetric::similarity(&points[test_point_id], &points[i]);
            assert!((score - scores[i]).abs() < 0.01);
        }
        println!("CPU scoring time = {:?}", timer.elapsed());

        gpu_vector_storage.element_type
    }

    #[test]
    fn test_gpu_vector_storage_scoring() {
        let element = test_gpu_vector_storage_scoring_impl(TestElementType::Float32, false);
        assert_eq!(element, GpuVectorStorageElementType::Float32);
    }

    #[test]
    fn test_gpu_vector_storage_scoring_f16() {
        let element = test_gpu_vector_storage_scoring_impl(TestElementType::Float16, false);
        assert_eq!(element, GpuVectorStorageElementType::Float16);
    }

    #[test]
    fn test_gpu_vector_storage_scoring_u8() {
        let element = test_gpu_vector_storage_scoring_impl(TestElementType::Uint8, false);
        assert_eq!(element, GpuVectorStorageElementType::Uint8);
    }

    #[test]
    fn test_gpu_vector_storage_force_half_precision() {
        let element = test_gpu_vector_storage_scoring_impl(TestElementType::Float32, true);
        assert_eq!(element, GpuVectorStorageElementType::Float16);
    }

    #[test]
    fn test_gpu_vector_storage_binary_quantization() {
        let num_vectors = 16;
        let dim = 1024;
        let capacity = 128;
        let test_point_id = 0usize;

        let mut rnd = StdRng::seed_from_u64(42);
        let points = (0..num_vectors)
            .map(|_| random_vector(&mut rnd, dim))
            .collect::<Vec<_>>();

        let dir = tempfile::Builder::new().prefix("db_dir").tempdir().unwrap();
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
        let mut storage =
            open_simple_dense_vector_storage(db, DB_VECTOR_CF, dim, Distance::Dot, &false.into())
                .unwrap();

        points.iter().enumerate().for_each(|(i, vec)| {
            storage
                .insert_vector(i as PointOffsetType, vec.into())
                .unwrap();
        });

        let quantized_vectors = QuantizedVectors::create(
            &storage,
            &QuantizationConfig::Binary(BinaryQuantization {
                binary: BinaryQuantizationConfig {
                    always_ram: Some(true),
                },
            }),
            dir.path(),
            1,
            &false.into(),
        )
        .unwrap();

        let debug_messenger = gpu::PanicIfErrorMessenger {};
        let instance =
            Arc::new(gpu::Instance::new("qdrant", Some(&debug_messenger), false).unwrap());
        let device =
            Arc::new(gpu::Device::new(instance.clone(), instance.vk_physical_devices[0]).unwrap());

        let gpu_vector_storage =
            GpuVectorStorage::new(device.clone(), &storage, Some(&quantized_vectors), false)
                .unwrap();
        assert_eq!(
            gpu_vector_storage.element_type,
            GpuVectorStorageElementType::Binary
        );

        let scores_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::Storage,
            num_vectors * std::mem::size_of::<f32>(),
        ));

        let descriptor_set_layout = gpu::DescriptorSetLayout::builder()
            .add_storage_buffer(0)
            .build(device.clone());

        let descriptor_set = gpu::DescriptorSet::builder(descriptor_set_layout.clone())
            .add_storage_buffer(0, scores_buffer.clone())
            .build();

        let shader = Arc::new(gpu::Shader::new(
            device.clone(),
            match gpu_vector_storage.element_type {
                GpuVectorStorageElementType::Float32 => {
                    println!("Float32 shader");
                    include_bytes!("./shaders/compiled/test_vector_storage_f32.spv")
                }
                GpuVectorStorageElementType::Float16 => {
                    println!("Float16 shader");
                    include_bytes!("./shaders/compiled/test_vector_storage_f16.spv")
                }
                GpuVectorStorageElementType::Uint8 => {
                    println!("Uint8 shader");
                    include_bytes!("./shaders/compiled/test_vector_storage_u8.spv")
                }
                GpuVectorStorageElementType::Binary => {
                    println!("Binary shader");
                    include_bytes!("./shaders/compiled/test_vector_storage_binary.spv")
                }
            },
        ));

        let pipeline = gpu::Pipeline::builder()
            .add_descriptor_set_layout(0, descriptor_set_layout.clone())
            .add_descriptor_set_layout(1, gpu_vector_storage.descriptor_set_layout.clone())
            .add_shader(shader.clone())
            .build(device.clone());

        let mut context = gpu::Context::new(device.clone());
        context.bind_pipeline(
            pipeline,
            &[descriptor_set, gpu_vector_storage.descriptor_set.clone()],
        );
        context.dispatch(num_vectors, 1, 1);

        let timer = std::time::Instant::now();
        context.run();
        context.wait_finish();
        println!("GPU scoring time = {:?}", timer.elapsed());

        let staging_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::GpuToCpu,
            num_vectors * std::mem::size_of::<f32>(),
        ));
        context.copy_gpu_buffer(
            scores_buffer,
            staging_buffer.clone(),
            0,
            0,
            num_vectors * std::mem::size_of::<f32>(),
        );
        context.run();
        context.wait_finish();

        let mut scores = vec![0.0f32; num_vectors];
        staging_buffer.download_slice(&mut scores, 0);

        context.copy_gpu_buffer(
            gpu_vector_storage.params_buffer.clone(),
            staging_buffer.clone(),
            0,
            0,
            std::mem::size_of::<GpuVectorParamsBuffer>(),
        );
        context.run();
        context.wait_finish();

        let mut vector_storage_params = GpuVectorParamsBuffer { dim: 0, count: 0 };
        staging_buffer.download(&mut vector_storage_params, 0);
        assert_eq!(vector_storage_params.dim, capacity as u32);
        assert_eq!(vector_storage_params.count, num_vectors as u32);

        let stopped = false.into();
        let point_deleted = BitVec::repeat(false, num_vectors);
        let scorer = quantized_vectors
            .raw_scorer(
                points[test_point_id].clone().into(),
                &point_deleted,
                &point_deleted,
                &stopped,
            )
            .unwrap();
        for i in 0..num_vectors {
            let score =
                scorer.score_internal(test_point_id as PointOffsetType, i as PointOffsetType);
            assert_eq!(score, scores[i]);
        }
    }
}
