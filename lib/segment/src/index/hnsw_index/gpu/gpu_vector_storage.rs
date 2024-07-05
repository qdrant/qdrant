use std::borrow::Cow;
use std::sync::Arc;

use common::types::PointOffsetType;

use crate::data_types::primitive::PrimitiveVectorElement;
use crate::data_types::vectors::{VectorElementType, VectorElementTypeByte, VectorElementTypeHalf};
use crate::vector_storage::quantized::quantized_vectors::{
    QuantizedVectorStorage, QuantizedVectors,
};
use crate::vector_storage::{DenseVectorStorage, VectorStorage, VectorStorageEnum};

pub const UPLOAD_CHUNK_SIZE: usize = 128 * 1024 * 1024;

pub struct GpuVectorStorage {
    pub device: Arc<gpu::Device>,
    pub vectors_buffer: Vec<Arc<gpu::Buffer>>,
    pub descriptor_set_layout: Arc<gpu::DescriptorSetLayout>,
    pub descriptor_set: Arc<gpu::DescriptorSet>,
    pub working_group_size: usize,
    pub dim: usize,
    pub count: usize,
    pub storages_count: usize,
    pub storage_size: usize,
    pub element_type: gpu::GpuVectorStorageElementType,
}

impl GpuVectorStorage {
    pub fn new(
        device: Arc<gpu::Device>,
        vector_storage: &VectorStorageEnum,
        quantized_storage: Option<&QuantizedVectors>,
        force_half_precision: bool,
    ) -> gpu::GpuResult<Self> {
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
    ) -> gpu::GpuResult<Self> {
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
            QuantizedVectorStorage::BinaryRam(_) | QuantizedVectorStorage::BinaryMmap(_) => {
                let first_vector = vector_storage.get_vector(0);
                // TODO(gpu): remove unwrap
                let first_dense: &[VectorElementType] =
                    first_vector.as_vec_ref().try_into().unwrap();
                let bits_per_read = device.subgroup_size() * std::mem::size_of::<u32>() * 8;
                let bits_count = first_dense.len().div_ceil(bits_per_read) * bits_per_read;
                let u32_count = bits_count / 32;
                Self::new_typed::<u32>(
                    device,
                    gpu::GpuVectorStorageElementType::Binary,
                    vector_storage.total_vector_count(),
                    |id| {
                        let vector = vector_storage.get_vector(id);
                        let dense: &[VectorElementType] = vector.as_vec_ref().try_into().unwrap();
                        let mut binary = vec![0u32; u32_count];
                        for (i, v) in dense.iter().enumerate() {
                            if *v > 0.0 {
                                binary[i / 32] |= 1u32 << (i % 32);
                            }
                        }
                        Cow::Owned(binary)
                    },
                )
            }
            QuantizedVectorStorage::ScalarRamMulti(_) => Err(gpu::GpuError::NotSupported),
            QuantizedVectorStorage::ScalarMmapMulti(_) => Err(gpu::GpuError::NotSupported),
            QuantizedVectorStorage::PQRamMulti(_) => Err(gpu::GpuError::NotSupported),
            QuantizedVectorStorage::PQMmapMulti(_) => Err(gpu::GpuError::NotSupported),
            QuantizedVectorStorage::BinaryRamMulti(_) => Err(gpu::GpuError::NotSupported),
            QuantizedVectorStorage::BinaryMmapMulti(_) => Err(gpu::GpuError::NotSupported),
        }
    }

    fn new_from_vector_storage(
        device: Arc<gpu::Device>,
        vector_storage: &VectorStorageEnum,
        force_half_precision: bool,
    ) -> gpu::GpuResult<Self> {
        match vector_storage {
            VectorStorageEnum::DenseSimple(vector_storage) => {
                if force_half_precision {
                    Self::new_typed::<VectorElementTypeHalf>(
                        device,
                        gpu::GpuVectorStorageElementType::Float16,
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
                        gpu::GpuVectorStorageElementType::Float32,
                        vector_storage.total_vector_count(),
                        |id| Cow::Borrowed(vector_storage.get_dense(id)),
                    )
                }
            }
            VectorStorageEnum::DenseSimpleByte(vector_storage) => {
                Self::new_typed::<VectorElementTypeByte>(
                    device,
                    gpu::GpuVectorStorageElementType::Uint8,
                    vector_storage.total_vector_count(),
                    |id| Cow::Borrowed(vector_storage.get_dense(id)),
                )
            }
            VectorStorageEnum::DenseSimpleHalf(vector_storage) => {
                Self::new_typed::<VectorElementTypeHalf>(
                    device,
                    gpu::GpuVectorStorageElementType::Float16,
                    vector_storage.total_vector_count(),
                    |id| Cow::Borrowed(vector_storage.get_dense(id)),
                )
            }
            VectorStorageEnum::DenseMemmap(vector_storage) => {
                if force_half_precision {
                    Self::new_typed::<VectorElementTypeHalf>(
                        device,
                        gpu::GpuVectorStorageElementType::Float16,
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
                        gpu::GpuVectorStorageElementType::Float32,
                        vector_storage.total_vector_count(),
                        |id| Cow::Borrowed(vector_storage.get_dense(id)),
                    )
                }
            }
            VectorStorageEnum::DenseMemmapByte(vector_storage) => {
                Self::new_typed::<VectorElementTypeByte>(
                    device,
                    gpu::GpuVectorStorageElementType::Uint8,
                    vector_storage.total_vector_count(),
                    |id| Cow::Borrowed(vector_storage.get_dense(id)),
                )
            }
            VectorStorageEnum::DenseMemmapHalf(vector_storage) => {
                Self::new_typed::<VectorElementTypeHalf>(
                    device,
                    gpu::GpuVectorStorageElementType::Float16,
                    vector_storage.total_vector_count(),
                    |id| Cow::Borrowed(vector_storage.get_dense(id)),
                )
            }
            VectorStorageEnum::DenseAppendableMemmap(vector_storage) => {
                if force_half_precision {
                    Self::new_typed::<VectorElementTypeHalf>(
                        device,
                        gpu::GpuVectorStorageElementType::Float16,
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
                        gpu::GpuVectorStorageElementType::Float32,
                        vector_storage.total_vector_count(),
                        |id| Cow::Borrowed(vector_storage.get_dense(id)),
                    )
                }
            }
            VectorStorageEnum::DenseAppendableMemmapByte(vector_storage) => {
                Self::new_typed::<VectorElementTypeByte>(
                    device,
                    gpu::GpuVectorStorageElementType::Uint8,
                    vector_storage.total_vector_count(),
                    |id| Cow::Borrowed(vector_storage.get_dense(id)),
                )
            }
            VectorStorageEnum::DenseAppendableMemmapHalf(vector_storage) => {
                Self::new_typed::<VectorElementTypeHalf>(
                    device,
                    gpu::GpuVectorStorageElementType::Float16,
                    vector_storage.total_vector_count(),
                    |id| Cow::Borrowed(vector_storage.get_dense(id)),
                )
            }
            VectorStorageEnum::SparseSimple(_) => Err(gpu::GpuError::NotSupported),
            VectorStorageEnum::MultiDenseSimple(_) => Err(gpu::GpuError::NotSupported),
            VectorStorageEnum::MultiDenseSimpleByte(_) => Err(gpu::GpuError::NotSupported),
            VectorStorageEnum::MultiDenseSimpleHalf(_) => Err(gpu::GpuError::NotSupported),
            VectorStorageEnum::MultiDenseAppendableMemmap(_) => Err(gpu::GpuError::NotSupported),
            VectorStorageEnum::MultiDenseAppendableMemmapByte(_) => {
                Err(gpu::GpuError::NotSupported)
            }
            VectorStorageEnum::MultiDenseAppendableMemmapHalf(_) => {
                Err(gpu::GpuError::NotSupported)
            }
        }
    }

    fn new_typed<'a, TElement: Sized + Default + Copy + Clone + 'static>(
        device: Arc<gpu::Device>,
        element_type: gpu::GpuVectorStorageElementType,
        count: usize,
        get_vector: impl Fn(PointOffsetType) -> Cow<'a, [TElement]>,
    ) -> gpu::GpuResult<Self> {
        let timer = std::time::Instant::now();

        let dim = get_vector(0).len();
        let dim = dim.div_ceil(device.subgroup_size()) * device.subgroup_size();
        let working_group_size = dim;
        let vector_size_bytes = dim * std::mem::size_of::<TElement>();

        let vectors_in_max_buffer = device.max_buffer_size() / vector_size_bytes;
        let storages_count = count.div_ceil(vectors_in_max_buffer);
        // TODO(gpu) use constant and proper error
        if storages_count > 8 {
            return Err(gpu::GpuError::NotSupported);
        }

        let vectors_buffer: Vec<_> = (0..storages_count)
            .map(|i| -> gpu::GpuResult<Arc<gpu::Buffer>> {
                let vectors_in_buffer_count = if i == storages_count - 1 {
                    count - vectors_in_max_buffer * i
                } else {
                    vectors_in_max_buffer
                };
                Ok(Arc::new(gpu::Buffer::new(
                    device.clone(),
                    gpu::BufferType::Storage,
                    vectors_in_buffer_count * vector_size_bytes,
                )?))
            })
            .collect::<gpu::GpuResult<Vec<_>>>()?;

        let upload_vectors_capacity = UPLOAD_CHUNK_SIZE / vector_size_bytes;
        // TODO(gpu): use proper error
        if upload_vectors_capacity == 0 {
            return Err(gpu::GpuError::NotSupported);
        }
        let staging_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::CpuToGpu,
            upload_vectors_capacity * vector_size_bytes,
        )?);
        log::trace!(
            "GPU vector storage staging buffer size {}, upload_vectors_capacity = {}",
            staging_buffer.size,
            upload_vectors_capacity
        );

        let mut upload_context = gpu::Context::new(device.clone());
        let mut start_point_id: PointOffsetType = 0;
        for (storage_index, vector_buffer) in vectors_buffer.iter().enumerate() {
            let mut gpu_buffer_offset = 0;
            let mut upload_size = 0;
            let mut extended_vector = vec![TElement::default(); dim];

            let storage_points_count = (vector_buffer.size / vector_size_bytes) as PointOffsetType;
            for point_id in start_point_id..start_point_id + storage_points_count {
                let vector = get_vector(point_id);
                extended_vector[..vector.len()].copy_from_slice(&vector);
                staging_buffer.upload_slice(&extended_vector, upload_size);
                upload_size += vector_size_bytes;
                if upload_size / vector_size_bytes == upload_vectors_capacity {
                    upload_context.copy_gpu_buffer(
                        staging_buffer.clone(),
                        vectors_buffer[storage_index].clone(),
                        0,
                        gpu_buffer_offset,
                        upload_size,
                    );
                    upload_context.run();
                    upload_context.wait_finish();
                    gpu_buffer_offset += upload_size;
                    upload_size = 0;
                }
            }
            if upload_size > 0 {
                upload_context.copy_gpu_buffer(
                    staging_buffer.clone(),
                    vectors_buffer[storage_index].clone(),
                    0,
                    gpu_buffer_offset,
                    upload_size,
                );
                upload_context.run();
                upload_context.wait_finish();
            }
            start_point_id += storage_points_count;
        }

        log::debug!(
            "Upload vector data to GPU time = {:?}, vector data size {} MB, element type: {:?}",
            timer.elapsed(),
            vectors_buffer
                .iter()
                .map(|buffer| buffer.size)
                .sum::<usize>()
                / 1024
                / 1024,
            element_type,
        );

        let mut descriptor_set_layout_builder = gpu::DescriptorSetLayout::builder();
        for i in 0..storages_count {
            descriptor_set_layout_builder = descriptor_set_layout_builder.add_storage_buffer(i);
        }
        let descriptor_set_layout = descriptor_set_layout_builder.build(device.clone());

        let mut descriptor_set_builder = gpu::DescriptorSet::builder(descriptor_set_layout.clone());
        for (i, vector_buffer) in vectors_buffer.iter().enumerate() {
            descriptor_set_builder =
                descriptor_set_builder.add_storage_buffer(i, vector_buffer.clone());
        }

        let descriptor_set = descriptor_set_builder.build();

        Ok(Self {
            device,
            vectors_buffer,
            descriptor_set_layout,
            descriptor_set,
            dim,
            count,
            storages_count,
            // it's okay to use max size because if it's not too big,
            // we use singe buffer and avoid this definition
            storage_size: vectors_in_max_buffer,
            working_group_size,
            element_type,
        })
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
    ) -> gpu::GpuVectorStorageElementType {
        let num_vectors = 2048;
        let dim = 128;
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

        let scores_buffer = Arc::new(
            gpu::Buffer::new(
                device.clone(),
                gpu::BufferType::Storage,
                num_vectors * std::mem::size_of::<f32>(),
            )
            .unwrap(),
        );

        let descriptor_set_layout = gpu::DescriptorSetLayout::builder()
            .add_storage_buffer(0)
            .build(device.clone());

        let descriptor_set = gpu::DescriptorSet::builder(descriptor_set_layout.clone())
            .add_storage_buffer(0, scores_buffer.clone())
            .build();

        let shader = Arc::new(
            gpu::ShaderBuilder::new(device.clone(), gpu_vector_storage.working_group_size)
                .with_shader_code(include_str!("./shaders/common.comp"))
                .with_shader_code(include_str!("./shaders/vector_storage.comp"))
                .with_shader_code(include_str!("./shaders/tests/test_vector_storage.comp"))
                .with_element_type(gpu_vector_storage.element_type)
                .with_layout(gpu::LayoutSetBinding::VectorStorage, 1)
                .with_dim(gpu_vector_storage.dim)
                .with_storages_count(gpu_vector_storage.storages_count)
                .with_storage_size(gpu_vector_storage.storage_size)
                .build(),
        );

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
        context.dispatch(1, num_vectors, 1);

        let timer = std::time::Instant::now();
        context.run();
        context.wait_finish();
        println!("GPU scoring time = {:?}", timer.elapsed());

        let staging_buffer = Arc::new(
            gpu::Buffer::new(
                device.clone(),
                gpu::BufferType::GpuToCpu,
                num_vectors * std::mem::size_of::<f32>(),
            )
            .unwrap(),
        );
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
        assert_eq!(element, gpu::GpuVectorStorageElementType::Float32);
    }

    #[test]
    fn test_gpu_vector_storage_scoring_f16() {
        let element = test_gpu_vector_storage_scoring_impl(TestElementType::Float16, false);
        assert_eq!(element, gpu::GpuVectorStorageElementType::Float16);
    }

    #[test]
    fn test_gpu_vector_storage_scoring_u8() {
        let element = test_gpu_vector_storage_scoring_impl(TestElementType::Uint8, false);
        assert_eq!(element, gpu::GpuVectorStorageElementType::Uint8);
    }

    #[test]
    fn test_gpu_vector_storage_force_half_precision() {
        let element = test_gpu_vector_storage_scoring_impl(TestElementType::Float32, true);
        assert_eq!(element, gpu::GpuVectorStorageElementType::Float16);
    }

    #[test]
    fn test_gpu_vector_storage_binary_quantization() {
        let num_vectors = 16;
        let dim = 1024;
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
            gpu::GpuVectorStorageElementType::Binary
        );

        let scores_buffer = Arc::new(
            gpu::Buffer::new(
                device.clone(),
                gpu::BufferType::Storage,
                num_vectors * std::mem::size_of::<f32>(),
            )
            .unwrap(),
        );

        let descriptor_set_layout = gpu::DescriptorSetLayout::builder()
            .add_storage_buffer(0)
            .build(device.clone());

        let descriptor_set = gpu::DescriptorSet::builder(descriptor_set_layout.clone())
            .add_storage_buffer(0, scores_buffer.clone())
            .build();

        let shader = Arc::new(
            gpu::ShaderBuilder::new(device.clone(), gpu_vector_storage.working_group_size)
                .with_shader_code(include_str!("./shaders/common.comp"))
                .with_shader_code(include_str!("./shaders/vector_storage.comp"))
                .with_shader_code(include_str!("./shaders/tests/test_vector_storage.comp"))
                .with_element_type(gpu_vector_storage.element_type)
                .with_dim(gpu_vector_storage.dim)
                .with_storages_count(gpu_vector_storage.storages_count)
                .with_storage_size(gpu_vector_storage.storage_size)
                .with_layout(gpu::LayoutSetBinding::VectorStorage, 1)
                .build(),
        );

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
        context.dispatch(1, num_vectors, 1);

        let timer = std::time::Instant::now();
        context.run();
        context.wait_finish();
        println!("GPU scoring time = {:?}", timer.elapsed());

        let staging_buffer = Arc::new(
            gpu::Buffer::new(
                device.clone(),
                gpu::BufferType::GpuToCpu,
                num_vectors * std::mem::size_of::<f32>(),
            )
            .unwrap(),
        );
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
