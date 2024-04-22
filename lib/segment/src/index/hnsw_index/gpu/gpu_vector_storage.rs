use std::sync::Arc;

use common::types::PointOffsetType;

use crate::common::operation_error::OperationResult;
use crate::data_types::vectors::DenseVector;
use crate::vector_storage::{VectorStorage, VectorStorageEnum};

pub const ALIGNMENT: usize = 32 * 4;
pub const UPLOAD_CHUNK_SIZE: usize = 64 * 1024 * 1024;
pub const STORAGES_COUNT: usize = 4;

#[repr(C)]
struct GpuVectorParamsBuffer {
    dim: u32,
    count: u32,
}

pub struct GpuVectorStorage {
    pub device: Arc<gpu::Device>,
    pub vectors_buffer: Vec<Arc<gpu::Buffer>>,
    pub params_buffer: Arc<gpu::Buffer>,
    pub descriptor_set_layout: Arc<gpu::DescriptorSetLayout>,
    pub descriptor_set: Arc<gpu::DescriptorSet>,
    pub dim: usize,
    pub count: usize,
}

impl GpuVectorStorage {
    pub fn new(
        device: Arc<gpu::Device>,
        vector_storage: &VectorStorageEnum,
    ) -> OperationResult<Self> {
        let timer = std::time::Instant::now();

        let dim = vector_storage.vector_dim();
        let capacity = Self::get_capacity(dim);
        let upload_points_count = UPLOAD_CHUNK_SIZE / (capacity * std::mem::size_of::<f32>());

        let count = vector_storage.total_vector_count();
        let points_in_storage_count = Self::get_points_in_storage_count(count);
        let vectors_buffer = (0..STORAGES_COUNT)
            .map(|_| {
                Arc::new(gpu::Buffer::new(
                    device.clone(),
                    gpu::BufferType::Storage,
                    points_in_storage_count * capacity * std::mem::size_of::<f32>(),
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
            upload_points_count * capacity * std::mem::size_of::<f32>(),
        ));
        println!(
            "Staging buffer size {}, upload_points_count = {}",
            staging_buffer.size, upload_points_count
        );

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

        for storage_index in 0..STORAGES_COUNT {
            let mut gpu_offset = 0;
            let mut upload_size = 0;
            let mut upload_points = 0;
            let mut extended_vector = vec![0.0f32; capacity];
            for point_id in 0..count {
                if point_id % STORAGES_COUNT != storage_index {
                    continue;
                }

                let vector = vector_storage.get_vector(point_id as PointOffsetType);
                let vector: DenseVector = vector.try_into().unwrap();
                extended_vector[..vector.len()].copy_from_slice(&vector);
                staging_buffer.upload_slice(
                    &extended_vector,
                    upload_points * capacity * std::mem::size_of::<f32>(),
                );
                upload_size += capacity * std::mem::size_of::<f32>();
                upload_points += 1;

                if upload_points == upload_points_count {
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
            "Upload vector data to GPU time = {:?}, vector data size {} MB",
            timer.elapsed(),
            STORAGES_COUNT * points_in_storage_count * capacity * std::mem::size_of::<f32>()
                / 1024
                / 1024
        );

        let mut descriptor_set_layout_builder =
            gpu::DescriptorSetLayout::builder().add_uniform_buffer(0);
        for i in 0..STORAGES_COUNT {
            descriptor_set_layout_builder = descriptor_set_layout_builder.add_storage_buffer(i + 1);
        }
        let descriptor_set_layout = descriptor_set_layout_builder.build(device.clone());

        let mut descriptor_set_builder = gpu::DescriptorSet::builder(descriptor_set_layout.clone())
            .add_uniform_buffer(0, params_buffer.clone());
        for i in 0..STORAGES_COUNT {
            descriptor_set_builder =
                descriptor_set_builder.add_storage_buffer(i + 1, vectors_buffer[i].clone());
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
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    use super::*;
    use crate::common::rocksdb_wrapper::{open_db, DB_VECTOR_CF};
    use crate::fixtures::index_fixtures::random_vector;
    use crate::spaces::metric::Metric;
    use crate::spaces::simple::DotProductMetric;
    use crate::types::Distance;
    use crate::vector_storage::dense::simple_dense_vector_storage::open_simple_dense_vector_storage;

    #[test]
    fn test_gpu_vector_storage_scoring() {
        let num_vectors = 2048;
        let dim = 1536;
        let capacity = 1536;
        let test_point_id = 0usize;

        let mut rnd = StdRng::seed_from_u64(42);
        let points = (0..num_vectors)
            .map(|_| random_vector(&mut rnd, dim))
            .collect::<Vec<_>>();

        let dir = tempfile::Builder::new().prefix("db_dir").tempdir().unwrap();
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
        let storage =
            open_simple_dense_vector_storage(db, DB_VECTOR_CF, dim, Distance::Dot, &false.into())
                .unwrap();
        {
            let mut borrowed_storage = storage.borrow_mut();
            points.iter().enumerate().for_each(|(i, vec)| {
                borrowed_storage
                    .insert_vector(i as PointOffsetType, vec.into())
                    .unwrap();
            });
        }

        let debug_messenger = gpu::PanicIfErrorMessenger {};
        let instance =
            Arc::new(gpu::Instance::new("qdrant", Some(&debug_messenger), false).unwrap());
        let device =
            Arc::new(gpu::Device::new(instance.clone(), instance.vk_physical_devices[0]).unwrap());

        let gpu_vector_storage = GpuVectorStorage::new(device.clone(), &storage.borrow()).unwrap();

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
            include_bytes!("./shaders/test_vector_storage.spv"),
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
        context.dispatch(32 * num_vectors, 1, 1);

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
            assert!((score - scores[i]).abs() < 1.0);
        }
        println!("CPU scoring time = {:?}", timer.elapsed());
    }
}
