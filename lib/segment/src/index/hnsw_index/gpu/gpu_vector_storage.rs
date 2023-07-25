use std::sync::Arc;

use crate::entry::entry_point::OperationResult;
use crate::types::PointOffsetType;
use crate::vector_storage::{VectorStorageEnum, VectorStorage};

#[repr(C)]
struct GpuVectorParamsBuffer {
    dim: u32,
    count: u32,
}

pub struct GpuVectorStorage {
    pub device: Arc<gpu::Device>,
    pub vectors_buffer: Arc<gpu::Buffer>,
    pub params_buffer: Arc<gpu::Buffer>,
    pub descriptor_set_layout: Arc<gpu::DescriptorSetLayout>,
    pub descriptor_set: Arc<gpu::DescriptorSet>,
}

impl GpuVectorStorage {
    pub fn new(
        device: Arc<gpu::Device>,
        vector_storage: &VectorStorageEnum,
    ) -> OperationResult<Self> {
        let timer = std::time::Instant::now();

        let dim = vector_storage.vector_dim();
        let count = vector_storage.total_vector_count();

        let storage_size = dim * count * std::mem::size_of::<f32>();
        let vectors_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::Storage,
            storage_size,
        ));
        let params_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::Uniform,
            std::mem::size_of::<GpuVectorParamsBuffer>(),
        ));

        let mut upload_context = gpu::Context::new(device.clone());
        let staging_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::CpuToGpu,
            dim * std::mem::size_of::<f32>(),
        ));

        let params = GpuVectorParamsBuffer {
            dim: dim as u32,
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

        for i in 0..count {
            let vector = vector_storage.get_vector(i as PointOffsetType);
            staging_buffer.upload_slice(vector, 0);
            upload_context.copy_gpu_buffer(
                staging_buffer.clone(),
                vectors_buffer.clone(),
                0,
                i * dim * std::mem::size_of::<f32>(),
                dim * std::mem::size_of::<f32>(),
            );
            upload_context.run();
            upload_context.wait_finish();
        }

        log::debug!(
            "Upload vector data to GPU time = {:?}, vector data size {} MB",
            timer.elapsed(),
            storage_size / 1024 / 1024
        );

        let descriptor_set_layout = gpu::DescriptorSetLayout::builder()
            .add_uniform_buffer(0)
            .add_storage_buffer(1)
            .build(device.clone());

        let descriptor_set = gpu::DescriptorSet::builder(descriptor_set_layout.clone())
            .add_uniform_buffer(0, params_buffer.clone())
            .add_storage_buffer(1, vectors_buffer.clone())
            .build();

        Ok(Self {
            device,
            vectors_buffer,
            params_buffer,
            descriptor_set_layout,
            descriptor_set,
        })
    }

    pub fn download_all(
        &self,
        dim: usize,
        count: usize,
    ) -> OperationResult<Vec<f32>> {
        let mut result = Vec::with_capacity(self.vectors_buffer.size);

        let mut download_context = gpu::Context::new(self.vectors_buffer.device.clone());
        let staging_buffer = Arc::new(gpu::Buffer::new(
            self.vectors_buffer.device.clone(),
            gpu::BufferType::GpuToCpu,
            dim * std::mem::size_of::<f32>(),
        ));
        let mut vector = vec![0.0; dim];
        for i in 0..count {
            download_context.copy_gpu_buffer(
                self.vectors_buffer.clone(),
                staging_buffer.clone(),
                i * dim * std::mem::size_of::<f32>(),
                0,
                dim * std::mem::size_of::<f32>(),
            );
            download_context.run();
            download_context.wait_finish();
            staging_buffer.download_slice(&mut vector, 0);
            result.extend_from_slice(&vector);
        }

        Ok(result)
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
    use crate::types::{Distance, PointOffsetType};
    use crate::vector_storage::simple_vector_storage::open_simple_vector_storage;

    #[test]
    fn test_gpu_vector_storage_upload() {
        let num_vectors = 1000;
        let dim = 256;
        println!("Data size = {} MB", num_vectors * dim * 4 / 1024 / 1024);

        let timer = std::time::Instant::now();
        let mut rnd = StdRng::seed_from_u64(42);
        let points = (0..num_vectors)
            .map(|_| random_vector(&mut rnd, dim))
            .collect::<Vec<_>>();
        println!("Random generate time = {:?}", timer.elapsed());

        let timer = std::time::Instant::now();
        let dir2 = tempfile::Builder::new().prefix("db_dir").tempdir().unwrap();
        let db = open_db(dir2.path(), &[DB_VECTOR_CF]).unwrap();
        let storage2 = open_simple_vector_storage(db, DB_VECTOR_CF, dim, Distance::Dot).unwrap();
        {
            let mut borrowed_storage2 = storage2.borrow_mut();
            points.iter().enumerate().for_each(|(i, vec)| {
                borrowed_storage2
                    .insert_vector(i as PointOffsetType, vec)
                    .unwrap();
            });
        }
        println!("Storage inserting time = {:?}", timer.elapsed());

        let debug_messenger = gpu::PanicIfErrorMessenger {};
        let instance =
            Arc::new(gpu::Instance::new("qdrant", Some(&debug_messenger), false).unwrap());
        let device =
            Arc::new(gpu::Device::new(instance.clone(), instance.vk_physical_devices[0]).unwrap());

        let timer = std::time::Instant::now();
        let gpu_vector_storage = GpuVectorStorage::new(device, &storage2.borrow()).unwrap();
        println!("Upload time = {:?}", timer.elapsed());

        let downloaded = gpu_vector_storage.download_all(
            storage2.borrow().vector_dim(),
            storage2.borrow().total_vector_count(),
        ).unwrap();
        assert_eq!(downloaded.len(), num_vectors * dim);
        for i in 0..num_vectors {
            let vec = &points[i];
            let downloaded_vec = &downloaded[i * dim..(i + 1) * dim];
            assert_eq!(vec, downloaded_vec);
        }
    }

    #[test]
    fn test_gpu_vector_storage_scoring() {
        let num_vectors = 1000;
        let dim = 256;

        let mut rnd = StdRng::seed_from_u64(42);
        let points = (0..num_vectors)
            .map(|_| random_vector(&mut rnd, dim))
            .collect::<Vec<_>>();

        let dir2 = tempfile::Builder::new().prefix("db_dir").tempdir().unwrap();
        let db = open_db(dir2.path(), &[DB_VECTOR_CF]).unwrap();
        let storage2 = open_simple_vector_storage(db, DB_VECTOR_CF, dim, Distance::Dot).unwrap();
        {
            let mut borrowed_storage2 = storage2.borrow_mut();
            points.iter().enumerate().for_each(|(i, vec)| {
                borrowed_storage2
                    .insert_vector(i as PointOffsetType, vec)
                    .unwrap();
            });
        }

        let debug_messenger = gpu::PanicIfErrorMessenger {};
        let instance =
            Arc::new(gpu::Instance::new("qdrant", Some(&debug_messenger), false).unwrap());
        let device =
            Arc::new(gpu::Device::new(instance.clone(), instance.vk_physical_devices[0]).unwrap());
        
        let gpu_vector_storage = GpuVectorStorage::new(device.clone(), &storage2.borrow()).unwrap();

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
            .add_descriptor_set_layout(0, gpu_vector_storage.descriptor_set_layout.clone())
            .add_descriptor_set_layout(1, descriptor_set_layout.clone())
            .add_shader(shader.clone())
            .build(device.clone());

        let mut context = gpu::Context::new(device.clone());
        context.bind_pipeline(
            pipeline,
            &[gpu_vector_storage.descriptor_set.clone(), descriptor_set],
        );
        context.dispatch(num_vectors, 0, 0);
        context.run();
        context.wait_finish();

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

        for i in 0..num_vectors {
            let score = DotProductMetric::similarity(&points[0], &points[i]);
            assert_eq!(score, scores[i]);
        }
    }
}
