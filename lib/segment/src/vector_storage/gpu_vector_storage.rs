use std::sync::Arc;

use super::{VectorStorage, VectorStorageEnum};
use crate::entry::entry_point::OperationResult;
use crate::types::PointOffsetType;

pub struct GpuVectorStorage {
    pub dim: usize,
    pub count: usize,
    pub gpu_buffer: Arc<gpu::Buffer>,
}

impl GpuVectorStorage {
    pub fn new(
        gpu_device: Arc<gpu::Device>,
        vector_storage: &VectorStorageEnum,
    ) -> OperationResult<Self> {
        let timer = std::time::Instant::now();

        let dim = vector_storage.vector_dim();
        let count = vector_storage.total_vector_count();

        let storage_size = dim * count * std::mem::size_of::<f32>();
        let gpu_buffer = Arc::new(gpu::Buffer::new(
            gpu_device.clone(),
            gpu::BufferType::Storage,
            storage_size,
        ));

        let mut upload_context = gpu::Context::new(gpu_device.clone());
        let staging_buffer = Arc::new(gpu::Buffer::new(
            gpu_device.clone(),
            gpu::BufferType::CpuToGpu,
            dim * std::mem::size_of::<f32>(),
        ));
        for i in 0..count {
            let vector = vector_storage.get_vector(i as PointOffsetType);
            staging_buffer.upload_slice(vector, 0);
            upload_context.copy_gpu_buffer(
                staging_buffer.clone(),
                gpu_buffer.clone(),
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

        Ok(Self {
            dim,
            count,
            gpu_buffer,
        })
    }

    pub fn download_all(&self) -> OperationResult<Vec<f32>> {
        let mut result = Vec::with_capacity(self.gpu_buffer.size);

        let mut download_context = gpu::Context::new(self.gpu_buffer.device.clone());
        let staging_buffer = Arc::new(gpu::Buffer::new(
            self.gpu_buffer.device.clone(),
            gpu::BufferType::GpuToCpu,
            self.dim * std::mem::size_of::<f32>(),
        ));
        let mut vector = vec![0.0; self.dim];
        for i in 0..self.count {
            download_context.copy_gpu_buffer(
                self.gpu_buffer.clone(),
                staging_buffer.clone(),
                i * self.dim * std::mem::size_of::<f32>(),
                0,
                self.dim * std::mem::size_of::<f32>(),
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
    use crate::types::{Distance, PointOffsetType};
    use crate::vector_storage::simple_vector_storage::open_simple_vector_storage;

    #[test]
    fn test_gpu_vector_storage() {
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

        let debug_messenger = gpu::PanicIfErrorMessenger {}; //Some(&debug_messenger)
        let instance =
            Arc::new(gpu::Instance::new("qdrant", Some(&debug_messenger), false).unwrap());
        let device =
            Arc::new(gpu::Device::new(instance.clone(), instance.vk_physical_devices[0]).unwrap());

        let timer = std::time::Instant::now();
        let gpu_vector_storage = GpuVectorStorage::new(device, &storage2.borrow()).unwrap();
        println!("Upload time = {:?}", timer.elapsed());

        let downloaded = gpu_vector_storage.download_all().unwrap();
        assert_eq!(downloaded.len(), num_vectors * dim);
        for i in 0..num_vectors {
            let vec = &points[i];
            let downloaded_vec = &downloaded[i * dim..(i + 1) * dim];
            assert_eq!(vec, downloaded_vec);
        }
    }
}
