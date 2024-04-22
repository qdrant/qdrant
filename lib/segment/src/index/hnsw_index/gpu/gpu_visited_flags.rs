use std::sync::Arc;

use crate::common::operation_error::{OperationError, OperationResult};

#[repr(C)]
struct GpuVisitedFlagsParamsBuffer {
    capacity: u32,
    generation: u32,
}

pub struct GpuVisitedFlags {
    pub device: Arc<gpu::Device>,
    params: GpuVisitedFlagsParamsBuffer,
    pub params_buffer: Arc<gpu::Buffer>,
    pub params_staging_buffer: Arc<gpu::Buffer>,
    pub visited_flags_buffer: Arc<gpu::Buffer>,
    pub descriptor_set_layout: Arc<gpu::DescriptorSetLayout>,
    pub descriptor_set: Arc<gpu::DescriptorSet>,
}

impl GpuVisitedFlags {
    pub fn new(
        device: Arc<gpu::Device>,
        threads_count: usize,
        points_count: usize,
    ) -> OperationResult<Self> {
        if threads_count % device.subgroup_size() != 0 {
            return Err(OperationError::service_error(
                "Threads count must be a multiple of subgroup size",
            ));
        }

        let params_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::Uniform,
            std::mem::size_of::<GpuVisitedFlagsParamsBuffer>(),
        ));
        let params_staging_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::CpuToGpu,
            std::mem::size_of::<GpuVisitedFlagsParamsBuffer>(),
        ));
        let visited_flags_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::Storage,
            (threads_count / device.subgroup_size()) * points_count * std::mem::size_of::<u8>(),
        ));

        let params = GpuVisitedFlagsParamsBuffer {
            capacity: points_count as u32,
            generation: 1,
        };
        params_staging_buffer.upload(&params, 0);

        let mut upload_context = gpu::Context::new(device.clone());
        upload_context.copy_gpu_buffer(
            params_staging_buffer.clone(),
            params_buffer.clone(),
            0,
            0,
            std::mem::size_of::<GpuVisitedFlagsParamsBuffer>(),
        );
        upload_context.run();
        upload_context.wait_finish();

        let descriptor_set_layout = gpu::DescriptorSetLayout::builder()
            .add_uniform_buffer(0)
            .add_storage_buffer(1)
            .build(device.clone());

        let descriptor_set = gpu::DescriptorSet::builder(descriptor_set_layout.clone())
            .add_uniform_buffer(0, params_buffer.clone())
            .add_storage_buffer(1, visited_flags_buffer.clone())
            .build();

        Ok(Self {
            device,
            params,
            params_buffer,
            params_staging_buffer,
            visited_flags_buffer,
            descriptor_set_layout,
            descriptor_set,
        })
    }

    pub fn clear(&mut self, gpu_context: &mut gpu::Context) {
        if self.params.generation == 255 {
            self.params.generation = 1;
            gpu_context.clear_buffer(self.visited_flags_buffer.clone());
        } else {
            self.params.generation += 1;
        }

        self.params_staging_buffer.upload(&self.params, 0);
        gpu_context.copy_gpu_buffer(
            self.params_staging_buffer.clone(),
            self.params_buffer.clone(),
            0,
            0,
            self.params_buffer.size,
        );
    }
}
