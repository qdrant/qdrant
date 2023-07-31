use std::sync::Arc;

use crate::types::PointOffsetType;

#[repr(C)]
struct GpuBuilderContextParamsBuffer {
    processed_requests_count: u32,
}

pub struct GpuBuilderContext {
    pub device: Arc<gpu::Device>,
    pub builder_params_buffer: Arc<gpu::Buffer>,
    pub requests_buffer: Arc<gpu::Buffer>,
    pub responses_buffer: Arc<gpu::Buffer>,
    pub process_points_buffer: Arc<gpu::Buffer>,
    pub process_points_staging_buffer: Arc<gpu::Buffer>,
    pub descriptor_set_layout: Arc<gpu::DescriptorSetLayout>,
    pub descriptor_set: Arc<gpu::DescriptorSet>,
}

impl GpuBuilderContext {
    pub fn new(
        device: Arc<gpu::Device>,
        m: usize,
        points_count: usize,
        threads_count: usize,
    ) -> Self {
        let builder_params_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::Uniform,
            std::mem::size_of::<GpuBuilderContextParamsBuffer>(),
        ));

        let requests_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::Storage,
            points_count * std::mem::size_of::<PointOffsetType>(),
        ));

        let response_size = (m + 2) * std::mem::size_of::<PointOffsetType>();
        let thread_response_size = m * response_size + std::mem::size_of::<PointOffsetType>();
        let full_responses_size = threads_count * thread_response_size;
        let responses_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::Storage,
            full_responses_size,
        ));

        let process_points_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::Storage,
            threads_count * std::mem::size_of::<PointOffsetType>(),
        ));
        let process_points_staging_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::CpuToGpu,
            threads_count * std::mem::size_of::<PointOffsetType>(),
        ));

        let descriptor_set_layout = gpu::DescriptorSetLayout::builder()
            .add_uniform_buffer(0)
            .add_storage_buffer(1)
            .add_storage_buffer(2)
            .add_storage_buffer(3)
            .build(device.clone());

        let descriptor_set = gpu::DescriptorSet::builder(descriptor_set_layout.clone())
            .add_uniform_buffer(0, builder_params_buffer.clone())
            .add_storage_buffer(1, requests_buffer.clone())
            .add_storage_buffer(2, responses_buffer.clone())
            .add_storage_buffer(3, process_points_buffer.clone())
            .build();

        Self {
            device,
            builder_params_buffer,
            requests_buffer,
            responses_buffer,
            process_points_buffer,
            process_points_staging_buffer,
            descriptor_set_layout,
            descriptor_set,
        }
    }

    pub fn upload_entries(&self, gpu_context: &mut gpu::Context, entries: &[PointOffsetType]) {
        let staging_buffer = Arc::new(gpu::Buffer::new(
            self.device.clone(),
            gpu::BufferType::CpuToGpu,
            entries.len() * std::mem::size_of::<PointOffsetType>(),
        ));
        staging_buffer.upload_slice(entries, 0);
        gpu_context.copy_gpu_buffer(
            staging_buffer.clone(),
            self.requests_buffer.clone(),
            0,
            0,
            entries.len() * std::mem::size_of::<PointOffsetType>(),
        );
        gpu_context.run();
        gpu_context.wait_finish();
    }

    pub fn upload_process_points(
        &self,
        gpu_context: &mut gpu::Context,
        points: &[PointOffsetType],
    ) {
        self.process_points_staging_buffer.upload_slice(points, 0);
        gpu_context.copy_gpu_buffer(
            self.process_points_staging_buffer.clone(),
            self.process_points_buffer.clone(),
            0,
            0,
            points.len() * std::mem::size_of::<PointOffsetType>(),
        );
        gpu_context.run();
        gpu_context.wait_finish();
    }
}
