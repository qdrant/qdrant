use std::sync::Arc;

use crate::entry::entry_point::{OperationError, OperationResult};

#[repr(C)]
struct GpuNearestHeapParamsBuffer {
    capacity: u32,
}

pub struct GpuNearestHeap {
    pub ef: usize,
    pub device: Arc<gpu::Device>,
    pub params_buffer: Arc<gpu::Buffer>,
    pub nearest_scores_buffer: Arc<gpu::Buffer>,
    pub nearest_indices_buffer: Arc<gpu::Buffer>,
    pub descriptor_set_layout: Arc<gpu::DescriptorSetLayout>,
    pub descriptor_set: Arc<gpu::DescriptorSet>,
}

impl GpuNearestHeap {
    pub fn new(
        device: Arc<gpu::Device>,
        threads_count: usize,
        ef: usize,
    ) -> OperationResult<Self> {
        if threads_count % device.subgroup_size() != 0 {
            return Err(OperationError::service_error("Threads count must be a multiple of subgroup size"));
        }

        let ceiled_ef = ef.div_ceil(device.subgroup_size()) * device.subgroup_size();
        let buffers_elements_count = ceiled_ef * threads_count / device.subgroup_size();

        let nearest_scores_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::Storage,
            buffers_elements_count * std::mem::size_of::<f32>(),
        ));
        let nearest_indices_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::Storage,
            buffers_elements_count * std::mem::size_of::<u32>(),
        ));
        let params_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::Uniform,
            std::mem::size_of::<GpuNearestHeapParamsBuffer>(),
        ));

        let staging_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::CpuToGpu,
            std::mem::size_of::<GpuNearestHeapParamsBuffer>(),
        ));

        let params = GpuNearestHeapParamsBuffer {
            capacity: ceiled_ef as u32,
        };
        staging_buffer.upload(&params, 0);

        let mut upload_context = gpu::Context::new(device.clone());
        upload_context.copy_gpu_buffer(
            staging_buffer.clone(),
            params_buffer.clone(),
            0,
            0,
            std::mem::size_of::<GpuNearestHeapParamsBuffer>(),
        );
        upload_context.run();
        upload_context.wait_finish();

        let descriptor_set_layout = gpu::DescriptorSetLayout::builder()
            .add_uniform_buffer(0)
            .add_storage_buffer(1)
            .add_storage_buffer(2)
            .build(device.clone());

        let descriptor_set = gpu::DescriptorSet::builder(descriptor_set_layout.clone())
            .add_uniform_buffer(0, params_buffer.clone())
            .add_storage_buffer(1, nearest_scores_buffer.clone())
            .add_storage_buffer(2, nearest_indices_buffer.clone())
            .build();

        Ok(Self {
            ef,
            device,
            params_buffer,
            nearest_scores_buffer,
            nearest_indices_buffer,
            descriptor_set_layout,
            descriptor_set,
        })
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::*;
    use crate::data_types::groups;
    use crate::index::hnsw_index::gpu::gpu_links::GpuLinks;
    use crate::types::PointOffsetType;

    #[test]
    fn test_gpu_nearest_heap() {
        let m = 8;
        let ef = 8;
        let points_count = 1024;
        let groups_count = 4;
        let inputs_count = 1024;

        let debug_messenger = gpu::PanicIfErrorMessenger {};
        let instance =
            Arc::new(gpu::Instance::new("qdrant", Some(&debug_messenger), false).unwrap());
        let device =
            Arc::new(gpu::Device::new(instance.clone(), instance.vk_physical_devices[0]).unwrap());

        let threads_count = device.subgroup_size() * groups_count;
        let mut context = gpu::Context::new(device.clone());
        let mut gpu_links = GpuLinks::new(device.clone(), m, ef, m, points_count).unwrap();
        let mut gpu_nearest_heap = GpuNearestHeap::new(device.clone(), threads_count, ef).unwrap();

        let shader = Arc::new(gpu::Shader::new(
            device.clone(),
            include_bytes!("./shaders/test_nearest_heap.spv"),
        ));

        let descriptor_set_layout = gpu::DescriptorSetLayout::builder()
            .add_storage_buffer(0)
            .add_storage_buffer(1)
            .add_storage_buffer(2)
            .build(device.clone());

        let descriptor_set = gpu::DescriptorSet::builder(descriptor_set_layout.clone())
            //.add_storage_buffer(0, input_points_buffer.clone())
            //.add_storage_buffer(1, scores_output_buffer.clone())
            //.add_storage_buffer(2, sorted_output_buffer.clone())
            .build();

        let pipeline = gpu::Pipeline::builder()
            .add_descriptor_set_layout(0, descriptor_set_layout.clone())
            .add_descriptor_set_layout(1, gpu_links.descriptor_set_layout.clone())
            .add_descriptor_set_layout(2, gpu_nearest_heap.descriptor_set_layout.clone())
            .add_shader(shader.clone())
            .build(device.clone());

        context.bind_pipeline(pipeline, &[
            descriptor_set.clone(),
            gpu_links.descriptor_set.clone(),
            gpu_nearest_heap.descriptor_set.clone(),
        ]);
        context.dispatch(threads_count, 1, 1);
        context.run();
        context.wait_finish();
    }
}
