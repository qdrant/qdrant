use std::sync::Arc;

use crate::common::operation_error::{OperationError, OperationResult};

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
    pub fn new(device: Arc<gpu::Device>, threads_count: usize, ef: usize) -> OperationResult<Self> {
        if threads_count % device.subgroup_size() != 0 {
            return Err(OperationError::service_error(
                "Threads count must be a multiple of subgroup size",
            ));
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
    use common::fixed_length_priority_queue::FixedLengthPriorityQueue;
    use common::types::{PointOffsetType, ScoredPointOffset};
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::*;

    #[test]
    fn test_gpu_nearest_heap() {
        let ef = 100;
        let points_count = 100; // TODO: revert to 1024
        let groups_count = 2; // TODO: revert to 8
        let inputs_count = points_count;

        let mut rng = StdRng::seed_from_u64(41);
        let inputs_data: Vec<ScoredPointOffset> = (0..inputs_count * groups_count)
            .map(|i| ScoredPointOffset {
                idx: (i % inputs_count) as PointOffsetType,
                score: rng.gen_range(-1.0..1.0),
            })
            .collect();

        let debug_messenger = gpu::PanicIfErrorMessenger {};
        let instance =
            Arc::new(gpu::Instance::new("qdrant", Some(&debug_messenger), false).unwrap());
        let device =
            Arc::new(gpu::Device::new(instance.clone(), instance.vk_physical_devices[0]).unwrap());

        let threads_count = device.subgroup_size() * groups_count;
        let mut context = gpu::Context::new(device.clone());
        let gpu_nearest_heap = GpuNearestHeap::new(device.clone(), threads_count, ef).unwrap();

        let shader = Arc::new(gpu::Shader::new(
            device.clone(),
            include_bytes!("./shaders/test_nearest_heap.spv"),
        ));

        let descriptor_set_layout = gpu::DescriptorSetLayout::builder()
            .add_storage_buffer(0)
            .add_storage_buffer(1)
            .add_storage_buffer(2)
            .build(device.clone());

        let input_points_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::Storage,
            inputs_count * groups_count * std::mem::size_of::<ScoredPointOffset>(),
        ));

        let upload_staging_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::CpuToGpu,
            inputs_count * groups_count * std::mem::size_of::<ScoredPointOffset>(),
        ));
        upload_staging_buffer.upload_slice(&inputs_data, 0);
        context.copy_gpu_buffer(
            upload_staging_buffer,
            input_points_buffer.clone(),
            0,
            0,
            input_points_buffer.size,
        );
        context.run();
        context.wait_finish();

        let scores_output_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::Storage,
            inputs_count * groups_count * std::mem::size_of::<f32>(),
        ));

        let sorted_output_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::Storage,
            ef * groups_count * std::mem::size_of::<ScoredPointOffset>(),
        ));

        let descriptor_set = gpu::DescriptorSet::builder(descriptor_set_layout.clone())
            .add_storage_buffer(0, input_points_buffer.clone())
            .add_storage_buffer(1, scores_output_buffer.clone())
            .add_storage_buffer(2, sorted_output_buffer.clone())
            .build();

        let pipeline = gpu::Pipeline::builder()
            .add_descriptor_set_layout(0, descriptor_set_layout.clone())
            .add_descriptor_set_layout(1, gpu_nearest_heap.descriptor_set_layout.clone())
            .add_shader(shader.clone())
            .build(device.clone());

        context.bind_pipeline(
            pipeline,
            &[
                descriptor_set.clone(),
                gpu_nearest_heap.descriptor_set.clone(),
            ],
        );
        context.dispatch(threads_count, 1, 1);
        context.run();
        context.wait_finish();

        let download_staging_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::GpuToCpu,
            std::cmp::max(scores_output_buffer.size, sorted_output_buffer.size),
        ));
        context.copy_gpu_buffer(
            scores_output_buffer.clone(),
            download_staging_buffer.clone(),
            0,
            0,
            scores_output_buffer.size,
        );
        context.run();
        context.wait_finish();

        let mut scores_output = vec![0.0; inputs_count * groups_count];
        download_staging_buffer.download_slice(&mut scores_output, 0);

        context.copy_gpu_buffer(
            sorted_output_buffer.clone(),
            download_staging_buffer.clone(),
            0,
            0,
            sorted_output_buffer.size,
        );
        context.run();
        context.wait_finish();

        let mut sorted_output = vec![ScoredPointOffset { idx: 0, score: 0.0 }; ef * groups_count];
        download_staging_buffer.download_slice(&mut sorted_output, 0);

        let mut scores_output_cpu = vec![0.0; inputs_count * groups_count];
        let mut sorted_output_cpu =
            vec![ScoredPointOffset { idx: 0, score: 0.0 }; ef * groups_count];
        for group in 0..groups_count {
            let mut queue = FixedLengthPriorityQueue::new(ef);
            for i in 0..inputs_count {
                let scored_point = inputs_data[group * inputs_count + i];
                queue.push(scored_point);
                scores_output_cpu[group * inputs_count + i] = queue.top().unwrap().score;
            }
            let sorted = queue.into_vec();
            for i in 0..ef {
                sorted_output_cpu[group * ef + i] = sorted[i];
            }
        }

        // TODO: remove
        for i in 0..inputs_count * groups_count {
            println!(
                "{}: gpu: {}, cpu: {}, input {}",
                i, scores_output[i], scores_output_cpu[i], inputs_data[i].score
            );
            assert!((scores_output[i] - scores_output_cpu[i]).abs() < 1e-6);
        }

        assert_eq!(scores_output, scores_output_cpu);
        //assert_eq!(sorted_output, sorted_output_cpu);
    }
}
