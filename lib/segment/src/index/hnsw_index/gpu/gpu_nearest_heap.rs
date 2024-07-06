pub struct GpuNearestHeap {
    pub ef: usize,
    pub capacity: usize,
}

impl GpuNearestHeap {
    pub fn new(
        ef: usize,
        capacity: usize,
        working_group_size: usize,
    ) -> gpu::GpuResult<Self> {
        assert!(capacity >= ef);
        let capacity = capacity.div_ceil(working_group_size) * working_group_size;
        Ok(Self { ef, capacity })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common::fixed_length_priority_queue::FixedLengthPriorityQueue;
    use common::types::{PointOffsetType, ScoredPointOffset};
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::*;

    #[repr(C)]
    struct TestParams {
        input_counts: u32,
    }

    #[test]
    fn test_gpu_nearest_heap() {
        let ef = 200;
        let points_count = 300;
        let groups_count = 4;
        let inputs_count = points_count;
        let working_group_size = 128;

        let mut rng = StdRng::seed_from_u64(42);
        let inputs_data: Vec<ScoredPointOffset> = (0..inputs_count * groups_count)
            .map(|i| ScoredPointOffset {
                idx: i as PointOffsetType,
                score: rng.gen_range(-1.0..1.0),
            })
            .collect();

        let debug_messenger = gpu::PanicIfErrorMessenger {};
        let instance =
            Arc::new(gpu::Instance::new("qdrant", Some(&debug_messenger), false).unwrap());
        let device =
            Arc::new(gpu::Device::new(instance.clone(), instance.vk_physical_devices[0]).unwrap());

        let gpu_nearest_heap = GpuNearestHeap::new(ef, ef, working_group_size).unwrap();

        let shader = Arc::new(
            gpu::ShaderBuilder::new(device.clone(), working_group_size)
                .with_shader_code(include_str!("./shaders/common.comp"))
                .with_shader_code(include_str!("./shaders/nearest_heap.comp"))
                .with_shader_code(include_str!("./shaders/tests/test_nearest_heap.comp"))
                .with_nearest_heap_capacity(gpu_nearest_heap.capacity)
                .with_nearest_heap_ef(gpu_nearest_heap.ef)
                .build(),
        );

        let input_points_buffer = Arc::new(
            gpu::Buffer::new(
                device.clone(),
                gpu::BufferType::Storage,
                inputs_count * groups_count * std::mem::size_of::<ScoredPointOffset>(),
            )
            .unwrap(),
        );

        let mut context = gpu::Context::new(device.clone());
        let upload_staging_buffer = Arc::new(
            gpu::Buffer::new(
                device.clone(),
                gpu::BufferType::CpuToGpu,
                inputs_count * groups_count * std::mem::size_of::<ScoredPointOffset>(),
            )
            .unwrap(),
        );
        upload_staging_buffer.upload_slice(&inputs_data, 0);
        context.copy_gpu_buffer(
            upload_staging_buffer.clone(),
            input_points_buffer.clone(),
            0,
            0,
            input_points_buffer.size,
        );
        context.run();
        context.wait_finish();

        let test_params_buffer = Arc::new(
            gpu::Buffer::new(
                device.clone(),
                gpu::BufferType::Uniform,
                std::mem::size_of::<TestParams>(),
            )
            .unwrap(),
        );
        upload_staging_buffer.upload(
            &TestParams {
                input_counts: inputs_count as u32,
            },
            0,
        );
        context.copy_gpu_buffer(
            upload_staging_buffer,
            test_params_buffer.clone(),
            0,
            0,
            test_params_buffer.size,
        );
        context.run();
        context.wait_finish();

        let scores_output_buffer = Arc::new(
            gpu::Buffer::new(
                device.clone(),
                gpu::BufferType::Storage,
                inputs_count * groups_count * std::mem::size_of::<f32>(),
            )
            .unwrap(),
        );
        let sorted_output_buffer = Arc::new(
            gpu::Buffer::new(
                device.clone(),
                gpu::BufferType::Storage,
                ef * groups_count * std::mem::size_of::<PointOffsetType>(),
            )
            .unwrap(),
        );

        let descriptor_set_layout = gpu::DescriptorSetLayout::builder()
            .add_uniform_buffer(0)
            .add_storage_buffer(1)
            .add_storage_buffer(2)
            .add_storage_buffer(3)
            .build(device.clone());

        let descriptor_set = gpu::DescriptorSet::builder(descriptor_set_layout.clone())
            .add_uniform_buffer(0, test_params_buffer.clone())
            .add_storage_buffer(1, input_points_buffer.clone())
            .add_storage_buffer(2, scores_output_buffer.clone())
            .add_storage_buffer(3, sorted_output_buffer.clone())
            .build();

        let pipeline = gpu::Pipeline::builder()
            .add_descriptor_set_layout(0, descriptor_set_layout.clone())
            .add_shader(shader.clone())
            .build(device.clone());

        context.bind_pipeline(pipeline, &[descriptor_set.clone()]);
        context.dispatch(1, groups_count, 1);
        context.run();
        context.wait_finish();

        let download_staging_buffer = Arc::new(
            gpu::Buffer::new(
                device.clone(),
                gpu::BufferType::GpuToCpu,
                std::cmp::max(scores_output_buffer.size, sorted_output_buffer.size),
            )
            .unwrap(),
        );
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

        let mut scores_output_cpu = vec![0.0; inputs_count * groups_count];
        let mut sorted_output_cpu = vec![PointOffsetType::default(); ef * groups_count];
        for group in 0..groups_count {
            let mut queue = FixedLengthPriorityQueue::<ScoredPointOffset>::new(ef);
            for i in 0..inputs_count {
                let scored_point = inputs_data[group * inputs_count + i];
                queue.push(scored_point);
                scores_output_cpu[group * inputs_count + i] = queue.top().unwrap().score;
            }
            let sorted = queue.into_vec();
            for i in 0..ef {
                sorted_output_cpu[group * ef + i] = sorted[i].idx;
            }
        }

        let mut nearest_gpu: Vec<PointOffsetType> =
            vec![Default::default(); gpu_nearest_heap.ef * groups_count];
        context.copy_gpu_buffer(
            sorted_output_buffer.clone(),
            download_staging_buffer.clone(),
            0,
            0,
            nearest_gpu.len() * std::mem::size_of::<PointOffsetType>(),
        );
        context.run();
        context.wait_finish();
        download_staging_buffer.download_slice(nearest_gpu.as_mut_slice(), 0);

        let mut sorted_output_gpu = Vec::new();
        for group in 0..groups_count {
            let mut nearest_group = Vec::new();
            for i in 0..ef {
                nearest_group.push(nearest_gpu[group * gpu_nearest_heap.ef + i]);
            }
            sorted_output_gpu.extend(nearest_group);
        }

        assert_eq!(scores_output, scores_output_cpu);
        assert_eq!(sorted_output_gpu, sorted_output_cpu);
    }
}
