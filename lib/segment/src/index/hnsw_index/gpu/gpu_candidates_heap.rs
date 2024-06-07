use std::sync::Arc;

use common::types::ScoredPointOffset;

use crate::common::operation_error::OperationResult;

#[repr(C)]
struct GpuCandidatesHeapParamsBuffer {
    capacity: u32,
}

pub struct GpuCandidatesHeap {
    pub capacity: usize,
    pub device: Arc<gpu::Device>,
    pub params_buffer: Arc<gpu::Buffer>,
    pub candidates_buffer: Arc<gpu::Buffer>,
    pub descriptor_set_layout: Arc<gpu::DescriptorSetLayout>,
    pub descriptor_set: Arc<gpu::DescriptorSet>,
}

impl GpuCandidatesHeap {
    pub fn new(device: Arc<gpu::Device>, groups_count: usize, capacity: usize) -> OperationResult<Self> {
        let ceiled_capacity = capacity.div_ceil(device.subgroup_size()) * device.subgroup_size();
        let buffers_elements_count = ceiled_capacity * groups_count;

        let candidates_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::Storage,
            buffers_elements_count * std::mem::size_of::<ScoredPointOffset>(),
        ));
        let params_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::Uniform,
            std::mem::size_of::<GpuCandidatesHeapParamsBuffer>(),
        ));

        let staging_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::CpuToGpu,
            std::mem::size_of::<GpuCandidatesHeapParamsBuffer>(),
        ));

        let params = GpuCandidatesHeapParamsBuffer {
            capacity: ceiled_capacity as u32,
        };
        staging_buffer.upload(&params, 0);

        let mut upload_context = gpu::Context::new(device.clone());
        upload_context.copy_gpu_buffer(
            staging_buffer.clone(),
            params_buffer.clone(),
            0,
            0,
            std::mem::size_of::<GpuCandidatesHeapParamsBuffer>(),
        );
        upload_context.run();
        upload_context.wait_finish();

        let descriptor_set_layout = gpu::DescriptorSetLayout::builder()
            .add_uniform_buffer(0)
            .add_storage_buffer(1)
            .build(device.clone());

        let descriptor_set = gpu::DescriptorSet::builder(descriptor_set_layout.clone())
            .add_uniform_buffer(0, params_buffer.clone())
            .add_storage_buffer(1, candidates_buffer.clone())
            .build();

        Ok(Self {
            capacity: ceiled_capacity,
            device,
            params_buffer,
            candidates_buffer,
            descriptor_set_layout,
            descriptor_set,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BinaryHeap;

    use common::types::PointOffsetType;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::*;

    #[repr(C)]
    struct TestParams {
        input_counts: u32,
    }

    #[test]
    fn test_gpu_candidates_heap() {
        let capacity = 128;
        let points_count = 128;
        let groups_count = 1;
        let inputs_count = points_count;

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

        let mut context = gpu::Context::new(device.clone());
        let gpu_candidates_heap = GpuCandidatesHeap::new(device.clone(), groups_count, capacity).unwrap();

        let shader = Arc::new(gpu::Shader::new(
            device.clone(),
            include_bytes!("./shaders/compiled/test_candidates_heap.spv"),
        ));

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
            upload_staging_buffer.clone(),
            input_points_buffer.clone(),
            0,
            0,
            input_points_buffer.size,
        );
        context.run();
        context.wait_finish();

        let test_params_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::Uniform,
            std::mem::size_of::<TestParams>(),
        ));
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

        let scores_output_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::Storage,
            inputs_count * groups_count * std::mem::size_of::<ScoredPointOffset>(),
        ));

        let descriptor_set_layout = gpu::DescriptorSetLayout::builder()
            .add_uniform_buffer(0)
            .add_storage_buffer(1)
            .add_storage_buffer(2)
            .build(device.clone());

        let descriptor_set = gpu::DescriptorSet::builder(descriptor_set_layout.clone())
            .add_uniform_buffer(0, test_params_buffer.clone())
            .add_storage_buffer(1, input_points_buffer.clone())
            .add_storage_buffer(2, scores_output_buffer.clone())
            .build();

        let pipeline = gpu::Pipeline::builder()
            .add_descriptor_set_layout(0, descriptor_set_layout.clone())
            .add_descriptor_set_layout(1, gpu_candidates_heap.descriptor_set_layout.clone())
            .add_shader(shader.clone())
            .build(device.clone());

        context.bind_pipeline(
            pipeline,
            &[
                descriptor_set.clone(),
                gpu_candidates_heap.descriptor_set.clone(),
            ],
        );
        context.dispatch(groups_count, 1, 1);
        context.run();
        context.wait_finish();

        let mut scores_cpu = vec![];
        for group in 0..groups_count {
            let mut heap = BinaryHeap::<ScoredPointOffset>::new();
            for i in 0..inputs_count {
                let scored_point = inputs_data[group * inputs_count + i];
                heap.push(scored_point);
            }
            while !heap.is_empty() {
                scores_cpu.push(heap.pop().unwrap());
            }
        }

        let download_staging_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::GpuToCpu,
            scores_output_buffer.size,
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
        let mut scores_gpu = vec![ScoredPointOffset::default(); inputs_count * groups_count];
        download_staging_buffer.download_slice(&mut scores_gpu, 0);

        for (i, (cpu, gpu)) in scores_cpu.iter().zip(scores_gpu.iter()).enumerate() {
            //println!("CAND {}: {}:{}, {}:{}", i, cpu.idx, cpu.score, gpu.idx, gpu.score);
            //println!("CAND {}: gpu: {}, cpu: {}", i%(inputs_count), gpu.score, cpu.score);
        }

        //assert_eq!(scores_gpu, scores_cpu);

        let download_staging_buffer = Arc::new(gpu::Buffer::new(
            device.clone(),
            gpu::BufferType::GpuToCpu,
            scores_output_buffer.size,
        ));
        context.copy_gpu_buffer(
            gpu_candidates_heap.candidates_buffer.clone(),
            download_staging_buffer.clone(),
            0,
            0,
            scores_output_buffer.size,
        );
        context.run();
        context.wait_finish();
        let mut scores_gpu = vec![ScoredPointOffset::default(); inputs_count * groups_count];
        download_staging_buffer.download_slice(&mut scores_gpu, 0);

        for (i, (cpu, gpu)) in scores_cpu.iter().zip(scores_gpu.iter()).enumerate() {
            //println!("CAND {}: {}:{}, {}:{}", i, cpu.idx, cpu.score, gpu.idx, gpu.score);
            println!("CAND {}: gpu: {}, cpu: {}", i%(inputs_count), gpu.score, cpu.score);
        }

        check(0, &scores_gpu);
    }

    fn check(node_index: usize, data: &[ScoredPointOffset]) {
        let nodes_count = data.len() / 32;
        if node_index >= nodes_count {
            return;
        }

        let node_data = read_node_data(node_index, data);
        for i in 0..31 {
            println!("CCC {}, {}, {}, {}", node_index * 32 + i, i, node_data[i].score, node_data[i + 1].score);
            assert!(node_data[i] > node_data[i + 1]);
        }

        let node_max = node_data.iter().max().unwrap();
        let node_min = node_data.iter().min().unwrap();

        let left_child = 2 * node_index + 1;
        if left_child < nodes_count {
            let left_data = read_node_data(left_child, data);
            let left_max = left_data.iter().max().unwrap();
            let left_min = left_data.iter().min().unwrap();

            //assert!(node_max > left_max);
            //assert!(node_min > left_max);
            //assert!(node_max > left_min);
            //assert!(node_min > left_min);

            check(left_child, data);
        }

        let right_child = 2 * node_index + 2;
        if right_child < nodes_count {
            let right_data = read_node_data(right_child, data);
            let right_max = right_data.iter().max().unwrap();
            let right_min = right_data.iter().min().unwrap();

            //assert!(node_max > right_max);
            //assert!(node_min > right_max);
            //assert!(node_max > right_min);
            //assert!(node_min > right_min);

            check(right_child, data);
        }
    }

    fn read_node_data(node_index: usize, data: &[ScoredPointOffset]) -> Vec<ScoredPointOffset> {
        let start = 32 * node_index;
        data[start..start+32].to_owned()
    }
}
