use std::collections::HashMap;
use std::sync::Arc;

use super::shader_builder::ShaderBuilderParameters;

pub struct GpuCandidatesHeap {
    pub capacity: usize,
    pub device: Arc<gpu::Device>,
}

impl GpuCandidatesHeap {
    pub fn new(device: Arc<gpu::Device>, capacity: usize) -> gpu::GpuResult<Self> {
        let ceiled_capacity = capacity.div_ceil(device.subgroup_size()) * device.subgroup_size();
        Ok(Self {
            capacity: ceiled_capacity,
            device,
        })
    }
}

impl ShaderBuilderParameters for GpuCandidatesHeap {
    fn shader_includes(&self) -> HashMap<String, String> {
        HashMap::from([(
            "candidates_heap.comp".to_string(),
            include_str!("shaders/candidates_heap.comp").to_string(),
        )])
    }

    fn shader_defines(&self) -> HashMap<String, Option<String>> {
        let mut defines = HashMap::new();
        defines.insert(
            "CANDIDATES_HEAP_CAPACITY".to_owned(),
            Some(self.capacity.to_string()),
        );
        defines
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BinaryHeap;

    use common::types::{PointOffsetType, ScoredPointOffset};
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::*;
    use crate::index::hnsw_index::gpu::shader_builder::ShaderBuilder;
    use crate::index::hnsw_index::gpu::GPU_TIMEOUT;

    #[repr(C)]
    struct TestParams {
        input_counts: u32,
    }

    #[test]
    fn test_gpu_candidates_heap() {
        let _ = env_logger::builder()
            .is_test(true)
            .filter_level(log::LevelFilter::Trace)
            .try_init();

        let capacity = 128;
        let points_count = 128;
        let groups_count = 8;
        let inputs_count = points_count;

        let mut rng = StdRng::seed_from_u64(42);
        let inputs_data: Vec<ScoredPointOffset> = (0..inputs_count * groups_count)
            .map(|i| ScoredPointOffset {
                idx: i as PointOffsetType,
                score: rng.gen_range(-1.0..1.0),
            })
            .collect();

        let debug_messenger = gpu::PanicIfErrorMessenger {};
        let instance = gpu::Instance::new(Some(&debug_messenger), None, false).unwrap();
        let device = gpu::Device::new(instance.clone(), &instance.physical_devices()[0]).unwrap();

        let gpu_candidates_heap = GpuCandidatesHeap::new(device.clone(), capacity).unwrap();

        let shader = ShaderBuilder::new(device.clone())
            .with_shader_code(include_str!("shaders/tests/test_candidates_heap.comp"))
            .with_parameters(&gpu_candidates_heap)
            .build()
            .unwrap();

        let input_points_buffer = gpu::Buffer::new(
            device.clone(),
            "Input points buffer",
            gpu::BufferType::Storage,
            inputs_count * groups_count * std::mem::size_of::<ScoredPointOffset>(),
        )
        .unwrap();

        let upload_staging_buffer = gpu::Buffer::new(
            device.clone(),
            "Candidates heap upload staging buffer",
            gpu::BufferType::CpuToGpu,
            inputs_count * groups_count * std::mem::size_of::<ScoredPointOffset>(),
        )
        .unwrap();
        upload_staging_buffer.upload_slice(&inputs_data, 0).unwrap();

        let mut context = gpu::Context::new(device.clone()).unwrap();
        context
            .copy_gpu_buffer(
                upload_staging_buffer.clone(),
                input_points_buffer.clone(),
                0,
                0,
                input_points_buffer.size(),
            )
            .unwrap();
        context.run().unwrap();
        context.wait_finish(GPU_TIMEOUT).unwrap();

        let test_params_buffer = gpu::Buffer::new(
            device.clone(),
            "Test params buffer",
            gpu::BufferType::Uniform,
            std::mem::size_of::<TestParams>(),
        )
        .unwrap();
        upload_staging_buffer
            .upload(
                &TestParams {
                    input_counts: inputs_count as u32,
                },
                0,
            )
            .unwrap();
        context
            .copy_gpu_buffer(
                upload_staging_buffer,
                test_params_buffer.clone(),
                0,
                0,
                test_params_buffer.size(),
            )
            .unwrap();
        context.run().unwrap();
        context.wait_finish(GPU_TIMEOUT).unwrap();

        let scores_output_buffer = gpu::Buffer::new(
            device.clone(),
            "Scores output buffer",
            gpu::BufferType::Storage,
            inputs_count * groups_count * std::mem::size_of::<ScoredPointOffset>(),
        )
        .unwrap();

        let descriptor_set_layout = gpu::DescriptorSetLayout::builder()
            .add_uniform_buffer(0)
            .add_storage_buffer(1)
            .add_storage_buffer(2)
            .build(device.clone())
            .unwrap();

        let descriptor_set = gpu::DescriptorSet::builder(descriptor_set_layout.clone())
            .add_uniform_buffer(0, test_params_buffer.clone())
            .add_storage_buffer(1, input_points_buffer.clone())
            .add_storage_buffer(2, scores_output_buffer.clone())
            .build()
            .unwrap();

        let pipeline = gpu::Pipeline::builder()
            .add_descriptor_set_layout(0, descriptor_set_layout.clone())
            .add_shader(shader.clone())
            .build(device.clone())
            .unwrap();

        context
            .bind_pipeline(pipeline, &[descriptor_set.clone()])
            .unwrap();
        context.dispatch(groups_count, 1, 1).unwrap();
        context.run().unwrap();
        context.wait_finish(GPU_TIMEOUT).unwrap();

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

        let download_staging_buffer = gpu::Buffer::new(
            device.clone(),
            "Candidates heap download staging buffer",
            gpu::BufferType::GpuToCpu,
            scores_output_buffer.size(),
        )
        .unwrap();
        context
            .copy_gpu_buffer(
                scores_output_buffer.clone(),
                download_staging_buffer.clone(),
                0,
                0,
                scores_output_buffer.size(),
            )
            .unwrap();
        context.run().unwrap();
        context.wait_finish(GPU_TIMEOUT).unwrap();
        let mut scores_gpu = vec![ScoredPointOffset::default(); inputs_count * groups_count];
        download_staging_buffer
            .download_slice(&mut scores_gpu, 0)
            .unwrap();

        assert_eq!(scores_gpu, scores_cpu);
    }
}
