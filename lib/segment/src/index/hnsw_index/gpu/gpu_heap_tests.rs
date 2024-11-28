//! Unit tests for GPU heap shader.

use std::collections::{BinaryHeap, HashMap};

use common::fixed_length_priority_queue::FixedLengthPriorityQueue;
use common::types::{PointOffsetType, ScoredPointOffset};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rstest::rstest;

use crate::index::hnsw_index::gpu::shader_builder::{ShaderBuilder, ShaderBuilderParameters};
use crate::index::hnsw_index::gpu::GPU_TIMEOUT;

struct GpuHeapTestConfig {
    ef: usize,
    linear: bool,
}

impl ShaderBuilderParameters for GpuHeapTestConfig {
    fn shader_includes(&self) -> HashMap<String, String> {
        HashMap::from([
            (
                "shared_buffer.comp".to_string(),
                include_str!("shaders/shared_buffer.comp").to_string(),
            ),
            (
                "bheap.comp".to_string(),
                include_str!("shaders/bheap.comp").to_string(),
            ),
        ])
    }

    fn shader_defines(&self) -> HashMap<String, Option<String>> {
        let mut defines = HashMap::new();
        defines.insert("EF".to_owned(), Some(self.ef.to_string()));
        if self.linear {
            defines.insert("BHEAP_LINEAR".to_owned(), None);
        }
        defines
    }
}

#[repr(C)]
struct GpuHeapTestParams {
    input_counts: u32,
}

#[rstest]
fn test_gpu_nearest_heap(#[values(true, false)] linear: bool) {
    let _ = env_logger::builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Trace)
        .try_init();

    let ef = 100;
    let points_count = 1024;
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

    let gpu_nearest_heap = GpuHeapTestConfig { ef, linear };

    let shader = ShaderBuilder::new(device.clone())
        .with_shader_code(include_str!("shaders/tests/test_nearest_heap.comp"))
        .with_parameters(&gpu_nearest_heap)
        .build("tests/test_nearest_heap.comp")
        .unwrap();

    let input_points_buffer = gpu::Buffer::new(
        device.clone(),
        "Nearest heap input points buffer",
        gpu::BufferType::Storage,
        inputs_count * groups_count * std::mem::size_of::<ScoredPointOffset>(),
    )
    .unwrap();

    let upload_staging_buffer = gpu::Buffer::new(
        device.clone(),
        "Nearest heap upload staging buffer",
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
        "Nearest heap test params buffer",
        gpu::BufferType::Uniform,
        std::mem::size_of::<GpuHeapTestParams>(),
    )
    .unwrap();
    upload_staging_buffer
        .upload(
            &GpuHeapTestParams {
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
        "Nearest heap scores output buffer",
        gpu::BufferType::Storage,
        inputs_count * groups_count * std::mem::size_of::<f32>(),
    )
    .unwrap();
    let sorted_output_buffer = gpu::Buffer::new(
        device.clone(),
        "Nearest heap sorted output buffer",
        gpu::BufferType::Storage,
        ef * groups_count * std::mem::size_of::<PointOffsetType>(),
    )
    .unwrap();

    let descriptor_set_layout = gpu::DescriptorSetLayout::builder()
        .add_uniform_buffer(0)
        .add_storage_buffer(1)
        .add_storage_buffer(2)
        .add_storage_buffer(3)
        .build(device.clone())
        .unwrap();

    let descriptor_set = gpu::DescriptorSet::builder(descriptor_set_layout.clone())
        .add_uniform_buffer(0, test_params_buffer.clone())
        .add_storage_buffer(1, input_points_buffer.clone())
        .add_storage_buffer(2, scores_output_buffer.clone())
        .add_storage_buffer(3, sorted_output_buffer.clone())
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

    let download_staging_buffer = gpu::Buffer::new(
        device.clone(),
        "Nearest heap download staging buffer",
        gpu::BufferType::GpuToCpu,
        std::cmp::max(scores_output_buffer.size(), sorted_output_buffer.size()),
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
    let mut scores_output = vec![0.0; inputs_count * groups_count];
    download_staging_buffer
        .download_slice(&mut scores_output, 0)
        .unwrap();

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
    context
        .copy_gpu_buffer(
            sorted_output_buffer.clone(),
            download_staging_buffer.clone(),
            0,
            0,
            nearest_gpu.len() * std::mem::size_of::<PointOffsetType>(),
        )
        .unwrap();
    context.run().unwrap();
    context.wait_finish(GPU_TIMEOUT).unwrap();
    download_staging_buffer
        .download_slice(nearest_gpu.as_mut_slice(), 0)
        .unwrap();

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

#[rstest]
fn test_gpu_candidates_heap(#[values(true, false)] linear: bool) {
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

    let gpu_candidates_heap = GpuHeapTestConfig {
        ef: capacity,
        linear,
    };

    let shader = ShaderBuilder::new(device.clone())
        .with_shader_code(include_str!("shaders/tests/test_candidates_heap.comp"))
        .with_parameters(&gpu_candidates_heap)
        .build("tests/test_candidates_heap.comp")
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
        std::mem::size_of::<GpuHeapTestParams>(),
    )
    .unwrap();
    upload_staging_buffer
        .upload(
            &GpuHeapTestParams {
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
