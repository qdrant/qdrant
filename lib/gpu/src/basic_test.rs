static SHADER_CODE: &str = "
#version 450

layout(set = 0, binding = 0) buffer Numbers {
    float data[];
} numbers;

layout(set = 0, binding = 1) uniform Param {
    float param;
} param;

void main() {
    uint index = gl_GlobalInvocationID.x;
    numbers.data[index] += param.param;
}
";

// Basic GPU test.
// It takes list of numbers and adds parameter to each number.
#[test]
fn basic_gpu_test() {
    // First step: initialize GPU device.

    // Panic if case of wrong Vulkan API calls.
    let debug_messenger = crate::PanicIfErrorMessenger {};
    // Use default CPU allocator.
    let allocation_callbacks = None;
    // Don't dump Vulkan API calls.
    let dump_api = false;
    // Create Vulkan API instance.
    let instance =
        crate::Instance::new(Some(&debug_messenger), allocation_callbacks, dump_api).unwrap();
    // Choose any GPU hardware to use.
    let physical_device = &instance.physical_devices()[0];
    // Create GPU device.
    let device = crate::Device::new(instance.clone(), physical_device).unwrap();

    // Create gpu context that records command to GPU and runs them.
    let mut context = crate::Context::new(device.clone()).unwrap();
    let context_timeout = std::time::Duration::from_secs(30);

    // Second step: create GPU resources.

    // Generate input numbers.
    let numbers_count = 512;
    let numbers = (0..numbers_count).map(|x| x as f32).collect::<Vec<_>>();
    let param = 5f32;

    // Create GPU buffers.
    // Storage buffer that contains numbers.
    let storage_buffer = crate::Buffer::new(
        device.clone(),
        "Storage buffer",
        crate::BufferType::Storage,
        numbers.len() * std::mem::size_of::<f32>(),
    )
    .unwrap();
    // Uniform buffer that contains parameter.
    let uniform_buffer = crate::Buffer::new(
        device.clone(),
        "Uniform buffer",
        crate::BufferType::Uniform,
        std::mem::size_of::<f32>(),
    )
    .unwrap();

    // Upload data to the GPU.
    // We cannot just call `memcpy` because GPU don't see RAM memory.
    // To upload data we need to create intermediate buffer
    // which is visible to both CPU and GPU.
    // We will copy data to this buffer and then copy it to the GPU buffer.
    // Use one buffer for both data and parameter.
    let upload_buffer = crate::Buffer::new(
        device.clone(),
        "Upload buffer",
        crate::BufferType::CpuToGpu, // Mark buffer as buffer to copy from CPU to GPU.
        storage_buffer.size() + uniform_buffer.size(),
    )
    .unwrap();
    // Copy numbers.
    upload_buffer.upload_slice(&numbers, 0).unwrap();
    // Copy parameter to the end.
    upload_buffer.upload(&param, storage_buffer.size()).unwrap();

    // Upload from intermediate buffer to GPU buffers.
    context
        .copy_gpu_buffer(
            upload_buffer.clone(),
            storage_buffer.clone(),
            0,
            0,
            storage_buffer.size(),
        )
        .unwrap();
    context
        .copy_gpu_buffer(
            upload_buffer.clone(),
            uniform_buffer.clone(),
            storage_buffer.size(),
            0,
            uniform_buffer.size(),
        )
        .unwrap();
    // run copy commands.
    context.run().unwrap();
    context.wait_finish(context_timeout).unwrap();

    // Third step: create computation pipeline.

    // Compile shader code to SPIR-V.
    let spirv = instance
        .compile_shader(SHADER_CODE, "shader.glsl", None, None)
        .unwrap();
    // Create shader.
    let shader = crate::Shader::new(device.clone(), &spirv).unwrap();

    // Create linking to the shader.
    let descriptor_set_layout = crate::DescriptorSetLayout::builder()
        .add_storage_buffer(0)
        .add_uniform_buffer(1)
        .build(device.clone())
        .unwrap();
    let descriptor_set = crate::DescriptorSet::builder(descriptor_set_layout.clone())
        .add_storage_buffer(0, storage_buffer.clone())
        .add_uniform_buffer(1, uniform_buffer.clone())
        .build()
        .unwrap();

    // Create computation pipeline.
    let pipeline = crate::Pipeline::builder()
        .add_descriptor_set_layout(0, descriptor_set_layout)
        .add_shader(shader)
        .build(device.clone())
        .unwrap();

    // Fourth step: run computation.
    let descriptor_sets = [descriptor_set.clone()];
    // Bind pipeline and descriptor sets.
    context.bind_pipeline(pipeline, &descriptor_sets).unwrap();
    // Run computeation command. Threads count is the inputs count.
    context.dispatch(numbers_count, 1, 1).unwrap();
    // Run GPU and wait finish.
    context.run().unwrap();
    context.wait_finish(context_timeout).unwrap();

    // Fifth step: download and check results.

    // Like upload, we need to create intermediate buffer to download data from GPU.
    let download_buffer = crate::Buffer::new(
        device.clone(),
        "Download buffer",
        crate::BufferType::GpuToCpu, // Mark buffer as buffer to copy from GPU to CPU.
        storage_buffer.size(),
    )
    .unwrap();
    // Copy data from GPU to intermediate buffer.
    context
        .copy_gpu_buffer(
            storage_buffer.clone(),
            download_buffer.clone(),
            0,
            0,
            storage_buffer.size(),
        )
        .unwrap();
    // Run copy command.
    context.run().unwrap();
    context.wait_finish(context_timeout).unwrap();

    // Download data from intermediate buffer.
    let mut result = vec![0f32; numbers_count];
    download_buffer.download_slice(&mut result, 0).unwrap();

    // Check results.
    for i in 0..numbers_count {
        assert_eq!(result[i], numbers[i] + param);
    }
}
