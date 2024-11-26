pub mod gpu_vector_storage;
pub mod shader_builder;

/// Each GPU operation has a timeout by Vulkan API specification.
/// Choose large enough timeout.
/// We cannot use too small timeout and check stopper in the loop because
/// GPU resources should be alive while GPU operation is in progress.
static GPU_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
