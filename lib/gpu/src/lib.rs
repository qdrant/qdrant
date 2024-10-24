#![cfg(feature = "gpu")]

pub mod allocation_callbacks;
pub use allocation_callbacks::*;

pub mod context;
pub use context::*;

pub mod debug_messenger;
pub use debug_messenger::*;

pub mod descriptor_set;
pub use descriptor_set::*;

pub mod descriptor_set_layout;
pub use descriptor_set_layout::*;

pub mod buffer;
pub use buffer::*;

pub mod device;
pub use device::*;

pub mod instance;
pub use instance::*;

pub mod pipeline;
pub use pipeline::*;

pub mod shader;
use ash::vk;
pub use shader::*;

#[cfg(test)]
mod basic_test;

/// A trait for GPU resources.
/// It's used keep GPU resources alive while they are in use by the GPU context.
pub trait Resource: Send + Sync {}

#[derive(Debug)]
pub enum GpuError {
    /// Error during allocation. This can happen when the GPU runs out of memory.
    /// But also for RAM out of memory in case of CpuToGpu or GpuToCpu buffers allocations.
    OutOfMemory,

    /// Error during buffer access while mapping or command buffer recording.
    /// Warning. This error doesn't handle shader out of bounds access.
    OutOfBounds(String),

    /// Some of required hardware features are not supported by the GPU.
    NotSupported(String),

    /// A fence or query has not yet completed.
    NotReady,

    /// A wait operation has not completed in the specified time.
    Timeout,

    /// All others errors.
    Other(String),
}

pub type GpuResult<T> = Result<T, GpuError>;

impl From<gpu_allocator::AllocationError> for GpuError {
    fn from(error: gpu_allocator::AllocationError) -> GpuError {
        match error {
            gpu_allocator::AllocationError::OutOfMemory => GpuError::OutOfMemory,
            _ => GpuError::Other(format!("GPU allocator error: {:?}", error)),
        }
    }
}

impl From<vk::Result> for GpuError {
    fn from(result: vk::Result) -> Self {
        match result {
            vk::Result::NOT_READY => GpuError::NotReady,
            vk::Result::TIMEOUT => GpuError::Timeout,
            vk::Result::ERROR_OUT_OF_HOST_MEMORY => GpuError::OutOfMemory,
            vk::Result::ERROR_OUT_OF_DEVICE_MEMORY => GpuError::OutOfMemory,
            vk::Result::ERROR_LAYER_NOT_PRESENT => {
                GpuError::NotSupported("Layer is not present".to_string())
            }
            vk::Result::ERROR_EXTENSION_NOT_PRESENT => {
                GpuError::NotSupported("Extension is not present".to_string())
            }
            vk::Result::ERROR_FEATURE_NOT_PRESENT => {
                GpuError::NotSupported("Feature is not present".to_string())
            }
            vk::Result::ERROR_INCOMPATIBLE_DRIVER => {
                GpuError::NotSupported("Unable to find a Vulkan driver".to_string())
            }
            vk::Result::ERROR_FORMAT_NOT_SUPPORTED => {
                GpuError::NotSupported("Format is not supported".to_string())
            }
            _ => GpuError::Other(format!("Vulkan API error: {:?}", result)),
        }
    }
}
