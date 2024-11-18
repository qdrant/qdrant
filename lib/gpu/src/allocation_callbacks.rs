use ash::vk;

/// AllocationCallbacks is a trait that provides access to Vulkan allocation callbacks.
/// It's used to provide custom memory allocation and deallocation functions on CPU side.
/// GPU memory allocation is managed by the `gpu-allocator` crate.
/// Even though Vulkan provides default allocation callbacks, it's helpful at least for debugging purposes.
pub trait AllocationCallbacks: Send + Sync + 'static {
    fn allocation_callbacks(&self) -> &vk::AllocationCallbacks;
}
