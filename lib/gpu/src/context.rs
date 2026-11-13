use std::sync::Arc;

use ash::vk;

use crate::*;

/// Timeout to wait for GPU execution in drop function.
static DROP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30 * 60);

/// GPU execution context.
/// It records commands and run them on GPU.
/// It keeps track of resources used in the commands.
/// Warnings!
/// Context is not thread safe.
/// Execution order is not guaranteed. Don't rely on it.
/// If you need to run commands in specific order, use `wait_finish` method.
/// And start next command after previous one is finished.
pub struct Context {
    // Which device to execute on.
    device: Arc<Device>,

    // Which of the device's queues the context submits to.
    queue: ContextQueue,

    // GPU execution handler.
    vk_queue: vk::Queue,

    // Command buffer is created using command pool.
    vk_command_pool: vk::CommandPool,

    // Command buffer is used to record commands to execute.
    vk_command_buffer: vk::CommandBuffer,

    // Synchronization fence to wait for GPU execution.
    vk_fence: vk::Fence,

    // Resources used in the context.
    resources: Vec<Arc<dyn Resource>>,
}

/// Which device queue a `Context` records for and submits to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContextQueue {
    /// `Device::compute_queue`: shaders, copies, and clears.
    Compute,

    /// `Device::transfer_queue`: copies and clears only, so uploads and downloads can run while
    /// the compute queue works. Every buffer such a context touches must be created with
    /// `BufferQueues::ComputeAndTransfer`.
    ///
    /// The two queues aren't ordered with respect to each other. Before compute work reads what a
    /// transfer context wrote, wait for the transfer context (`wait_finish`), then `run` the
    /// compute context; in the other direction, wait for the compute context before a transfer
    /// context overwrites a buffer region that compute work still reads.
    Transfer,
}

impl Context {
    /// Context on the compute queue.
    pub fn new(device: Arc<Device>) -> GpuResult<Self> {
        Self::with_queue(device, ContextQueue::Compute)
    }

    /// Context on the given queue of `device`.
    pub fn with_queue(device: Arc<Device>, queue_kind: ContextQueue) -> GpuResult<Self> {
        // Get GPU execution queue from device.
        let queue = match queue_kind {
            ContextQueue::Compute => device.compute_queue(),
            ContextQueue::Transfer => device.transfer_queue(),
        };

        // Create command pool.
        let command_pool_create_info = vk::CommandPoolCreateInfo::default()
            .queue_family_index(queue.vk_queue_family_index as u32)
            .flags(vk::CommandPoolCreateFlags::default());
        let vk_command_pool = unsafe {
            device
                .vk_device()
                .create_command_pool(&command_pool_create_info, device.cpu_allocation_callbacks())?
        };

        // Create fence to wait for GPU execution.
        // We create fence as signaled because we reset fence before start.
        let fence_create_info =
            vk::FenceCreateInfo::default().flags(vk::FenceCreateFlags::SIGNALED);
        let vk_fence = unsafe {
            device
                .vk_device()
                .create_fence(&fence_create_info, device.cpu_allocation_callbacks())
        };
        let vk_fence = match vk_fence {
            Ok(fence) => fence,
            Err(e) => {
                // If fence creation failed, destroy created command pool and return error.
                unsafe {
                    device
                        .vk_device()
                        .destroy_command_pool(vk_command_pool, device.cpu_allocation_callbacks());
                }
                return Err(GpuError::from(e));
            }
        };

        let mut context = Self {
            queue: queue_kind,
            vk_queue: queue.vk_queue,
            device,
            vk_command_pool,
            vk_command_buffer: vk::CommandBuffer::null(),
            vk_fence,
            resources: Vec::new(),
        };

        context.init_command_buffer()?;

        Ok(context)
    }

    /// The queue this context submits to.
    pub fn queue(&self) -> ContextQueue {
        self.queue
    }

    /// Native handle of the queue this context submits to.
    #[cfg(test)]
    pub(crate) fn vk_queue(&self) -> vk::Queue {
        self.vk_queue
    }

    /// Keep `buffers` alive until the work recorded in this context has finished, as the context
    /// does for the buffers it binds or copies. For buffers that shaders reach only through their
    /// device address (`Buffer::device_address`): the context can't see those, and dropping one
    /// while a shader still uses its address lets the shader write into freed or reused memory.
    /// The context releases them in `wait_finish` once the work has finished. If the work never
    /// finishes, they are leaked together with the context's other resources.
    pub fn keep_alive(&mut self, buffers: &[Arc<Buffer>]) -> GpuResult<()> {
        if self.vk_command_buffer == vk::CommandBuffer::null() {
            self.init_command_buffer()?;
        }

        self.resources.extend(
            buffers
                .iter()
                .map(|buffer| buffer.clone() as Arc<dyn Resource>),
        );
        Ok(())
    }

    pub fn dispatch(&mut self, x: usize, y: usize, z: usize) -> GpuResult<()> {
        self.check_compute_queue("Dispatch")?;

        if self.vk_command_buffer == vk::CommandBuffer::null() {
            self.init_command_buffer()?;
        }

        let max_compute_work_group_count = self.device.max_compute_work_group_count();
        if x > max_compute_work_group_count[0]
            || y > max_compute_work_group_count[1]
            || z > max_compute_work_group_count[2]
        {
            return Err(GpuError::OutOfBounds(
                "Dispatch work group size is out of bounds".to_string(),
            ));
        }

        unsafe {
            self.device.vk_device().cmd_dispatch(
                self.vk_command_buffer,
                x as u32,
                y as u32,
                z as u32,
            );
        }
        Ok(())
    }

    /// Barrier for buffers.
    /// It's used to synchronize access to buffers between different shaders.
    /// By Vulkan specification, resources between different shaders/transfers must be synchronized.
    /// Example of compute shader sync:
    /// https://github.com/KhronosGroup/Vulkan-Docs/wiki/Synchronization-Examples#compute-to-compute-dependencies
    /// In practice, we use `context.wait_finish()` to synchronize between different shaders.
    /// But, for additional safety, we have this `barrier_buffers` to synchronize access to buffers.
    pub fn barrier_buffers(&mut self, buffers: &[Arc<Buffer>]) -> GpuResult<()> {
        self.check_compute_queue("Buffer barrier")?;

        if self.vk_command_buffer == vk::CommandBuffer::null() {
            self.init_command_buffer()?;
        }

        let buffer_memory_barriers = buffers
            .iter()
            .map(|buffer| {
                self.resources.push(buffer.clone() as Arc<dyn Resource>);
                // No queue family ownership transfer: equal families for an exclusive buffer.
                // For a buffer shared by several families, Vulkan without `synchronization2`
                // requires `VK_QUEUE_FAMILY_IGNORED`.
                let queue_family_index = if buffer.vk_sharing_mode() == vk::SharingMode::CONCURRENT
                {
                    vk::QUEUE_FAMILY_IGNORED
                } else {
                    self.device.compute_queue().vk_queue_family_index as u32
                };
                vk::BufferMemoryBarrier::default()
                    .buffer(buffer.vk_buffer())
                    .offset(0)
                    .size(buffer.size() as vk::DeviceSize)
                    .src_queue_family_index(queue_family_index)
                    .dst_queue_family_index(queue_family_index)
                    .src_access_mask(vk::AccessFlags::SHADER_WRITE)
                    .dst_access_mask(
                        vk::AccessFlags::TRANSFER_READ
                            | vk::AccessFlags::SHADER_READ
                            | vk::AccessFlags::SHADER_WRITE,
                    )
            })
            .collect::<Vec<_>>();

        unsafe {
            self.device.vk_device().cmd_pipeline_barrier(
                self.vk_command_buffer,
                vk::PipelineStageFlags::COMPUTE_SHADER,
                vk::PipelineStageFlags::ALL_COMMANDS,
                vk::DependencyFlags::empty(),
                &[],
                &buffer_memory_barriers,
                &[],
            );
        }

        Ok(())
    }

    /// Bind pipeline to the context.
    /// It means which shader and binded resources to shader will be used.
    /// It records command to run it on GPU after `run` call.
    pub fn bind_pipeline(
        &mut self,
        pipeline: Arc<Pipeline>,
        descriptor_sets: &[Arc<DescriptorSet>],
    ) -> GpuResult<()> {
        self.check_compute_queue("Pipeline binding")?;

        if self.vk_command_buffer == vk::CommandBuffer::null() {
            self.init_command_buffer()?;
        }

        unsafe {
            self.device.vk_device().cmd_bind_pipeline(
                self.vk_command_buffer,
                vk::PipelineBindPoint::COMPUTE,
                pipeline.vk_pipeline(),
            );
        }

        unsafe {
            if !descriptor_sets.is_empty() {
                let vk_descriptor_sets: Vec<_> = descriptor_sets
                    .iter()
                    .map(|set| set.as_ref().vk_descriptor_set())
                    .collect();
                self.device.vk_device().cmd_bind_descriptor_sets(
                    self.vk_command_buffer,
                    vk::PipelineBindPoint::COMPUTE,
                    pipeline.vk_pipeline_layout(),
                    0,
                    &vk_descriptor_sets,
                    &[],
                );
            }
        }

        // Add resources to the list to keep them alive.
        self.resources.extend(
            descriptor_sets
                .iter()
                .map(|r| r.clone() as Arc<dyn Resource>),
        );
        self.resources.push(pipeline);

        Ok(())
    }

    /// Copy data from one buffer to another. It records command to run it on GPU after `run` call.
    pub fn copy_gpu_buffer(
        &mut self,
        src: Arc<Buffer>,
        dst: Arc<Buffer>,
        src_offset: usize,
        dst_offset: usize,
        size: usize,
    ) -> GpuResult<()> {
        if size == 0 {
            return Ok(());
        }

        self.check_buffer_queues(&src)?;
        self.check_buffer_queues(&dst)?;

        if self.vk_command_buffer == vk::CommandBuffer::null() {
            self.init_command_buffer()?;
        }

        if src.size() < src_offset + size || dst.size() < dst_offset + size {
            return Err(GpuError::OutOfBounds(
                "Buffer copy out of bounds".to_string(),
            ));
        }

        let buffer_copy = vk::BufferCopy::default()
            .src_offset(src_offset as vk::DeviceSize)
            .dst_offset(dst_offset as vk::DeviceSize)
            .size(size as vk::DeviceSize);
        unsafe {
            self.device.vk_device().cmd_copy_buffer(
                self.vk_command_buffer,
                src.vk_buffer(),
                dst.vk_buffer(),
                &[buffer_copy],
            );
        }

        // Add resources to the list to keep them alive.
        self.resources.push(src);
        self.resources.push(dst);

        Ok(())
    }

    /// Clear buffer with zeros command. It records command to run it on GPU after `run` call.
    pub fn clear_buffer(&mut self, buffer: Arc<Buffer>) -> GpuResult<()> {
        if buffer.size() == 0 {
            return Ok(());
        }

        if !buffer.size().is_multiple_of(std::mem::size_of::<u32>()) {
            return Err(GpuError::OutOfBounds(
                "Buffer size must be a multiple of `uint32` size to clear it".to_string(),
            ));
        }

        self.check_buffer_queues(&buffer)?;

        if self.vk_command_buffer == vk::CommandBuffer::null() {
            self.init_command_buffer()?;
        }

        unsafe {
            self.device.vk_device().cmd_fill_buffer(
                self.vk_command_buffer,
                buffer.vk_buffer(),
                0,
                buffer.size() as vk::DeviceSize,
                0,
            );
        }

        // Add resources to the list to keep them alive.
        self.resources.push(buffer);

        Ok(())
    }

    /// Run the recorded commands on GPU.
    /// Warning: order of recorded commands is not guaranteed. Don't rely on it.
    pub fn run(&mut self) -> GpuResult<()> {
        if self.vk_command_buffer == vk::CommandBuffer::null() {
            // Nothing to run.
            return Ok(());
        }

        // Finish recording of command buffer.
        let end_record_result = unsafe {
            self.device
                .vk_device()
                .end_command_buffer(self.vk_command_buffer)
        };

        // If command buffer recording failed, destroy created command buffer and return error.
        if let Err(e) = end_record_result {
            self.destroy_command_buffer();
            return Err(GpuError::from(e));
        }

        // Reset fence to unsignaled state.
        let fence_reset_result = unsafe { self.device.vk_device().reset_fences(&[self.vk_fence]) };
        if let Err(e) = fence_reset_result {
            self.destroy_command_buffer();
            return Err(GpuError::from(e));
        }

        // Start execution of recorded commands.
        let submit_buffers = [self.vk_command_buffer];
        let submit_info = vec![vk::SubmitInfo::default().command_buffers(&submit_buffers)];
        let submit_result = {
            let _queue_submit_guard = self.device.lock_queue_submit();
            unsafe {
                self.device
                    .vk_device()
                    .queue_submit(self.vk_queue, &submit_info, self.vk_fence)
            }
        };

        if let Err(e) = submit_result {
            // If submit failed, destroy created command buffer and return error.
            // It's important here to avoid fence waiting of non-started command buffer.
            self.destroy_command_buffer();
            return Err(GpuError::from(e));
        }

        Ok(())
    }

    /// Wait for GPU execution to finish.
    pub fn wait_finish(&mut self, timeout: std::time::Duration) -> GpuResult<()> {
        if self.vk_command_buffer == vk::CommandBuffer::null() {
            // Nothing to wait for.
            return Ok(());
        }

        // Get the current status of fence.
        let fence_status = unsafe {
            self.device
                .vk_device()
                .get_fence_status(self.vk_fence)
                .map_err(GpuError::from)
        };

        match fence_status {
            Ok(true) => {
                // GPU execution finished already, clear command buffer and return.
                self.destroy_command_buffer();
                Ok(())
            }
            Ok(false) => {
                // GPU is processing. Wait for signal with timeout.
                let wait_result = unsafe {
                    self.device
                        .vk_device()
                        .wait_for_fences(&[self.vk_fence], true, timeout.as_nanos() as u64)
                        .map_err(GpuError::from)
                };

                if matches!(wait_result, Err(GpuError::Timeout)) {
                    // If we detect timeout, don't clear command buffer, just return a timeout error.
                    Err(GpuError::Timeout)
                } else {
                    // If the error is not a timeout, clear command buffer.
                    self.destroy_command_buffer();
                    wait_result
                }
            }
            Err(e) => {
                // By Vulkan specification, error while getting fence status
                // may happen is special cases like hardware device lost.
                // In this cases we don't care about status of gpu execution and just clear resources.
                self.destroy_command_buffer();
                Err(e)
            }
        }
    }

    /// Shaders and buffer barriers are recorded on the compute queue only.
    fn check_compute_queue(&self, operation: &str) -> GpuResult<()> {
        match self.queue {
            ContextQueue::Compute => Ok(()),
            ContextQueue::Transfer => Err(GpuError::NotSupported(format!(
                "{operation} is not available in a transfer context"
            ))),
        }
    }

    /// A transfer context only accepts buffers created for the transfer queue.
    fn check_buffer_queues(&self, buffer: &Buffer) -> GpuResult<()> {
        match (self.queue, buffer.queues()) {
            (ContextQueue::Compute, BufferQueues::Compute | BufferQueues::ComputeAndTransfer)
            | (ContextQueue::Transfer, BufferQueues::ComputeAndTransfer) => Ok(()),
            (ContextQueue::Transfer, BufferQueues::Compute) => Err(GpuError::NotSupported(
                "A transfer context only accepts buffers created with \
                 `BufferQueues::ComputeAndTransfer`"
                    .to_string(),
            )),
        }
    }

    fn init_command_buffer(&mut self) -> GpuResult<()> {
        if self.vk_command_buffer != vk::CommandBuffer::null() {
            return Err(GpuError::Other(
                "Vulkan command buffer was already created".to_string(),
            ));
        }

        // Create new command buffer from pool.
        let command_buffer_allocate_info = vk::CommandBufferAllocateInfo::default()
            .command_pool(self.vk_command_pool)
            .level(vk::CommandBufferLevel::PRIMARY)
            .command_buffer_count(1);
        self.vk_command_buffer = unsafe {
            self.device
                .vk_device()
                .allocate_command_buffers(&command_buffer_allocate_info)?[0]
        };

        let command_buffer_begin_info =
            vk::CommandBufferBeginInfo::default().flags(vk::CommandBufferUsageFlags::default());
        //.inheritance_info(..);

        let begin_result = unsafe {
            self.device
                .vk_device()
                .begin_command_buffer(self.vk_command_buffer, &command_buffer_begin_info)
        };

        // If command buffer creation failed, destroy created command buffer and return error.
        if let Err(e) = begin_result {
            self.destroy_command_buffer();
            return Err(GpuError::from(e));
        }

        Ok(())
    }

    fn destroy_command_buffer(&mut self) {
        if self.vk_command_buffer != vk::CommandBuffer::null() {
            unsafe {
                self.device
                    .vk_device()
                    .free_command_buffers(self.vk_command_pool, &[self.vk_command_buffer]);
            }
            self.vk_command_buffer = vk::CommandBuffer::null();
        }
        self.resources.clear();
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        let wait_result = self.wait_finish(DROP_TIMEOUT);
        match wait_result {
            Err(GpuError::Timeout) => {
                // Timeout reached, resources are still in use.
                // Vulkan API cannot stop GPU execution.
                // This sutiation may appear if shader has infinite loop, etc.
                // There is no good way to handle this error.
                // So just log it and ignore resources deallocation.
                // This approach may cause memory leaks and used gpu kernels,
                // but it's better than potential segfault.
                log::error!("Failed to wait for GPU context to finish");

                // Error was logged, do memory leak to keep the gpu running.
                let resources = self.resources.clone();
                self.resources.clear();
                for resource in resources {
                    // !!!!!!!!!
                    std::mem::forget(resource);
                }
            }
            // If there is no timeout, we can safely deallocate resources.
            wait_result => {
                wait_result.unwrap_or_else(|e|
                    // Cannot return error from Drop trait.
                    // Log it instead.
                    log::error!("Error while clear GPU context: {e:?}"));

                // If command buffer was not destroyed, destroy it.
                // This situation may appear if `wait_finish` is an error.
                self.destroy_command_buffer();

                // Destroy fence.
                if self.vk_fence != vk::Fence::null() {
                    unsafe {
                        self.device
                            .vk_device()
                            .destroy_fence(self.vk_fence, self.device.cpu_allocation_callbacks());
                    }
                    self.vk_fence = vk::Fence::null();
                }

                // Destroy command pool.
                if self.vk_command_pool != vk::CommandPool::null() {
                    unsafe {
                        self.device.vk_device().destroy_command_pool(
                            self.vk_command_pool,
                            self.device.cpu_allocation_callbacks(),
                        );
                    }
                    self.vk_command_pool = vk::CommandPool::null();
                }
            }
        }
    }
}
