use std::sync::Arc;

use crate::{GpuError, GpuResult};
use super::buffer::CudaBuffer;
use super::device::CudaDevice;
use super::driver::Handle;
use super::pipeline::CudaPipeline;
use super::descriptor_set::CudaDescriptorSet;

/// Timeout to wait for GPU execution in drop function (same as Vulkan Context).
static DROP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30 * 60);

/// CUDA/HIP execution context — records and dispatches compute kernels.
///
/// One stream per context; pipeline binding and dispatch are synchronous
/// from the CPU's perspective (stream is flushed in `run()`/`wait_finish()`).
pub struct CudaContext {
    device: Arc<CudaDevice>,

    /// CUDA/HIP stream handle.
    stream: Handle,

    /// Pending pipeline + descriptor sets for the next dispatch.
    pending_pipeline: Option<Arc<CudaPipeline>>,
    pending_params: Vec<u8>,

    /// Resources to keep alive until execution completes.
    _resources: Vec<Arc<dyn crate::Resource>>,
}

impl CudaContext {
    pub fn new(device: Arc<CudaDevice>) -> GpuResult<Self> {
        device.make_current()?;

        let mut stream: Handle = 0;
        crate::cuda::driver::GpuDriver::check(
            unsafe { (device.driver().stream_create)(&mut stream) },
            "stream_create",
        )?;

        Ok(CudaContext {
            device,
            stream,
            pending_pipeline: None,
            pending_params: Vec::new(),
            _resources: Vec::new(),
        })
    }

    /// Bind a pipeline and descriptor sets, building the GlobalParams byte buffer.
    pub fn bind_pipeline(
        &mut self,
        pipeline: Arc<CudaPipeline>,
        descriptor_sets: &[Arc<CudaDescriptorSet>],
    ) -> GpuResult<()> {
        // Build the GlobalParams byte buffer from the descriptor sets.
        // The order follows the binding indices stored in the pipeline's param_order.
        let param_order = pipeline.param_order();
        let mut params = vec![0u8; param_order.len() * 8];
        for (slot, &(set_idx, binding)) in param_order.iter().enumerate() {
            let set = descriptor_sets.get(set_idx).ok_or_else(|| {
                GpuError::Other(format!("Descriptor set index {set_idx} out of range"))
            })?;
            let addr = set.device_address(binding)?;
            log::trace!(
                "bind_pipeline: slot {slot} → set[{set_idx}].binding({binding}) = 0x{addr:016x}"
            );
            params[slot * 8..slot * 8 + 8].copy_from_slice(&addr.to_le_bytes());
        }
        self.pending_pipeline = Some(pipeline);
        self.pending_params = params;
        Ok(())
    }

    /// Dispatch a compute kernel.
    pub fn dispatch(&mut self, x: usize, _y: usize, _z: usize) -> GpuResult<()> {
        let pipeline = self
            .pending_pipeline
            .clone()
            .ok_or_else(|| GpuError::Other("No pipeline bound for dispatch".to_string()))?;

        self.device.make_current()?;

        // Upload GlobalParams to constant memory.
        let mut global_ptr: Handle = 0;
        let mut global_size: usize = 0;
        crate::cuda::driver::GpuDriver::check(
            unsafe {
                (self.device.driver().module_get_global)(
                    &mut global_ptr,
                    &mut global_size,
                    pipeline.module(),
                    c"SLANG_globalParams".as_ptr() as *const i8,
                )
            },
            "module_get_global(SLANG_globalParams)",
        )?;

        let params = &self.pending_params;
        if global_size != params.len() {
            return Err(GpuError::Other(format!(
                "GlobalParams size mismatch: kernel expects {global_size} bytes, \
                 descriptor sets provide {} bytes",
                params.len()
            )));
        }

        self.device.driver().memcpy_htod(
            global_ptr,
            params.as_ptr() as *const std::ffi::c_void,
            global_size,
        )?;

        // Launch the kernel: x work-groups, block size = subgroup_size (set by shader define).
        let block_x = self.device.subgroup_size() as u32;
        crate::cuda::driver::GpuDriver::check(
            unsafe {
                (self.device.driver().launch_kernel)(
                    pipeline.function(),
                    x as u32, 1, 1,
                    block_x, 1, 1,
                    0,
                    self.stream,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                )
            },
            "launch_kernel",
        )?;
        Ok(())
    }

    /// Copy between two CUDA buffers (device-to-device).
    pub fn copy_gpu_buffer(
        &mut self,
        src: Arc<CudaBuffer>,
        dst: Arc<CudaBuffer>,
        src_offset: usize,
        dst_offset: usize,
        size: usize,
    ) -> GpuResult<()> {
        if size == 0 {
            return Ok(());
        }
        if src.size() < src_offset + size || dst.size() < dst_offset + size {
            return Err(GpuError::OutOfBounds("Buffer copy out of bounds".to_string()));
        }
        self.device.make_current()?;
        // For CUDA d2d copy we use a host bounce (we don't load cuMemcpyDtoD separately).
        // This is only used for staging buffers so perf is acceptable.
        let mut tmp = vec![0u8; size];
        self.device.driver().memcpy_dtoh(
            tmp.as_mut_ptr() as *mut std::ffi::c_void,
            src.device_address() + src_offset as u64,
            size,
        )?;
        self.device.driver().memcpy_htod(
            dst.device_address() + dst_offset as u64,
            tmp.as_ptr() as *const std::ffi::c_void,
            size,
        )
    }

    /// Clear a buffer to zero.
    pub fn clear_buffer(&mut self, buffer: Arc<CudaBuffer>) -> GpuResult<()> {
        if buffer.size() == 0 {
            return Ok(());
        }
        self.device.make_current()?;
        self.device.driver().memset(buffer.device_address(), 0, buffer.size())
    }

    /// Barrier is a no-op for CUDA/HIP (single stream, sequential execution).
    pub fn barrier_buffers(&mut self, _buffers: &[Arc<CudaBuffer>]) -> GpuResult<()> {
        Ok(())
    }

    /// Submit the pending work to the stream (no-op in synchronous model).
    pub fn run(&mut self) -> GpuResult<()> {
        // With synchronous launches, nothing additional to do.
        Ok(())
    }

    /// Wait for all previously submitted work to complete.
    pub fn wait_finish(&mut self, _timeout: std::time::Duration) -> GpuResult<()> {
        self.device.make_current()?;
        crate::cuda::driver::GpuDriver::check(
            unsafe { (self.device.driver().stream_synchronize)(self.stream) },
            "stream_synchronize",
        )?;
        Ok(())
    }
}

impl Drop for CudaContext {
    fn drop(&mut self) {
        let _ = self.wait_finish(DROP_TIMEOUT);
        if self.stream != 0 {
            let rc = unsafe { (self.device.driver().stream_destroy)(self.stream) };
            if rc != 0 {
                log::error!("stream_destroy failed (code {rc})");
            }
            self.stream = 0;
        }
    }
}

unsafe impl Send for CudaContext {}
