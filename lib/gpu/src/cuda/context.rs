use std::cell::Cell;
use std::sync::Arc;

use super::buffer::CudaBuffer;
use super::descriptor_set::CudaDescriptorSet;
use super::device::CudaDevice;
use super::driver::Handle;
use super::pipeline::CudaPipeline;
use crate::{GpuError, GpuResult};

thread_local! {
    /// Tracks which CUDA/HIP context handle is current on this thread.
    /// Avoids redundant cuCtxSetCurrent calls in the hot loop.
    static CURRENT_CTX: Cell<Handle> = const { Cell::new(0) };
}

/// Timeout to wait for GPU execution in drop function (same as Vulkan Context).
static DROP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30 * 60);

/// CUDA/HIP execution context — queues work onto a stream.
///
/// All operations (copies, memset, kernel launches) are enqueued onto the
/// stream asynchronously.  Only `wait_finish()` blocks the CPU.
pub struct CudaContext {
    device: Arc<CudaDevice>,

    /// CUDA/HIP stream handle.
    stream: Handle,

    /// Pending pipeline + descriptor sets for the next dispatch.
    pending_pipeline: Option<Arc<CudaPipeline>>,

    /// Pinned host memory for GlobalParams upload.
    ///
    /// `cuMemcpyHtoDAsync` from pageable (non-pinned) memory causes an
    /// implicit stream synchronization, defeating the purpose of async
    /// transfers.  By staging params in a pinned buffer, the upload is truly
    /// asynchronous and overlaps with prior GPU work on the stream.
    pinned_params: *mut u8,
    pinned_params_capacity: usize,
    pinned_params_len: usize,

    /// Resources to keep alive until execution completes.
    _resources: Vec<Arc<dyn crate::Resource>>,

    /// True after any async GPU operation (copy, memset, kernel launch).
    /// Reset to false after `stream_synchronize`.  Allows consecutive
    /// `wait_finish()` calls to be free when no GPU work was queued between
    /// them (e.g. after a no-op `run()`).
    needs_sync: bool,
}

impl CudaContext {
    /// Set the device context current on this thread, skipping the driver call
    /// if it's already current.  CudaContext is single-threaded, so the cached
    /// value remains valid as long as no other CUDA context is activated on
    /// the same thread.
    fn make_current_cached(&self) -> GpuResult<()> {
        let ctx = self.device.context;
        CURRENT_CTX.with(|c| {
            if c.get() != ctx {
                self.device.make_current()?;
                c.set(ctx);
            }
            Ok(())
        })
    }

    /// Initial pinned params buffer size (enough for most shaders).
    const INITIAL_PARAMS_CAPACITY: usize = 256;

    pub fn new(device: Arc<CudaDevice>) -> GpuResult<Self> {
        device.make_current()?;

        let mut stream: Handle = 0;
        crate::cuda::driver::GpuDriver::check(
            unsafe { (device.driver().stream_create)(&mut stream, 0) },
            "stream_create",
        )?;

        // Allocate pinned host memory for params upload.
        let mut pinned_ptr: *mut std::ffi::c_void = std::ptr::null_mut();
        crate::cuda::driver::GpuDriver::check(
            unsafe {
                (device.driver().mem_host_alloc)(
                    &mut pinned_ptr,
                    Self::INITIAL_PARAMS_CAPACITY,
                    0, // no flags needed — just pinned
                )
            },
            "mem_host_alloc (params)",
        )?;

        Ok(CudaContext {
            device,
            stream,
            pending_pipeline: None,
            pinned_params: pinned_ptr as *mut u8,
            pinned_params_capacity: Self::INITIAL_PARAMS_CAPACITY,
            pinned_params_len: 0,
            _resources: Vec::new(),
            needs_sync: false,
        })
    }

    /// Ensure the pinned params buffer is at least `required` bytes.
    fn ensure_pinned_params_capacity(&mut self, required: usize) -> GpuResult<()> {
        if required <= self.pinned_params_capacity {
            return Ok(());
        }
        // Free old buffer and allocate a larger one.
        if !self.pinned_params.is_null() {
            self.device
                .driver()
                .free_host(self.pinned_params as *mut std::ffi::c_void);
            self.pinned_params = std::ptr::null_mut();
            self.pinned_params_capacity = 0;
        }
        let new_cap = required.next_power_of_two();
        let mut ptr: *mut std::ffi::c_void = std::ptr::null_mut();
        crate::cuda::driver::GpuDriver::check(
            unsafe { (self.device.driver().mem_host_alloc)(&mut ptr, new_cap, 0) },
            "mem_host_alloc (params grow)",
        )?;
        self.pinned_params = ptr as *mut u8;
        self.pinned_params_capacity = new_cap;
        Ok(())
    }

    /// Bind a pipeline and descriptor sets, building the GlobalParams byte buffer.
    pub fn bind_pipeline(
        &mut self,
        pipeline: Arc<CudaPipeline>,
        descriptor_sets: &[Arc<CudaDescriptorSet>],
    ) -> GpuResult<()> {
        // Build the GlobalParams byte buffer from the descriptor sets.
        // The order follows the binding indices stored in the pipeline's param_order.
        // Written directly into pinned host memory so the subsequent
        // memcpy_htod_async in dispatch() is truly asynchronous.
        let param_order = pipeline.param_order();
        let required = param_order.len() * 8;
        self.ensure_pinned_params_capacity(required)?;
        self.pinned_params_len = required;

        // Zero the region first (pinned memory isn't auto-zeroed on reuse).
        unsafe {
            std::ptr::write_bytes(self.pinned_params, 0, required);
        }

        for (slot, &(set_idx, binding)) in param_order.iter().enumerate() {
            let set = descriptor_sets.get(set_idx).ok_or_else(|| {
                GpuError::Other(format!("Descriptor set index {set_idx} out of range"))
            })?;
            let addr = set.device_address(binding)?;
            log::trace!(
                "bind_pipeline: slot {slot} → set[{set_idx}].binding({binding}) = 0x{addr:016x}"
            );
            unsafe {
                std::ptr::copy_nonoverlapping(
                    addr.to_le_bytes().as_ptr(),
                    self.pinned_params.add(slot * 8),
                    8,
                );
            }
        }
        self.pending_pipeline = Some(pipeline);
        Ok(())
    }

    /// Dispatch a compute kernel.
    pub fn dispatch(&mut self, x: usize, _y: usize, _z: usize) -> GpuResult<()> {
        let pipeline = self
            .pending_pipeline
            .as_ref()
            .ok_or_else(|| GpuError::Other("No pipeline bound for dispatch".to_string()))?;

        self.make_current_cached()?;

        // Upload GlobalParams to constant memory (uses cached pointer from pipeline).
        let global_ptr = pipeline.global_params_ptr();
        let global_size = pipeline.global_params_size();

        if global_size != self.pinned_params_len {
            return Err(GpuError::Other(format!(
                "GlobalParams size mismatch: kernel expects {global_size} bytes, \
                 descriptor sets provide {} bytes",
                self.pinned_params_len
            )));
        }

        // Async upload from pinned host memory — truly asynchronous because
        // the source is pinned (no implicit stream synchronization).
        // pinned_params stays alive until the next bind_pipeline() call,
        // which won't happen before wait_finish().
        self.device.driver().memcpy_htod_async(
            global_ptr,
            self.pinned_params as *const std::ffi::c_void,
            global_size,
            self.stream,
        )?;

        // Launch the kernel: x work-groups, block size = subgroup_size (set by shader define).
        let block_x = self.device.subgroup_size() as u32;
        let function = pipeline.function();
        crate::cuda::driver::GpuDriver::check(
            unsafe {
                (self.device.driver().launch_kernel)(
                    function,
                    x as u32,
                    1,
                    1,
                    block_x,
                    1,
                    1,
                    0,
                    self.stream,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                )
            },
            "launch_kernel",
        )?;
        self.needs_sync = true;
        Ok(())
    }

    /// Copy between two CUDA buffers (async on stream).
    ///
    /// Uses the correct transfer direction based on buffer types:
    /// - staging (host) → device: `cuMemcpyHtoDAsync`
    /// - device → staging (host): `cuMemcpyDtoHAsync`
    /// - device → device: `cuMemcpyDtoDAsync`
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
            return Err(GpuError::OutOfBounds(
                "Buffer copy out of bounds".to_string(),
            ));
        }
        self.make_current_cached()?;
        self.needs_sync = true;

        let src_host = src.host_ptr();
        let dst_host = dst.host_ptr();
        let driver = self.device.driver();

        if !src_host.is_null() && dst_host.is_null() {
            // Staging → device: use H2D async.
            driver.memcpy_htod_async(
                dst.device_address() + dst_offset as u64,
                unsafe { src_host.add(src_offset) } as *const std::ffi::c_void,
                size,
                self.stream,
            )
        } else if src_host.is_null() && !dst_host.is_null() {
            // Device → staging: use D2H async.
            driver.memcpy_dtoh_async(
                unsafe { dst_host.add(dst_offset) } as *mut std::ffi::c_void,
                src.device_address() + src_offset as u64,
                size,
                self.stream,
            )
        } else {
            // Both device or both staging: use D2D async.
            driver.memcpy_dtod_async(
                dst.device_address() + dst_offset as u64,
                src.device_address() + src_offset as u64,
                size,
                self.stream,
            )
        }
    }

    /// Clear a buffer to zero (async on stream).
    pub fn clear_buffer(&mut self, buffer: Arc<CudaBuffer>) -> GpuResult<()> {
        if buffer.size() == 0 {
            return Ok(());
        }
        self.make_current_cached()?;
        self.needs_sync = true;
        self.device
            .driver()
            .memset_async(buffer.device_address(), 0, buffer.size(), self.stream)
    }

    /// Barrier is a no-op for CUDA/HIP (single stream, sequential execution).
    pub fn barrier_buffers(&mut self, _buffers: &[Arc<CudaBuffer>]) -> GpuResult<()> {
        Ok(())
    }

    /// Submit the pending work to the stream (no-op — work is already queued).
    pub fn run(&mut self) -> GpuResult<()> {
        Ok(())
    }

    /// Wait for all previously submitted work to complete.
    ///
    /// Optimisation: if no kernel has been dispatched since the last sync we
    /// skip the expensive `cuStreamSynchronize` entirely.  CUDA stream
    /// ordering guarantees that lightweight ops (copies, memsets) queued
    /// before a kernel launch will have completed by the time the *next*
    /// `wait_finish` (after the kernel) returns.  This eliminates the
    /// redundant first sync in the HNSW hot loop.
    ///
    /// When a sync IS needed, `cuStreamQuery` is tried first as a cheap
    /// non-blocking check — the kernel may already be done by the time the
    /// CPU reaches this call.
    pub fn wait_finish(&mut self, _timeout: std::time::Duration) -> GpuResult<()> {
        if !self.needs_sync {
            return Ok(());
        }
        self.make_current_cached()?;
        let rc = unsafe { (self.device.driver().stream_query)(self.stream) };
        if rc == 0 {
            // Stream is already idle.
            self.needs_sync = false;
            return Ok(());
        }
        // CUDA_ERROR_NOT_READY (600) / hipErrorNotReady (34) — work in progress.
        crate::cuda::driver::GpuDriver::check(
            unsafe { (self.device.driver().stream_synchronize)(self.stream) },
            "stream_synchronize",
        )?;
        self.needs_sync = false;
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
        if !self.pinned_params.is_null() {
            self.device
                .driver()
                .free_host(self.pinned_params as *mut std::ffi::c_void);
            self.pinned_params = std::ptr::null_mut();
        }
    }
}

unsafe impl Send for CudaContext {}
