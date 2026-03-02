use std::ffi::CStr;
use std::sync::Arc;

use super::driver::{CuDevice, GpuDriver, Handle, Runtime};
use crate::GpuResult;

/// A CUDA or HIP compute device.
pub struct CudaDevice {
    pub(super) driver: Arc<GpuDriver>,
    pub(super) runtime: Runtime,

    /// Index in the driver's device list.
    pub(super) device_index: i32,

    /// Raw CUdevice / hipDevice_t (same type: i32).
    pub(super) cu_device: CuDevice,

    /// Primary context for this device.
    pub(super) context: Handle,

    /// Warp/wavefront size queried from device properties.
    subgroup_size: usize,

    /// Maximum number of work-groups (blocks) in the X dimension.
    max_work_groups: usize,

    /// Maximum total device memory (bytes). Used as a proxy for max buffer size.
    max_buffer_size: usize,

    /// Whether the device supports f16 arithmetic.
    has_half_precision: bool,

    /// Hardware name.
    name: String,

    /// AMD GPU target string (e.g. "gfx90a") — only set for HIP.
    pub(super) amdgpu_target: Option<String>,
}

impl CudaDevice {
    /// Enumerate devices using the supplied driver and return one device per device found.
    pub fn enumerate(driver: Arc<GpuDriver>, runtime: Runtime) -> GpuResult<Vec<Arc<CudaDevice>>> {
        let mut count: i32 = 0;
        GpuDriver::check(unsafe { (driver.device_count)(&mut count) }, "device_count")?;

        let mut devices = Vec::new();
        for i in 0..count {
            match CudaDevice::new(driver.clone(), runtime, i) {
                Ok(dev) => devices.push(dev),
                Err(e) => log::warn!("Skipping {runtime:?} device {i}: {e:?}"),
            }
        }
        Ok(devices)
    }

    fn new(driver: Arc<GpuDriver>, runtime: Runtime, device_index: i32) -> GpuResult<Arc<Self>> {
        let mut cu_device: CuDevice = 0;
        GpuDriver::check(
            unsafe { (driver.device_get)(&mut cu_device, device_index) },
            "device_get",
        )?;

        // Get device name.
        let mut name_buf = vec![0i8; 256];
        GpuDriver::check(
            unsafe { (driver.device_get_name)(name_buf.as_mut_ptr(), 256, cu_device) },
            "device_get_name",
        )?;
        let name = unsafe { CStr::from_ptr(name_buf.as_ptr()) }
            .to_string_lossy()
            .into_owned();

        // Query warp/wavefront size. For HIP, the attribute enum changes between
        // ROCm releases, so we probe multiple candidates.
        let subgroup_size = driver.query_warp_size(runtime, cu_device)? as usize;

        // Max grid X — same probing approach for HIP.
        let max_work_groups = driver.query_max_grid_dim_x(runtime, cu_device)? as usize;

        // All modern AMD Instinct and NVIDIA GPUs support f16.
        let has_half_precision = true;

        // Max buffer size: use 4 GiB as a conservative default.
        // BDA pointers are u64, so we're not actually limited by this.
        let max_buffer_size = 4 * 1024 * 1024 * 1024;

        // Create a primary context for this device.
        let mut ctx: Handle = 0;
        GpuDriver::check(
            unsafe { (driver.ctx_create)(&mut ctx, 0, cu_device) },
            "ctx_create",
        )?;

        log::info!(
            "Found {runtime:?} device [{device_index}]: {name} \
             (subgroup_size={subgroup_size})"
        );

        Ok(Arc::new(CudaDevice {
            driver,
            runtime,
            device_index,
            cu_device,
            context: ctx,
            subgroup_size,
            max_work_groups,
            max_buffer_size,
            has_half_precision,
            name,
            amdgpu_target: None,
        }))
    }

    /// Create a `CudaDevice` from a `CudaPhysicalDevice` (from instance enumeration).
    pub fn from_physical(pd: &crate::cuda::CudaPhysicalDevice) -> crate::GpuResult<Arc<Self>> {
        Self::new(pd.driver.clone(), pd.runtime, pd.device_index)
    }

    /// Make this device's context current on the calling thread.
    pub fn make_current(&self) -> GpuResult<()> {
        GpuDriver::check(
            unsafe { (self.driver.ctx_set_current)(self.context) },
            "ctx_set_current",
        )
    }

    pub fn subgroup_size(&self) -> usize {
        self.subgroup_size
    }

    pub fn max_buffer_size(&self) -> usize {
        self.max_buffer_size
    }

    pub fn has_half_precision(&self) -> bool {
        self.has_half_precision
    }

    /// CUDA/HIP always supports BDA (pointers are native in device memory).
    pub fn has_buffer_device_address(&self) -> bool {
        true
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn max_compute_work_group_count(&self) -> [usize; 3] {
        [self.max_work_groups, 65535, 65535]
    }

    pub fn runtime(&self) -> Runtime {
        self.runtime
    }

    pub fn driver(&self) -> &Arc<GpuDriver> {
        &self.driver
    }
}

impl Drop for CudaDevice {
    fn drop(&mut self) {
        if self.context != 0 {
            let rc = unsafe { (self.driver.ctx_destroy)(self.context) };
            if rc != 0 {
                log::error!("ctx_destroy failed (code {rc})");
            }
            self.context = 0;
        }
    }
}

unsafe impl Send for CudaDevice {}
unsafe impl Sync for CudaDevice {}
