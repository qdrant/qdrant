/// Unified CUDA/HIP driver API loaded dynamically at runtime.
///
/// CUDA: loaded from `libcuda.so.1` → functions named `cu*`
/// HIP:  loaded from `libamdhip64.so` → functions named `hip*`
///
/// Both APIs have the same calling convention and semantics; only the names differ.
use std::ffi::c_void;
use std::sync::Arc;

use crate::{GpuError, GpuResult};

// Opaque handle types — pointers stored as u64 to avoid lifetime complications.
pub type CuDevice = i32; // CUdevice is just an int
pub type Handle = u64; // CUcontext, CUmodule, CUfunction, CUstream, CUdeviceptr

/// One function pointer table that covers both CUDA and HIP driver APIs.
/// Each field is the raw function pointer loaded from the shared library.
pub struct GpuDriver {
    pub _lib: libloading::Library, // Keep library alive.

    // ---- device / init ----
    pub init: unsafe extern "C" fn(flags: u32) -> i32,
    pub device_count: unsafe extern "C" fn(count: *mut i32) -> i32,
    pub device_get: unsafe extern "C" fn(dev: *mut CuDevice, ordinal: i32) -> i32,
    pub device_get_name: unsafe extern "C" fn(name: *mut i8, len: i32, dev: CuDevice) -> i32,
    pub device_get_attribute: unsafe extern "C" fn(pi: *mut i32, attrib: i32, dev: CuDevice) -> i32,
    pub ctx_create: unsafe extern "C" fn(ctx: *mut Handle, flags: u32, dev: CuDevice) -> i32,
    pub ctx_destroy: unsafe extern "C" fn(ctx: Handle) -> i32,
    pub ctx_set_current: unsafe extern "C" fn(ctx: Handle) -> i32,

    // ---- memory ----
    pub mem_alloc: unsafe extern "C" fn(dptr: *mut Handle, size: usize) -> i32,
    pub mem_free: unsafe extern "C" fn(dptr: Handle) -> i32,
    pub memcpy_htod: unsafe extern "C" fn(dst: Handle, src: *const c_void, size: usize) -> i32,
    pub memcpy_dtoh: unsafe extern "C" fn(dst: *mut c_void, src: Handle, size: usize) -> i32,
    pub memset_d8: unsafe extern "C" fn(dst: Handle, value: u8, count: usize) -> i32,

    // ---- modules / kernels ----
    pub module_load_data: unsafe extern "C" fn(module: *mut Handle, image: *const c_void) -> i32,
    pub module_unload: unsafe extern "C" fn(module: Handle) -> i32,
    pub module_get_function:
        unsafe extern "C" fn(func: *mut Handle, module: Handle, name: *const i8) -> i32,
    pub module_get_global: unsafe extern "C" fn(
        dptr: *mut Handle,
        bytes: *mut usize,
        module: Handle,
        name: *const i8,
    ) -> i32,

    // ---- launch ----
    pub launch_kernel: unsafe extern "C" fn(
        func: Handle,
        grid_x: u32,
        grid_y: u32,
        grid_z: u32,
        block_x: u32,
        block_y: u32,
        block_z: u32,
        shared_mem_bytes: u32,
        stream: Handle,
        kernel_params: *mut *mut c_void,
        extra: *mut *mut c_void,
    ) -> i32,

    // ---- streams ----
    pub stream_create: unsafe extern "C" fn(stream: *mut Handle) -> i32,
    pub stream_synchronize: unsafe extern "C" fn(stream: Handle) -> i32,
    pub stream_destroy: unsafe extern "C" fn(stream: Handle) -> i32,
}

/// Which GPU runtime is in use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Runtime {
    Cuda,
    Hip,
}

impl GpuDriver {
    /// Try to load the HIP runtime.
    pub fn try_load_hip() -> GpuResult<(Arc<Self>, Runtime)> {
        #[cfg(target_os = "windows")]
        let candidates = &["amdhip64.dll", "amdhip64_7.dll", "amdhip64_6.dll"][..];
        #[cfg(not(target_os = "windows"))]
        let candidates = &["libamdhip64.so", "libamdhip64.so.6", "libamdhip64.so.5"][..];
        Self::load_from_candidates(candidates, Runtime::Hip)
    }

    /// Try to load the CUDA driver.
    pub fn try_load_cuda() -> GpuResult<(Arc<Self>, Runtime)> {
        #[cfg(target_os = "windows")]
        let candidates = &["nvcuda.dll"][..];
        #[cfg(not(target_os = "windows"))]
        let candidates = &["libcuda.so.1", "libcuda.so"][..];
        Self::load_from_candidates(candidates, Runtime::Cuda)
    }

    fn load_from_candidates(
        candidates: &[&str],
        runtime: Runtime,
    ) -> GpuResult<(Arc<Self>, Runtime)> {
        let mut last_err = format!("No candidates for {runtime:?}");
        for &name in candidates {
            match Self::load(name, runtime) {
                Ok(driver) => return Ok((Arc::new(driver), runtime)),
                Err(e) => last_err = format!("{e:?}"),
            }
        }
        Err(GpuError::NotSupported(format!(
            "{runtime:?} driver not found: {last_err}"
        )))
    }

    fn load(lib_name: &str, runtime: Runtime) -> GpuResult<Self> {
        let lib = unsafe {
            libloading::Library::new(lib_name)
                .map_err(|e| GpuError::NotSupported(format!("Cannot load {lib_name}: {e}")))?
        };

        // For CUDA: cu* functions; for HIP: hip* functions.
        // We unify them under the same function pointer table.
        macro_rules! load_fn {
            ($cuda_name:expr, $hip_name:expr) => {{
                let name: &[u8] = match runtime {
                    Runtime::Cuda => $cuda_name,
                    Runtime::Hip => $hip_name,
                };
                let sym = unsafe {
                    lib.get::<*mut c_void>(name).map_err(|e| {
                        GpuError::NotSupported(format!(
                            "Symbol {name:?} not found in {lib_name}: {e}"
                        ))
                    })?
                };
                unsafe { std::mem::transmute(*sym) }
            }};
        }

        let driver = Self {
            init: load_fn!(b"cuInit\0", b"hipInit\0"),
            device_count: load_fn!(b"cuDeviceGetCount\0", b"hipGetDeviceCount\0"),
            device_get: load_fn!(b"cuDeviceGet\0", b"hipDeviceGet\0"),
            device_get_name: load_fn!(b"cuDeviceGetName\0", b"hipDeviceGetName\0"),
            device_get_attribute: load_fn!(b"cuDeviceGetAttribute\0", b"hipDeviceGetAttribute\0"),
            ctx_create: load_fn!(b"cuCtxCreate_v2\0", b"hipCtxCreate\0"),
            ctx_destroy: load_fn!(b"cuCtxDestroy_v2\0", b"hipCtxDestroy\0"),
            ctx_set_current: load_fn!(b"cuCtxSetCurrent\0", b"hipCtxSetCurrent\0"),
            mem_alloc: load_fn!(b"cuMemAlloc_v2\0", b"hipMalloc\0"),
            mem_free: load_fn!(b"cuMemFree_v2\0", b"hipFree\0"),
            memcpy_htod: load_fn!(b"cuMemcpyHtoD_v2\0", b"hipMemcpyHtoD\0"),
            memcpy_dtoh: load_fn!(b"cuMemcpyDtoH_v2\0", b"hipMemcpyDtoH\0"),
            memset_d8: load_fn!(b"cuMemsetD8_v2\0", b"hipMemsetD8\0"),
            module_load_data: load_fn!(b"cuModuleLoadData\0", b"hipModuleLoadData\0"),
            module_unload: load_fn!(b"cuModuleUnload\0", b"hipModuleUnload\0"),
            module_get_function: load_fn!(b"cuModuleGetFunction\0", b"hipModuleGetFunction\0"),
            module_get_global: load_fn!(b"cuModuleGetGlobal_v2\0", b"hipModuleGetGlobal\0"),
            launch_kernel: load_fn!(b"cuLaunchKernel\0", b"hipModuleLaunchKernel\0"),
            stream_create: load_fn!(b"cuStreamCreate\0", b"hipStreamCreate\0"),
            stream_synchronize: load_fn!(b"cuStreamSynchronize\0", b"hipStreamSynchronize\0"),
            stream_destroy: load_fn!(b"cuStreamDestroy_v2\0", b"hipStreamDestroy\0"),
            _lib: lib,
        };

        // Initialize the runtime (cuInit / hipInit always takes flags=0).
        let rc = unsafe { (driver.init)(0) };
        if rc != 0 {
            return Err(GpuError::Other(format!(
                "{runtime:?} init failed with code {rc}"
            )));
        }

        Ok(driver)
    }

    /// Check a CUDA/HIP return code and convert non-zero to `GpuError`.
    pub fn check(rc: i32, op: &str) -> GpuResult<()> {
        if rc == 0 {
            Ok(())
        } else {
            Err(GpuError::Other(format!("{op} failed (error code {rc})")))
        }
    }

    /// Allocate device memory.
    pub fn alloc(&self, size: usize) -> GpuResult<Handle> {
        let mut ptr: Handle = 0;
        Self::check(unsafe { (self.mem_alloc)(&mut ptr, size) }, "mem_alloc")?;
        Ok(ptr)
    }

    /// Free device memory.
    pub fn free(&self, ptr: Handle) {
        if ptr != 0 {
            let rc = unsafe { (self.mem_free)(ptr) };
            if rc != 0 {
                log::error!("GPU free failed (error code {rc})");
            }
        }
    }

    /// Copy host → device (synchronous).
    pub fn memcpy_htod(&self, dst: Handle, src: *const c_void, size: usize) -> GpuResult<()> {
        Self::check(unsafe { (self.memcpy_htod)(dst, src, size) }, "memcpy_htod")
    }

    /// Copy device → host (synchronous).
    pub fn memcpy_dtoh(&self, dst: *mut c_void, src: Handle, size: usize) -> GpuResult<()> {
        Self::check(unsafe { (self.memcpy_dtoh)(dst, src, size) }, "memcpy_dtoh")
    }

    /// Set device memory bytes to `value`.
    pub fn memset(&self, dst: Handle, value: u8, count: usize) -> GpuResult<()> {
        Self::check(unsafe { (self.memset_d8)(dst, value, count) }, "memset")
    }

    // ---- CUDA (NVIDIA) device attribute indices ----
    // These match CUdevice_attribute enum values (stable across CUDA versions).
    pub const CUDA_ATTR_WARP_SIZE: i32 = 10; // CU_DEVICE_ATTRIBUTE_WARP_SIZE
    pub const CUDA_ATTR_MAX_GRID_DIM_X: i32 = 5; // CU_DEVICE_ATTRIBUTE_MAX_GRID_DIM_X

    // ---- HIP (AMD) device attribute indices ----
    // HIP re-numbers its enum between ROCm releases, so we cannot hardcode a single
    // value.  Instead we probe a set of known values and pick the one that succeeds
    // with a plausible result.
    //   ROCm 6.2: WarpSize=89, MaxGridDimX=31
    //   ROCm 7.1: WarpSize=90, MaxGridDimX=31
    const HIP_ATTR_WARP_SIZE_CANDIDATES: &'static [i32] = &[90, 89, 88, 87];
    const HIP_ATTR_MAX_GRID_DIM_X_CANDIDATES: &'static [i32] = &[31, 30, 29];

    /// Query warp/wavefront size for the given device.
    pub fn query_warp_size(&self, runtime: Runtime, dev: CuDevice) -> GpuResult<i32> {
        match runtime {
            Runtime::Cuda => self.get_attribute(Self::CUDA_ATTR_WARP_SIZE, dev),
            Runtime::Hip => {
                for &attr in Self::HIP_ATTR_WARP_SIZE_CANDIDATES {
                    if let Ok(val) = self.get_attribute(attr, dev) {
                        if val == 32 || val == 64 {
                            return Ok(val);
                        }
                    }
                }
                Err(GpuError::Other(
                    "Failed to query HIP warp size: no candidate attribute returned 32 or 64"
                        .to_string(),
                ))
            }
        }
    }

    /// Query max grid dim X for the given device.
    pub fn query_max_grid_dim_x(&self, runtime: Runtime, dev: CuDevice) -> GpuResult<i32> {
        match runtime {
            Runtime::Cuda => self.get_attribute(Self::CUDA_ATTR_MAX_GRID_DIM_X, dev),
            Runtime::Hip => {
                for &attr in Self::HIP_ATTR_MAX_GRID_DIM_X_CANDIDATES {
                    if let Ok(val) = self.get_attribute(attr, dev) {
                        // Max grid dim X should be a large positive number (typically 2^31-1).
                        if val > 1024 {
                            return Ok(val);
                        }
                    }
                }
                Err(GpuError::Other(
                    "Failed to query HIP max grid dim X".to_string(),
                ))
            }
        }
    }

    pub fn get_attribute(&self, attrib: i32, dev: CuDevice) -> GpuResult<i32> {
        let mut val: i32 = 0;
        Self::check(
            unsafe { (self.device_get_attribute)(&mut val, attrib, dev) },
            "device_get_attribute",
        )?;
        Ok(val)
    }
}

unsafe impl Send for GpuDriver {}
unsafe impl Sync for GpuDriver {}
