use std::sync::Arc;

use super::device::CudaDevice;
use super::driver::Handle;
use super::shader::CudaShader;
use crate::GpuResult;
use crate::cuda::driver::GpuDriver;

/// A CUDA/HIP compute pipeline.
///
/// Holds the loaded module and kernel function handle, plus the GlobalParams
/// member ordering extracted from the compiled shader.
pub struct CudaPipeline {
    device: Arc<CudaDevice>,

    /// Loaded CUDA/HIP module.
    module: Handle,

    /// Kernel function handle (`main_0` by Slang's naming convention).
    function: Handle,

    /// The parameter layout: ordered (set_index, binding_index) pairs that
    /// map to consecutive 8-byte slots in SLANG_globalParams.
    param_order: Vec<(usize, usize)>,

    /// Cached device pointer and size of the `SLANG_globalParams` constant symbol.
    global_params_ptr: Handle,
    global_params_size: usize,

    /// Keep shader alive.
    _shader: Arc<CudaShader>,
}

pub struct CudaPipelineBuilder {
    shader: Option<Arc<CudaShader>>,
}

impl CudaPipelineBuilder {
    pub fn add_shader(mut self, shader: Arc<CudaShader>) -> Self {
        self.shader = Some(shader);
        self
    }

    pub fn build(&self, device: Arc<CudaDevice>) -> GpuResult<Arc<CudaPipeline>> {
        CudaPipeline::new(device, self)
    }
}

impl CudaPipeline {
    pub fn builder() -> CudaPipelineBuilder {
        CudaPipelineBuilder { shader: None }
    }

    fn new(device: Arc<CudaDevice>, builder: &CudaPipelineBuilder) -> GpuResult<Arc<Self>> {
        let shader = builder
            .shader
            .clone()
            .ok_or_else(|| crate::GpuError::Other("Pipeline requires a shader".to_string()))?;

        device.make_current()?;

        // Load the binary as a module.
        let mut module: Handle = 0;
        GpuDriver::check(
            unsafe {
                (device.driver().module_load_data)(
                    &mut module,
                    shader.binary().as_ptr() as *const std::ffi::c_void,
                )
            },
            "module_load_data",
        )?;

        // Slang names the entry point `main_0` in generated CUDA output.
        let mut function: Handle = 0;
        let kernel_name = c"main_0";
        let rc = unsafe {
            (device.driver().module_get_function)(
                &mut function,
                module,
                kernel_name.as_ptr() as *const i8,
            )
        };
        if rc != 0 {
            unsafe { (device.driver().module_unload)(module) };
            return Err(crate::GpuError::Other(format!(
                "module_get_function(main_0) failed (code {rc})"
            )));
        }

        // Cache the SLANG_globalParams constant symbol location.
        let mut global_params_ptr: Handle = 0;
        let mut global_params_size: usize = 0;
        GpuDriver::check(
            unsafe {
                (device.driver().module_get_global)(
                    &mut global_params_ptr,
                    &mut global_params_size,
                    module,
                    c"SLANG_globalParams".as_ptr() as *const i8,
                )
            },
            "module_get_global(SLANG_globalParams)",
        )?;

        Ok(Arc::new(CudaPipeline {
            device,
            module,
            function,
            param_order: shader.param_order().to_vec(),
            global_params_ptr,
            global_params_size,
            _shader: shader,
        }))
    }

    pub fn module(&self) -> Handle {
        self.module
    }

    pub fn function(&self) -> Handle {
        self.function
    }

    pub fn param_order(&self) -> &[(usize, usize)] {
        &self.param_order
    }

    pub fn global_params_ptr(&self) -> Handle {
        self.global_params_ptr
    }

    pub fn global_params_size(&self) -> usize {
        self.global_params_size
    }
}

impl Drop for CudaPipeline {
    fn drop(&mut self) {
        if self.module != 0 {
            let rc = unsafe { (self.device.driver().module_unload)(self.module) };
            if rc != 0 {
                log::error!("module_unload failed (code {rc})");
            }
            self.module = 0;
        }
    }
}

impl crate::Resource for CudaPipeline {}

unsafe impl Send for CudaPipeline {}
unsafe impl Sync for CudaPipeline {}
