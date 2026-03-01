use std::sync::Arc;

use super::device::CudaDevice;
use crate::GpuResult;

/// A compiled CUDA/HIP shader (binary blob: HIP .co or CUDA PTX).
pub struct CudaShader {
    #[allow(dead_code)]
    device: Arc<CudaDevice>,

    /// Raw binary loaded from disk (HIP code object or CUDA PTX).
    pub(super) binary: Vec<u8>,

    /// Parameter layout: [(set_index, binding_index)] in GlobalParams_0 member order.
    pub(super) param_order: Vec<(usize, usize)>,
}

impl CudaShader {
    pub fn new(
        device: Arc<CudaDevice>,
        binary: Vec<u8>,
        param_order: Vec<(usize, usize)>,
    ) -> GpuResult<Arc<Self>> {
        Ok(Arc::new(CudaShader {
            device,
            binary,
            param_order,
        }))
    }

    pub fn binary(&self) -> &[u8] {
        &self.binary
    }

    pub fn param_order(&self) -> &[(usize, usize)] {
        &self.param_order
    }
}

impl crate::Resource for CudaShader {}

unsafe impl Send for CudaShader {}
unsafe impl Sync for CudaShader {}
