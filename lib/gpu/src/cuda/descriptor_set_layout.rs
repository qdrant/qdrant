use std::sync::Arc;

use crate::GpuResult;
use super::device::CudaDevice;

/// Describes which bindings (set_index, binding_index) map to which slot in GlobalParams.
///
/// In CUDA/HIP mode, there are no real descriptor sets or layouts.
/// We just track which (set, binding) pairs exist so the context can look up
/// device pointers when building the SLANG_globalParams byte buffer.
#[derive(Clone)]
pub struct CudaDescriptorSetLayout {
    device: Arc<CudaDevice>,

    /// Uniform buffer binding indices.
    uniform_buffer_bindings: Vec<usize>,

    /// Storage buffer binding indices.
    storage_buffer_bindings: Vec<usize>,
}

pub struct CudaDescriptorSetLayoutBuilder {
    uniform_buffer_bindings: Vec<usize>,
    storage_buffer_bindings: Vec<usize>,
}

impl CudaDescriptorSetLayoutBuilder {
    pub fn add_uniform_buffer(mut self, binding: usize) -> Self {
        self.uniform_buffer_bindings.push(binding);
        self
    }

    pub fn add_storage_buffer(mut self, binding: usize) -> Self {
        self.storage_buffer_bindings.push(binding);
        self
    }

    pub fn build(&self, device: Arc<CudaDevice>) -> GpuResult<Arc<CudaDescriptorSetLayout>> {
        Ok(Arc::new(CudaDescriptorSetLayout {
            device,
            uniform_buffer_bindings: self.uniform_buffer_bindings.clone(),
            storage_buffer_bindings: self.storage_buffer_bindings.clone(),
        }))
    }
}

impl CudaDescriptorSetLayout {
    pub fn builder() -> CudaDescriptorSetLayoutBuilder {
        CudaDescriptorSetLayoutBuilder {
            uniform_buffer_bindings: Vec::new(),
            storage_buffer_bindings: Vec::new(),
        }
    }

    pub fn device(&self) -> Arc<CudaDevice> {
        self.device.clone()
    }

    pub fn uniform_buffer_bindings(&self) -> &[usize] {
        &self.uniform_buffer_bindings
    }

    pub fn storage_buffer_bindings(&self) -> &[usize] {
        &self.storage_buffer_bindings
    }

    /// Collect all bindings in declaration order (uniforms first, then storage).
    pub fn all_bindings(&self) -> impl Iterator<Item = usize> + '_ {
        self.uniform_buffer_bindings
            .iter()
            .chain(self.storage_buffer_bindings.iter())
            .copied()
    }
}

unsafe impl Send for CudaDescriptorSetLayout {}
unsafe impl Sync for CudaDescriptorSetLayout {}
