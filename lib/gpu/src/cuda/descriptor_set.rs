use std::collections::HashMap;
use std::sync::Arc;

use crate::{GpuError, GpuResult};
use super::buffer::CudaBuffer;
use super::descriptor_set_layout::CudaDescriptorSetLayout;

/// A collection of buffers bound to specific shader binding slots.
///
/// In CUDA/HIP mode, there are no actual descriptor sets.
/// This struct just maps binding_index → device pointer.
#[derive(Clone)]
pub struct CudaDescriptorSet {
    _layout: Arc<CudaDescriptorSetLayout>,

    /// binding_index → Arc<CudaBuffer>
    buffers: HashMap<usize, Arc<CudaBuffer>>,
}

pub struct CudaDescriptorSetBuilder {
    layout: Arc<CudaDescriptorSetLayout>,
    uniform_buffers: Vec<(usize, Arc<CudaBuffer>)>,
    storage_buffers: Vec<(usize, Arc<CudaBuffer>)>,
}

impl CudaDescriptorSetBuilder {
    pub fn add_uniform_buffer(mut self, binding: usize, buffer: Arc<CudaBuffer>) -> Self {
        self.uniform_buffers.push((binding, buffer));
        self
    }

    pub fn add_storage_buffer(mut self, binding: usize, buffer: Arc<CudaBuffer>) -> Self {
        self.storage_buffers.push((binding, buffer));
        self
    }

    pub fn build(&self) -> GpuResult<Arc<CudaDescriptorSet>> {
        let mut buffers = HashMap::new();
        for (binding, buf) in self.uniform_buffers.iter().chain(self.storage_buffers.iter()) {
            buffers.insert(*binding, buf.clone());
        }
        Ok(Arc::new(CudaDescriptorSet {
            _layout: self.layout.clone(),
            buffers,
        }))
    }
}

impl CudaDescriptorSet {
    pub fn builder(layout: Arc<CudaDescriptorSetLayout>) -> CudaDescriptorSetBuilder {
        CudaDescriptorSetBuilder {
            layout,
            uniform_buffers: Vec::new(),
            storage_buffers: Vec::new(),
        }
    }

    /// Look up the device address for the buffer at `binding`.
    pub fn device_address(&self, binding: usize) -> GpuResult<u64> {
        self.buffers
            .get(&binding)
            .map(|b| b.device_address())
            .ok_or_else(|| {
                GpuError::Other(format!("No buffer at binding {binding} in descriptor set"))
            })
    }
}

impl crate::Resource for CudaDescriptorSet {}

unsafe impl Send for CudaDescriptorSet {}
unsafe impl Sync for CudaDescriptorSet {}
