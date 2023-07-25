use std::collections::BTreeMap;
use std::sync::Arc;

use crate::*;

#[derive(Default)]
pub struct PipelineBuilder {
    pub(crate) shader: Option<Arc<Shader>>,
    pub(crate) descriptor_set_layouts: BTreeMap<usize, Arc<DescriptorSetLayout>>,
}

impl PipelineBuilder {
    pub fn add_shader(mut self, shader: Arc<Shader>) -> Self {
        self.shader = Some(shader);
        self
    }

    pub fn add_descriptor_set_layout(
        mut self,
        set: usize,
        descriptor_set_layout: Arc<DescriptorSetLayout>,
    ) -> Self {
        self.descriptor_set_layouts
            .insert(set, descriptor_set_layout);
        self
    }

    pub fn build(&self, device: Arc<Device>) -> Arc<Pipeline> {
        Arc::new(Pipeline::new(device, self))
    }
}
