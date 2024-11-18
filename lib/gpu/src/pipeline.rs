use std::collections::HashMap;
use std::sync::Arc;

use ash::vk;

use crate::*;

static SHADER_ENTRY_POINT: &std::ffi::CStr = c"main";

/// Pipeline is an abstraction over a Vulkan compute pipeline.
/// Pipeline is a GPU resource that defines how a shader should be executed on the GPU.
/// For compute pipelines it's a single shader with binded resources.
pub struct Pipeline {
    // Device that owns the pipeline.
    device: Arc<Device>,

    // Shader that is executed by the pipeline.
    // Keep a reference to the shader to prevent it from being dropped.
    _shader: Arc<Shader>,

    // Descriptor set layouts that are used by the pipeline.
    // It describes how the resources are binded to the shader.
    descriptor_set_layouts: Vec<Arc<DescriptorSetLayout>>,

    // Native Vulkan pipeline layout handle.
    vk_pipeline_layout: vk::PipelineLayout,

    // Native Vulkan pipeline handle.
    vk_pipeline: vk::Pipeline,
}

#[derive(Default)]
pub struct PipelineBuilder {
    shader: Option<Arc<Shader>>,
    descriptor_set_layouts: HashMap<usize, Arc<DescriptorSetLayout>>,
}

// Mark `Pipeline` as a GPU resource that should be kept alive while it's in use by the GPU context.
impl Resource for Pipeline {}

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

    pub fn build(&self, device: Arc<Device>) -> GpuResult<Arc<Pipeline>> {
        Ok(Arc::new(Pipeline::new(device, self)?))
    }
}

impl Pipeline {
    pub fn builder() -> PipelineBuilder {
        Default::default()
    }

    pub(crate) fn new(device: Arc<Device>, builder: &PipelineBuilder) -> GpuResult<Self> {
        let vk_descriptor_set_layouts = (0..builder.descriptor_set_layouts.len())
            .map(|descriptor_set_index| {
                if let Some(descriptor_set_layouts) =
                    builder.descriptor_set_layouts.get(&descriptor_set_index)
                {
                    Ok(descriptor_set_layouts.vk_descriptor_set_layout())
                } else {
                    Err(GpuError::Other(format!(
                        "Descriptor set layout {} is missing",
                        descriptor_set_index
                    )))
                }
            })
            .collect::<GpuResult<Vec<_>>>()?;

        // Create a Vulkan pipeline layout.
        let vk_pipeline_layout_create_info = vk::PipelineLayoutCreateInfo::default()
            .set_layouts(&vk_descriptor_set_layouts)
            .push_constant_ranges(&[]);
        let vk_pipeline_layout = unsafe {
            device.vk_device().create_pipeline_layout(
                &vk_pipeline_layout_create_info,
                device.cpu_allocation_callbacks(),
            )?
        };

        let shader = builder
            .shader
            .clone()
            .ok_or_else(|| GpuError::Other("Pipeline shader is required".to_string()))?;

        // Create a Vulkan compute pipeline.
        // Before we start, we need to check if the device supports dynamic subgroup size.
        // If it does, we need to set the required subgroup size for the shader.
        // Do do that, we need to create a `vk::PipelineShaderStageRequiredSubgroupSizeCreateInfo`
        // which is an v1.3 api structure that is used to specify the required subgroup size for a shader.
        let mut subgroup_size_create_info = if device.is_dynamic_subgroup_size() {
            Some(
                vk::PipelineShaderStageRequiredSubgroupSizeCreateInfo::default()
                    .required_subgroup_size(device.subgroup_size() as u32),
            )
        } else {
            None
        };

        // Initialize the shader stage create info.
        // It contains the shader module, entry point, mark the stage as compute etc.
        let mut vk_pipeline_shader_stage_create_info = vk::PipelineShaderStageCreateInfo::default()
            .stage(vk::ShaderStageFlags::COMPUTE)
            .module(shader.vk_shader_module())
            .name(SHADER_ENTRY_POINT);

        // Append the subgroup size info to the shader stage create info if it's present.
        if let Some(subgroup_size_info) = &mut subgroup_size_create_info {
            vk_pipeline_shader_stage_create_info =
                vk_pipeline_shader_stage_create_info.push_next(subgroup_size_info);
        }

        // Finally, create the pipeline.
        let vk_compute_pipeline_create_info = vk::ComputePipelineCreateInfo::default()
            .stage(vk_pipeline_shader_stage_create_info)
            .layout(vk_pipeline_layout);

        let vk_pipelines_result = unsafe {
            device.vk_device().create_compute_pipelines(
                vk::PipelineCache::null(),
                &[vk_compute_pipeline_create_info],
                device.cpu_allocation_callbacks(),
            )
        };

        match vk_pipelines_result {
            Ok(vk_pipelines) => {
                let vk_pipeline = vk_pipelines.first().copied().ok_or_else(|| {
                    GpuError::Other("Failed to create compute pipeline".to_string())
                })?;
                Ok(Self {
                    device,
                    _shader: shader,
                    vk_pipeline_layout,
                    vk_pipeline,
                    descriptor_set_layouts: builder
                        .descriptor_set_layouts
                        .values()
                        .cloned()
                        .collect(),
                })
            }
            Err(error) => {
                // if we failed to create the pipeline, we need to destroy the pipeline layout.
                unsafe {
                    device.vk_device().destroy_pipeline_layout(
                        vk_pipeline_layout,
                        device.cpu_allocation_callbacks(),
                    );
                }
                Err(GpuError::from(error.1))
            }
        }
    }

    pub fn vk_pipeline(&self) -> vk::Pipeline {
        self.vk_pipeline
    }

    pub fn vk_pipeline_layout(&self) -> vk::PipelineLayout {
        self.vk_pipeline_layout
    }
}

impl Drop for Pipeline {
    fn drop(&mut self) {
        if self.vk_pipeline != vk::Pipeline::null() {
            unsafe {
                self.device
                    .vk_device()
                    .destroy_pipeline(self.vk_pipeline, self.device.cpu_allocation_callbacks());
            }
            self.vk_pipeline = vk::Pipeline::null();
        }

        if self.vk_pipeline_layout != vk::PipelineLayout::null() {
            unsafe {
                self.device.vk_device().destroy_pipeline_layout(
                    self.vk_pipeline_layout,
                    self.device.cpu_allocation_callbacks(),
                );
            }
            self.vk_pipeline_layout = vk::PipelineLayout::null();
        }

        // free binded resources
        self.descriptor_set_layouts.clear();
    }
}
