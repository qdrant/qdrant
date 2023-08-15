use std::sync::Arc;

use ash::vk;

use crate::*;

pub struct DescriptorSetLayoutBuilder {
    pub uniform_buffer_bindings: Vec<usize>,
    pub storage_buffer_bindings: Vec<usize>,
}

#[derive(Clone)]
pub struct DescriptorSetLayout {
    pub device: Arc<Device>,
    pub uniform_buffer_bindings: Vec<usize>,
    pub storage_buffer_bindings: Vec<usize>,
    pub vk_descriptor_set_layout: vk::DescriptorSetLayout,
}

impl DescriptorSetLayoutBuilder {
    pub fn add_uniform_buffer(mut self, binding: usize) -> Self {
        self.uniform_buffer_bindings.push(binding);
        self
    }

    pub fn add_storage_buffer(mut self, binding: usize) -> Self {
        self.storage_buffer_bindings.push(binding);
        self
    }

    pub fn build(&self, device: Arc<Device>) -> Arc<DescriptorSetLayout> {
        let mut descriptor_set_layout_bindings = Vec::new();
        for binding in &self.uniform_buffer_bindings {
            descriptor_set_layout_bindings.push(
                vk::DescriptorSetLayoutBinding::builder()
                    .binding(*binding as u32)
                    .descriptor_type(vk::DescriptorType::UNIFORM_BUFFER)
                    .descriptor_count(1)
                    .stage_flags(vk::ShaderStageFlags::COMPUTE)
                    .build(),
            );
        }

        for binding in &self.storage_buffer_bindings {
            descriptor_set_layout_bindings.push(
                vk::DescriptorSetLayoutBinding::builder()
                    .binding(*binding as u32)
                    .descriptor_type(vk::DescriptorType::STORAGE_BUFFER)
                    .descriptor_count(1)
                    .stage_flags(vk::ShaderStageFlags::COMPUTE)
                    .build(),
            );
        }

        let descriptor_set_layout_create_info = vk::DescriptorSetLayoutCreateInfo::builder()
            .bindings(&descriptor_set_layout_bindings)
            .build();

        let vk_descriptor_set_layout = unsafe {
            device
                .vk_device
                .create_descriptor_set_layout(&descriptor_set_layout_create_info, device.alloc())
                .unwrap()
        };

        Arc::new(DescriptorSetLayout {
            device,
            uniform_buffer_bindings: self.uniform_buffer_bindings.clone(),
            storage_buffer_bindings: self.storage_buffer_bindings.clone(),
            vk_descriptor_set_layout,
        })
    }
}

impl Drop for DescriptorSetLayout {
    fn drop(&mut self) {
        unsafe {
            if self.vk_descriptor_set_layout != vk::DescriptorSetLayout::null() {
                self.device.vk_device.destroy_descriptor_set_layout(
                    self.vk_descriptor_set_layout,
                    self.device.alloc(),
                );
                self.vk_descriptor_set_layout = vk::DescriptorSetLayout::null();
            }
            self.storage_buffer_bindings.clear();
            self.uniform_buffer_bindings.clear();
        }
    }
}

impl DescriptorSetLayout {
    pub fn builder() -> DescriptorSetLayoutBuilder {
        DescriptorSetLayoutBuilder {
            uniform_buffer_bindings: Vec::new(),
            storage_buffer_bindings: Vec::new(),
        }
    }
}
