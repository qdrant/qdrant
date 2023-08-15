use std::ffi::CStr;
use std::sync::Arc;

use ash::vk;

use crate::*;

pub struct Shader {
    pub device: Arc<Device>,
    pub vk_shader_module: vk::ShaderModule,
}

unsafe impl Send for Shader {}
unsafe impl Sync for Shader {}

impl Resource for Shader {}

impl Drop for Shader {
    fn drop(&mut self) {
        if self.vk_shader_module != vk::ShaderModule::null() {
            unsafe {
                self.device
                    .vk_device
                    .destroy_shader_module(self.vk_shader_module, self.device.alloc());
            }
            self.vk_shader_module = vk::ShaderModule::null();
        }
    }
}

impl Shader {
    pub fn new(device: Arc<Device>, shader_code: &[u8]) -> Self {
        let mut spv_file = std::io::Cursor::new(shader_code);
        let shader_code = ash::util::read_spv(&mut spv_file).unwrap();

        let shader_module_create_info = vk::ShaderModuleCreateInfo::builder()
            .code(&shader_code)
            .build();
        let shader_module = unsafe {
            device
                .vk_device
                .create_shader_module(&shader_module_create_info, device.alloc())
                .unwrap()
        };
        Self {
            device,
            vk_shader_module: shader_module,
        }
    }

    pub(crate) fn get_pipeline_shader_stage_create_info(
        &self,
    ) -> vk::PipelineShaderStageCreateInfo {
        vk::PipelineShaderStageCreateInfo::builder()
            .stage(vk::ShaderStageFlags::COMPUTE)
            .module(self.vk_shader_module)
            .name(CStr::from_bytes_with_nul(b"main\0").unwrap())
            .build()
    }
}
