use std::sync::Arc;

use ash::vk;

use crate::*;

/// Struct that represents a compiled shader.
/// If you want to create a new shader, you have to compile it first using `shaderc`.
pub struct Shader {
    /// Device that owns the shader.
    pub device: Arc<Device>,

    /// Vulkan shader module handle.
    pub vk_shader_module: vk::ShaderModule,
}

// Mark `Shader` as a GPU resource that should be kept alive while it's in use by the GPU context.
impl Resource for Shader {}

impl Shader {
    /// Create a new shader from the given compiled shader code.
    /// `shader_code` is a compiled shader code in the binary SPIR-V format.
    pub fn new(device: Arc<Device>, shader_code: &[u8]) -> GpuResult<Self> {
        // Decode SPIR-V from bytes with correct alignment.
        let mut spv_file = std::io::Cursor::new(shader_code);
        let shader_code = ash::util::read_spv(&mut spv_file)
            .map_err(|_| GpuError::Other("Failed to read SPIR-V shader code".to_string()))?;

        // Create shader.
        let shader_module_create_info = vk::ShaderModuleCreateInfo::builder()
            .code(&shader_code)
            .build();
        let shader_module = unsafe {
            device.vk_device.create_shader_module(
                &shader_module_create_info,
                device.cpu_allocation_callbacks(),
            )?
        };

        Ok(Self {
            device,
            vk_shader_module: shader_module,
        })
    }
}

impl Drop for Shader {
    fn drop(&mut self) {
        if self.vk_shader_module != vk::ShaderModule::null() {
            unsafe {
                self.device.vk_device.destroy_shader_module(
                    self.vk_shader_module,
                    self.device.cpu_allocation_callbacks(),
                );
            }
            self.vk_shader_module = vk::ShaderModule::null();
        }
    }
}
