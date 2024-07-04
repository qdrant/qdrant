use std::ffi::CStr;
use std::sync::Arc;

use ash::vk;

use crate::*;

pub struct Shader {
    pub device: Arc<Device>,
    pub vk_shader_module: vk::ShaderModule,
}

pub struct ShaderBuilder {
    device: Arc<Device>,
    shader_code: String,
    element_type: Option<GpuVectorStorageElementType>,
    layout_bindings: Vec<(LayoutSetBinding, usize)>,
    dim: Option<usize>,
}

impl ShaderBuilder {
    pub fn new(device: Arc<Device>) -> Self {
        Self {
            device,
            shader_code: Default::default(),
            element_type: None,
            layout_bindings: Default::default(),
            dim: None,
        }
    }

    pub fn with_shader_code(&mut self, shader_code: &str) -> &mut Self {
        self.shader_code.push_str("\n");
        self.shader_code.push_str(shader_code);
        self
    }

    pub fn with_element_type(&mut self, element_type: GpuVectorStorageElementType) -> &mut Self {
        self.element_type = Some(element_type);
        self
    }

    pub fn with_dim(&mut self, dim: usize) -> &mut Self {
        self.dim = Some(dim);
        self
    }

    pub fn with_layout(&mut self, layout: LayoutSetBinding, binding: usize) -> &mut Self {
        self.layout_bindings.push((layout, binding));
        self
    }

    pub fn build(&self) -> Shader {
        let mut options = shaderc::CompileOptions::new().unwrap();
        options.set_optimization_level(shaderc::OptimizationLevel::Performance);
        options.set_target_env(shaderc::TargetEnv::Vulkan, shaderc::EnvVersion::Vulkan1_3 as u32);
        options.set_target_spirv(shaderc::SpirvVersion::V1_3);

        if let Some(element_type) = self.element_type {
            match element_type {
                GpuVectorStorageElementType::Float32 => options.add_macro_definition("VECTOR_STORAGE_ELEMENT_FLOAT32", None),
                GpuVectorStorageElementType::Float16 => options.add_macro_definition("VECTOR_STORAGE_ELEMENT_FLOAT16", None),
                GpuVectorStorageElementType::Uint8 => options.add_macro_definition("VECTOR_STORAGE_ELEMENT_UINT8", None),
                GpuVectorStorageElementType::Binary => options.add_macro_definition("VECTOR_STORAGE_ELEMENT_BINARY", None),
            }
        }

        for (layout, binding) in &self.layout_bindings {
            options.add_macro_definition(layout.to_string(), Some(&binding.to_string()));
        }

        if let Some(dim) = self.dim {
            options.add_macro_definition("DIM", Some(&dim.to_string()));
        }

        let timer = std::time::Instant::now();
        let compiled = self.device.compiler.compile_into_spirv(
            &self.shader_code, shaderc::ShaderKind::Compute,
            "shader.glsl", "main", Some(&options)).unwrap();
        log::debug!("Shader compilation took: {:?}", timer.elapsed());
        Shader::new(self.device.clone(), compiled.as_binary_u8())
    }

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
