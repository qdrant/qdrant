use std::collections::HashMap;
use std::sync::Arc;

use crate::common::operation_error::OperationResult;

pub struct ShaderBuilder {
    device: Arc<gpu::Device>,
    shader_code: String,
    includes: HashMap<String, String>,
    defines: HashMap<String, Option<String>>,
}

pub trait ShaderBuilderParameters {
    fn shader_includes(&self) -> HashMap<String, String>;
    fn shader_defines(&self) -> HashMap<String, Option<String>>;
}

impl ShaderBuilder {
    pub fn new(device: Arc<gpu::Device>) -> Self {
        let includes = HashMap::from([
            (
                "common.comp".to_string(),
                include_str!("shaders/common.comp").to_string(),
            ),
            (
                "extensions.comp".to_string(),
                include_str!("shaders/extensions.comp").to_string(),
            ),
        ]);

        let mut defines = HashMap::new();
        defines.insert(
            "SUBGROUP_SIZE".to_owned(),
            Some(device.subgroup_size().to_string()),
        );

        Self {
            device,
            shader_code: Default::default(),
            includes,
            defines,
        }
    }

    pub fn with_parameters<T: ShaderBuilderParameters>(&mut self, parameters: &T) -> &mut Self {
        self.includes.extend(parameters.shader_includes());
        self.defines.extend(parameters.shader_defines());
        self
    }

    pub fn with_shader_code(&mut self, shader_code: &str) -> &mut Self {
        self.shader_code.push_str(shader_code);
        self.shader_code.push('\n');
        self
    }

    pub fn build(&self, shader_name: &str) -> OperationResult<Arc<gpu::Shader>> {
        let timer = std::time::Instant::now();
        let compiled = self.device.instance().compile_shader(
            &self.shader_code,
            shader_name,
            Some(&self.defines),
            Some(&self.includes),
        )?;
        log::debug!("Shader compilation took: {:?}", timer.elapsed());
        Ok(gpu::Shader::new(self.device.clone(), &compiled)?)
    }
}
