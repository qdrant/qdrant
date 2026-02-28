use std::collections::HashMap;
use std::sync::Arc;

use crate::common::operation_error::OperationResult;

pub struct ShaderBuilder {
    device: Arc<gpu::Device>,
    shader_code: String,
    modules: HashMap<String, String>,
    defines: HashMap<String, Option<String>>,
}

pub trait ShaderBuilderParameters {
    fn shader_includes(&self) -> HashMap<String, String>;
    fn shader_defines(&self) -> HashMap<String, Option<String>>;
}

impl ShaderBuilder {
    pub fn new(device: Arc<gpu::Device>) -> Self {
        let modules = HashMap::from([
            (
                "common.slang".to_string(),
                include_str!("shaders/common.slang").to_string(),
            ),
            (
                "distance_metric.slang".to_string(),
                include_str!("shaders/distance_metric.slang").to_string(),
            ),
        ]);

        let mut defines = HashMap::new();
        defines.insert(
            "WAVE_SIZE".to_owned(),
            Some(device.subgroup_size().to_string()),
        );

        Self {
            device,
            shader_code: Default::default(),
            modules,
            defines,
        }
    }

    pub fn with_parameters<T: ShaderBuilderParameters>(&mut self, parameters: &T) -> &mut Self {
        self.modules.extend(parameters.shader_includes());
        self.defines.extend(parameters.shader_defines());
        self
    }

    pub fn with_shader_code(&mut self, shader_code: &str) -> &mut Self {
        self.shader_code.push_str(shader_code);
        self.shader_code.push('\n');
        self
    }

    /// Generate the `config.slang` module from collected defines.
    fn generate_config_slang(&self) -> String {
        let mut config = String::from("// Generated config module — do not edit.\n\n");

        for (key, value) in &self.defines {
            match value {
                Some(val) => {
                    if val.contains('.') {
                        config.push_str(&format!("static const float {key} = {val};\n"));
                    } else {
                        config.push_str(&format!("static const uint {key} = {val};\n"));
                    }
                }
                None => {
                    config.push_str(&format!("static const bool {key} = true;\n"));
                }
            }
        }

        // Derived constants.
        let get_uint = |key: &str| -> u32 {
            self.defines
                .get(key)
                .and_then(|v| v.as_ref())
                .and_then(|v| v.parse().ok())
                .unwrap_or(0)
        };

        let dim = get_uint("DIM");
        let wave_size = get_uint("WAVE_SIZE").max(1);
        let ef = get_uint("EF");

        let pack_size: u32 = 4;
        config.push_str(&format!(
            "\nstatic const uint PACK_SIZE = {pack_size};\n"
        ));

        if dim > 0 {
            let waves_per_vector = dim / (pack_size * wave_size);
            let single_wave = waves_per_vector < 2;
            config.push_str(&format!(
                "static const uint WAVES_PER_VECTOR = {waves_per_vector};\n"
            ));
            config.push_str(&format!(
                "static const bool SINGLE_WAVE_PER_VECTOR = {single_wave};\n",
            ));
        }

        // Only generate SHARED_BUFFER_CAPACITY and heap offsets if not explicitly provided.
        if ef > 0 && !self.defines.contains_key("SHARED_BUFFER_CAPACITY") {
            config.push_str(&format!(
                "\nstatic const uint SHARED_BUFFER_CAPACITY = {};\n",
                2 * ef
            ));
            config.push_str("static const uint NEAREST_HEAP_OFFSET = 0;\n");
            config.push_str(&format!(
                "static const uint CANDIDATES_HEAP_OFFSET = {ef};\n"
            ));
        }

        // Binding layout set indices (fixed layout).
        config.push_str("\nstatic const uint VECTOR_STORAGE_LAYOUT_SET = 1;\n");
        config.push_str("static const uint LINKS_LAYOUT_SET = 2;\n");
        config.push_str("static const uint VISITED_FLAGS_LAYOUT_SET = 3;\n");

        // Default false for boolean flags that may not always be set.
        let bool_defaults = [
            "EXACT",
            "BHEAP_LINEAR",
            "COSINE_DISTANCE",
            "DOT_DISTANCE",
            "EUCLID_DISTANCE",
            "MANHATTAN_DISTANCE",
        ];
        for key in bool_defaults {
            if !self.defines.contains_key(key) {
                config.push_str(&format!("static const bool {key} = false;\n"));
            }
        }

        config
    }

    /// Generate the `metric_config.slang` module from collected defines.
    fn generate_metric_config_slang(&self) -> String {
        let has = |key: &str| self.defines.contains_key(key);
        let distance = if has("COSINE_DISTANCE") {
            "Cosine"
        } else if has("DOT_DISTANCE") {
            "Dot"
        } else if has("EUCLID_DISTANCE") {
            "Euclid"
        } else if has("MANHATTAN_DISTANCE") {
            "Manhattan"
        } else {
            "Cosine"
        };

        let (import_module, element_type, score_type, metric_name) =
            if has("VECTOR_STORAGE_ELEMENT_BQ") {
                ("vector_storage_bq", "uint", "uint", "BQMetric".to_string())
            } else if has("VECTOR_STORAGE_ELEMENT_SQ") {
                let metric = if has("MANHATTAN_DISTANCE") {
                    "SQManhattan"
                } else {
                    "SQDot"
                };
                (
                    "vector_storage_sq",
                    "vector<uint8_t, 4>",
                    "uint",
                    metric.to_string(),
                )
            } else if has("VECTOR_STORAGE_ELEMENT_PQ") {
                (
                    "vector_storage_pq",
                    "vector<uint8_t, 4>",
                    "float",
                    "PQMetric".to_string(),
                )
            } else if has("VECTOR_STORAGE_ELEMENT_FLOAT16") {
                ("vector_storage_f16", "half4", "float", format!("{distance}F16"))
            } else if has("VECTOR_STORAGE_ELEMENT_UINT8") {
                (
                    "vector_storage_u8",
                    "vector<uint8_t, 4>",
                    "uint",
                    format!("{distance}U8"),
                )
            } else {
                ("vector_storage_f32", "float4", "float", format!("{distance}F32"))
            };

        format!(
            "// Generated metric config — do not edit.\n\
             import {import_module};\n\
             \n\
             typedef {element_type} ElementType;\n\
             typedef {score_type} ScoreType;\n\
             typedef {metric_name} ActiveMetric;\n"
        )
    }

    pub fn build(&self, shader_name: &str) -> OperationResult<Arc<gpu::Shader>> {
        let timer = std::time::Instant::now();

        let mut modules = self.modules.clone();
        modules.insert("config.slang".to_string(), self.generate_config_slang());

        // Generate metric_config if vector storage is configured.
        if self.defines.contains_key("DIM") {
            modules.insert(
                "metric_config.slang".to_string(),
                self.generate_metric_config_slang(),
            );
        }

        let compiled = self.device.instance().compile_shader(
            &self.shader_code,
            shader_name,
            None,
            Some(&modules),
        )?;
        log::debug!("Shader compilation took: {:?}", timer.elapsed());
        Ok(gpu::Shader::new(self.device.clone(), &compiled)?)
    }
}
