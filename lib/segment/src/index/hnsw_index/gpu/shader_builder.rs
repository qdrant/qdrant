use std::collections::HashMap;
use std::sync::Arc;

use crate::common::operation_error::OperationResult;

pub struct ShaderBuilder {
    device: Arc<gpu::Device>,
    shader_code: String,
    modules: HashMap<String, String>,
    defines: HashMap<String, Option<String>>,
    extra_slangc_args: Vec<String>,
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
            (
                "config.slang".to_string(),
                include_str!("shaders/config.slang").to_string(),
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
            extra_slangc_args: Vec::new(),
        }
    }

    pub fn with_parameters<T: ShaderBuilderParameters>(&mut self, parameters: &T) -> &mut Self {
        self.modules.extend(parameters.shader_includes());
        self.defines.extend(parameters.shader_defines());
        self
    }

    pub fn with_extra_slangc_args(&mut self, args: &[&str]) -> &mut Self {
        self.extra_slangc_args
            .extend(args.iter().map(|s| s.to_string()));
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

        // BDA uses -D USE_BDA on the slangc command line so the define propagates
        // to all imported modules. Here we only emit the BufferOffset typedef.
        let use_bda = self.defines.contains_key("USE_BDA");
        if use_bda {
            config.push_str("typedef uint64_t BufferOffset;\n\n");
        } else {
            config.push_str("typedef uint BufferOffset;\n\n");
        }

        for (key, value) in &self.defines {
            // USE_BDA is handled above as a preprocessor #define.
            if key == "USE_BDA" {
                continue;
            }
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

        // Quantization buffer bindings (fixed layout within VECTOR_STORAGE_LAYOUT_SET).
        // Bindings 0-3 are vector data (ByteAddressBuffer).
        config.push_str("\nstatic const uint SQ_OFFSETS_BINDING = 4;\n");
        config.push_str("static const uint PQ_CENTROIDS_BINDING = 5;\n");
        config.push_str("static const uint PQ_DIVISIONS_BINDING = 6;\n");

        // Default false for boolean flags that may not always be set.
        let bool_defaults = [
            "EXACT",
            "BHEAP_LINEAR",
            "COSINE_DISTANCE",
            "DOT_DISTANCE",
            "EUCLID_DISTANCE",
            "MANHATTAN_DISTANCE",
            "VECTOR_STORAGE_ELEMENT_FLOAT32",
            "VECTOR_STORAGE_ELEMENT_FLOAT16",
            "VECTOR_STORAGE_ELEMENT_UINT8",
            "VECTOR_STORAGE_ELEMENT_BQ",
            "VECTOR_STORAGE_ELEMENT_SQ",
            "VECTOR_STORAGE_ELEMENT_PQ",
            "VECTOR_STORAGE_QUANTIZATION",
            "MULTIVECTORS",
            "VISITED_FLAGS_REMAP",
        ];
        for key in bool_defaults {
            if !self.defines.contains_key(key) {
                config.push_str(&format!("static const bool {key} = false;\n"));
            }
        }

        // Default values for uint constants that may not always be set.
        let uint_defaults = [
            ("BQ_SKIP_COUNT", 0),
            ("PQ_DIVISIONS_COUNT", 0),
            ("PQ_CENTROIDS_DIM", 0),
            ("MULTIVECTOR_OFFSETS_BINDING", 7),
            ("LINKS_CAPACITY", 0),
            ("VISITED_FLAGS_CAPACITY", 0),
        ];
        for (key, default) in uint_defaults {
            if !self.defines.contains_key(key) {
                config.push_str(&format!("static const uint {key} = {default};\n"));
            }
        }

        // Default values for float constants that may not always be set.
        let float_defaults = [("SQ_MULTIPLIER", "1.0"), ("SQ_DIFF", "0.0")];
        for (key, default) in float_defaults {
            if !self.defines.contains_key(key) {
                config.push_str(&format!("static const float {key} = {default};\n"));
            }
        }

        config
    }

    pub fn build(&self, shader_name: &str) -> OperationResult<Arc<gpu::Shader>> {
        let timer = std::time::Instant::now();

        let mut modules = self.modules.clone();
        modules.insert("config.slang".to_string(), self.generate_config_slang());

        // Collect extra slangc args. Add BDA capability automatically when USE_BDA is set.
        let mut extra_args: Vec<&str> = self.extra_slangc_args.iter().map(|s| s.as_str()).collect();
        if self.defines.contains_key("USE_BDA") {
            extra_args.extend([
                "-capability",
                "SPV_EXT_physical_storage_buffer",
                "-D",
                "USE_BDA",
            ]);
        }

        let compiled = self.device.compile_shader_with_extra_args(
            &self.shader_code,
            shader_name,
            None,
            Some(&modules),
            &extra_args,
        )?;
        log::debug!("Shader compilation took: {:?}", timer.elapsed());
        Ok(gpu::Shader::new(self.device.clone(), &compiled)?)
    }
}
