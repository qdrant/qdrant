use std::collections::HashMap;
use std::ffi::CStr;
use std::sync::Arc;

use ash::vk;
use parking_lot::Mutex;

use crate::*;

static APPLICATION_NAME: &std::ffi::CStr = c"qdrant";

static VALIDATION_LAYER_NAME: &std::ffi::CStr = c"VK_LAYER_KHRONOS_validation";

/// `Instance` is a Vulkan instance wrapper.
/// It's a root structure for all Vulkan operations and provides access the API.
/// It also manages all Vulkan API layers and API extensions.
pub struct Instance {
    /// Vulkan API entry point. It should be kept alive while the instance is alive.
    _entry: ash::Entry,

    /// Native Vulkan instance handle.
    vk_instance: ash::Instance,

    /// List of physical devices found by the Vulkan instance.
    vk_physical_devices: Vec<PhysicalDevice>,

    /// CPU allocator.
    allocation_callbacks: Option<Box<dyn AllocationCallbacks>>,

    /// Validation layer handler.
    vk_debug_utils_loader: Option<ash::ext::debug_utils::Instance>,

    /// Validation layer messenger.
    vk_debug_messenger: vk::DebugUtilsMessengerEXT,

    /// Shader compiler.
    compiler: Mutex<shaderc::Compiler>,

    /// Disable half precision support. It's useful for unit tests.
    skip_half_precision: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Hardware type of the physical device.
pub enum PhysicalDeviceType {
    /// Discrete GPU like Nvidia or AMD.
    Discrete,
    /// Integrated graphics like Intel HD Graphics.
    Integrated,
    /// Other types of hardware like software emulated GPU.
    Other,
}

#[derive(Clone)]
pub struct PhysicalDevice {
    pub vk_physical_device: vk::PhysicalDevice,
    pub name: String,
    pub device_type: PhysicalDeviceType,
}

#[derive(Default)]
pub struct InstanceBuilder<'a> {
    debug_messenger: Option<&'a dyn DebugMessenger>,
    allocation_callbacks: Option<Box<dyn AllocationCallbacks>>,
    dump_api: bool,
    skip_half_precision: bool,
}

impl<'a> InstanceBuilder<'a> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set debug messenger for the instance.
    pub fn with_debug_messenger(mut self, debug_messenger: &'a dyn DebugMessenger) -> Self {
        self.debug_messenger = Some(debug_messenger);
        self
    }

    /// Set CPU allocation callbacks for the instance.
    pub fn with_allocation_callbacks(
        mut self,
        allocation_callbacks: Box<dyn AllocationCallbacks>,
    ) -> Self {
        self.allocation_callbacks = Some(allocation_callbacks);
        self
    }

    // Enable API dump layer.
    pub fn with_dump_api(mut self, dump_api: bool) -> Self {
        self.dump_api = dump_api;
        self
    }

    /// Disable half precision support. It's useful for unit tests.
    pub fn with_skip_half_precision(mut self, skip_half_precision: bool) -> Self {
        self.skip_half_precision = skip_half_precision;
        self
    }

    pub fn build(self) -> GpuResult<Arc<Instance>> {
        Instance::new(
            self.debug_messenger,
            self.allocation_callbacks,
            self.dump_api,
            self.skip_half_precision,
        )
    }
}

impl Instance {
    pub fn builder() -> InstanceBuilder<'static> {
        InstanceBuilder::new()
    }

    fn new(
        debug_messenger: Option<&dyn DebugMessenger>,
        allocation_callbacks: Option<Box<dyn AllocationCallbacks>>,
        dump_api: bool,
        skip_half_precision: bool,
    ) -> GpuResult<Arc<Self>> {
        // Create a shader compiler before we start.
        // It's used to compile GLSL into SPIR-V.
        let compiler = Mutex::new(
            shaderc::Compiler::new()
                .ok_or_else(|| GpuError::Other("Failed to create shaderc compiler".to_string()))?,
        );

        // Create Vulkan API entry point.
        let entry = unsafe {
            ash::Entry::load().map_err(|e| {
                GpuError::Other(format!("Failed to create Vulkan API entry point {e:?}"))
            })?
        };

        // Collect Vulkan application info.
        // It contains application name and required Vulkan API version.
        let app_info = vk::ApplicationInfo::default()
            .application_name(APPLICATION_NAME)
            .application_version(0)
            .engine_name(APPLICATION_NAME)
            .engine_version(0)
            .api_version(vk::make_api_version(0, 1, 3, 0));

        // Collect Vulkan API extensions and convert it in raw pointers.
        let extensions = Self::extensions_list(debug_messenger.is_some());
        // Check presence of all required extensions.
        Self::check_extensions_list(&entry, &extensions)?;
        let extension_names_raw: Vec<*const i8> = extensions
            .iter()
            .map(|raw_name| raw_name.as_ptr())
            .collect();

        // Collect Vulkan API layers and convert it in raw pointers.
        let layers = Self::layers_list(debug_messenger.is_some(), dump_api);
        // Check presence of all required layers.
        Self::check_layers_list(&entry, &layers)?;
        let layers_raw: Vec<*const i8> = layers.iter().map(|raw_name| raw_name.as_ptr()).collect();

        // If we provide debug messenger, we need to create a debug messenger info.
        let mut debug_utils_create_info = debug_messenger.map(Self::debug_messenger_create_info);

        let create_flags = if cfg!(any(target_os = "macos")) {
            // On MacOS we need to enable portability extension to enable MoltenVK.
            vk::InstanceCreateFlags::ENUMERATE_PORTABILITY_KHR
        } else {
            vk::InstanceCreateFlags::default()
        };

        // Collect all parameters together and create Vulkan instance.
        let mut create_info = vk::InstanceCreateInfo::default()
            .flags(create_flags)
            .application_info(&app_info)
            .enabled_layer_names(&layers_raw)
            .enabled_extension_names(&extension_names_raw);

        if let Some(debug_utils_create_info) = &mut debug_utils_create_info {
            create_info = create_info.push_next(debug_utils_create_info);
        }

        let mut settings = LayerSettingsBuilder::default();
        let mut settings_create_info;
        if layers.contains(&VALIDATION_LAYER_NAME) {
            Self::set_validation_settings(&mut settings);
            settings_create_info =
                vk::LayerSettingsCreateInfoEXT::default().settings(settings.settings());
            create_info = create_info.push_next(&mut settings_create_info);
        }

        // Get CPU allocation callbacks if they are provided.
        let vk_allocation_callbacks = allocation_callbacks
            .as_ref()
            .map(|a| a.allocation_callbacks());

        // Finally, create Vulkan instance.
        let vk_instance: ash::Instance =
            unsafe { entry.create_instance(&create_info, vk_allocation_callbacks)? };

        // Find all available physical GPU devices.
        let vk_physical_devices_result = unsafe { vk_instance.enumerate_physical_devices() };
        let vk_physical_devices = match vk_physical_devices_result {
            Ok(vk_physical_devices) => vk_physical_devices,
            Err(e) => {
                // Don't forget to destroy the instance if we failed to find any physical devices.
                unsafe {
                    vk_instance.destroy_instance(vk_allocation_callbacks);
                }
                return Err(GpuError::from(e));
            }
        };

        let vk_physical_devices = vk_physical_devices
            .iter()
            .map(|&vk_physical_device| {
                let device_properties =
                    unsafe { vk_instance.get_physical_device_properties(vk_physical_device) };
                let device_name =
                    unsafe { ::std::ffi::CStr::from_ptr(device_properties.device_name.as_ptr()) };
                let device_name = device_name.to_str().unwrap_or("Unnamed GPU").to_owned();

                let device_type = match device_properties.device_type {
                    vk::PhysicalDeviceType::DISCRETE_GPU => PhysicalDeviceType::Discrete,
                    vk::PhysicalDeviceType::INTEGRATED_GPU => PhysicalDeviceType::Integrated,
                    _ => PhysicalDeviceType::Other,
                };

                log::info!("Found GPU device: {device_name}");
                PhysicalDevice {
                    vk_physical_device,
                    name: device_name,
                    device_type,
                }
            })
            .collect::<Vec<_>>();

        if vk_physical_devices.is_empty() {
            // Don't forget to destroy the instance if we failed to find any physical devices.
            unsafe {
                vk_instance.destroy_instance(vk_allocation_callbacks);
            }
            return Err(GpuError::Other(
                "No Vulkan physical devices found".to_string(),
            ));
        }

        // If we have a debug messenger, we need to create it.
        let (vk_debug_utils_loader, vk_debug_messenger) = if let Some(debug_messenger) =
            debug_messenger
        {
            let debug_utils_loader = ash::ext::debug_utils::Instance::new(&entry, &vk_instance);
            let messenger_create_info = Self::debug_messenger_create_info(debug_messenger);
            let utils_messenger_result = unsafe {
                debug_utils_loader
                    .create_debug_utils_messenger(&messenger_create_info, vk_allocation_callbacks)
            };

            let utils_messenger = match utils_messenger_result {
                Ok(messenger) => messenger,
                Err(e) => {
                    // Don't forget to destroy the instance if we failed to create a debug messenger.
                    unsafe {
                        vk_instance.destroy_instance(vk_allocation_callbacks);
                    }
                    return Err(GpuError::from(e));
                }
            };
            (Some(debug_utils_loader), utils_messenger)
        } else {
            (None, vk::DebugUtilsMessengerEXT::null())
        };

        Ok(Arc::new(Self {
            _entry: entry,
            vk_instance,
            vk_physical_devices,
            allocation_callbacks,
            vk_debug_utils_loader,
            vk_debug_messenger,
            skip_half_precision,
            compiler,
        }))
    }

    fn debug_messenger_create_info(
        debug_messenger: &dyn DebugMessenger,
    ) -> vk::DebugUtilsMessengerCreateInfoEXT {
        vk::DebugUtilsMessengerCreateInfoEXT::default()
            .flags(vk::DebugUtilsMessengerCreateFlagsEXT::empty())
            .message_severity(debug_messenger.severity_flags())
            .message_type(debug_messenger.message_type_flags())
            .pfn_user_callback(debug_messenger.callback())
    }

    pub fn cpu_allocation_callbacks(&self) -> Option<&vk::AllocationCallbacks> {
        self.allocation_callbacks
            .as_ref()
            .map(|alloc| alloc.allocation_callbacks())
    }

    pub fn vk_instance(&self) -> &ash::Instance {
        &self.vk_instance
    }

    pub fn physical_devices(&self) -> &[PhysicalDevice] {
        &self.vk_physical_devices
    }

    pub fn skip_half_precision(&self) -> bool {
        self.skip_half_precision
    }

    pub fn compile_shader(
        &self,
        shader: &str,
        shader_name: &str,
        defines: Option<&HashMap<String, Option<String>>>,
        includes: Option<&HashMap<String, String>>,
    ) -> GpuResult<Vec<u8>> {
        let mut options = shaderc::CompileOptions::new().ok_or_else(|| {
            GpuError::Other("Failed to create shaderc compile options".to_string())
        })?;
        options.set_optimization_level(shaderc::OptimizationLevel::Performance);
        options.set_target_env(
            shaderc::TargetEnv::Vulkan,
            shaderc::EnvVersion::Vulkan1_3 as u32,
        );
        options.set_target_spirv(shaderc::SpirvVersion::V1_3);

        if let Some(defines) = defines {
            for (define, value) in defines {
                match value {
                    Some(value) => {
                        options.add_macro_definition(define, Some(value));
                    }
                    None => {
                        options.add_macro_definition(define, None);
                    }
                }
            }
        }

        if let Some(includes) = includes {
            options.set_include_callback(|filename, _, _, _| {
                if let Some(code) = includes.get(filename) {
                    Ok(shaderc::ResolvedInclude {
                        resolved_name: filename.to_string(),
                        content: code.to_owned(),
                    })
                } else {
                    Err(format!("Include file not found: {filename}"))
                }
            });
        }

        let compiler = self.compiler.lock();
        let result = compiler
            .compile_into_spirv(
                shader,
                shaderc::ShaderKind::Compute,
                shader_name,
                "main",
                Some(&options),
            )
            .map_err(|e| GpuError::Other(format!("Failed to compile shader: {e:?}")))?;
        Ok(result.as_binary_u8().to_owned())
    }

    fn layers_list(validation: bool, dump_api: bool) -> Vec<&'static CStr> {
        let mut result = Vec::new();
        if validation {
            result.push(VALIDATION_LAYER_NAME);
        }
        if dump_api {
            result.push(c"VK_LAYER_LUNARG_api_dump");
        }
        result
    }

    fn extensions_list(validation: bool) -> Vec<&'static CStr> {
        let mut extensions_list = Vec::new();
        if validation {
            extensions_list.push(ash::ext::debug_utils::NAME);
        }

        #[cfg(target_os = "macos")]
        {
            extensions_list.push(ash::khr::portability_enumeration::NAME);
            extensions_list.push(ash::khr::get_physical_device_properties2::NAME);
        }
        extensions_list
    }

    /// Sets validation settings to enable the `debugPrintfEXT()` GLSL function.
    ///
    /// References:
    /// - <https://github.com/KhronosGroup/Vulkan-ValidationLayers/blob/vulkan-sdk-1.4.304.1/docs/debug_printf.md>
    /// - <https://github.com/KhronosGroup/GLSL/blob/9618e893003113a715a06094cf187cef865f8565/extensions/ext/GLSL_EXT_debug_printf.txt>
    fn set_validation_settings(settings: &mut LayerSettingsBuilder) {
        settings.str(
            VALIDATION_LAYER_NAME,
            c"validate_gpu_based",
            c"GPU_BASED_DEBUG_PRINTF",
        );
        settings.bool(VALIDATION_LAYER_NAME, c"printf_to_stdout", true);
        settings.bool(VALIDATION_LAYER_NAME, c"printf_verbose", false);
        settings.uint32(VALIDATION_LAYER_NAME, c"printf_buffer_size", 16384);
    }

    fn check_extensions_list(entry: &ash::Entry, extensions: &[&CStr]) -> GpuResult<()> {
        let extension_properties = unsafe { entry.enumerate_instance_extension_properties(None)? };
        for extension in extensions {
            let extension_found = extension_properties
                .iter()
                .any(|ep| ep.extension_name_as_c_str() == Ok(extension));
            if !extension_found {
                return Err(GpuError::Other(format!(
                    "Extension {extension:?} not found"
                )));
            }
        }
        Ok(())
    }

    fn check_layers_list(entry: &ash::Entry, layers: &[&CStr]) -> GpuResult<()> {
        let layer_properties = unsafe { entry.enumerate_instance_layer_properties()? };
        for layer in layers {
            let layer_found = layer_properties
                .iter()
                .any(|lp| lp.layer_name_as_c_str() == Ok(layer));
            if !layer_found {
                return Err(GpuError::Other(format!("Layer {layer:?} not found")));
            }
        }
        Ok(())
    }
}

impl Drop for Instance {
    fn drop(&mut self) {
        let allocation_callbacks = self.cpu_allocation_callbacks();
        unsafe {
            // Destroy first debug messenger if it's present.
            if let Some(loader) = &self.vk_debug_utils_loader {
                if self.vk_debug_messenger != vk::DebugUtilsMessengerEXT::null() {
                    loader.destroy_debug_utils_messenger(
                        self.vk_debug_messenger,
                        allocation_callbacks,
                    );
                }
            }

            // Last step after all drops of all GPU resources: destroy vulkan instance.
            self.vk_instance.destroy_instance(allocation_callbacks);
        }
    }
}

/// A builder for a slice of [`vk::LayerSettingEXT`], to be used in
/// [`vk::LayerSettingsCreateInfoEXT`].
///
/// Using the Vulkan API is not the only way to set these settings. See
/// <https://vulkan.lunarg.com/doc/view/1.4.304.1/linux/layer_configuration.html>.
#[derive(Default)]
pub struct LayerSettingsBuilder {
    /// Setting values are copied into this arena.
    bump: bumpalo::Bump,
    /// It is marked as 'static to make appeasing the borrow checker less
    /// miserable, but the actual pointers point to the [`Self::bump`] arena.
    ///
    /// We just need make sure that LayerSettingEXT<'static> is not exposed in
    /// the public API.
    settings: Vec<vk::LayerSettingEXT<'static>>,
}

impl LayerSettingsBuilder {
    #[expect(
        clippy::needless_lifetimes,
        reason = "Make it clear that values are borrowed, not 'static."
    )]
    pub fn settings<'a>(&'a self) -> &'a [vk::LayerSettingEXT<'a>] {
        &self.settings
    }

    /// Adds a [`BOOL32`](vk::LayerSettingTypeEXT::BOOL32) setting.
    pub fn bool(&mut self, layer_name: &CStr, setting_name: &CStr, value: bool) {
        self.add_setting(
            layer_name,
            setting_name,
            vk::LayerSettingTypeEXT::BOOL32,
            &[vk::Bool32::from(value)],
        );
    }

    /// Adds an [`UINT32`](vk::LayerSettingTypeEXT::UINT32) setting.
    pub fn uint32(&mut self, layer_name: &CStr, setting_name: &CStr, value: u32) {
        self.add_setting(
            layer_name,
            setting_name,
            vk::LayerSettingTypeEXT::UINT32,
            &[value],
        );
    }

    /// Adds a [`STRING`](vk::LayerSettingTypeEXT::STRING) setting.
    pub fn str(&mut self, layer_name: &CStr, setting_name: &CStr, value: &CStr) {
        let value = self.copy_str(value);
        self.add_setting(
            layer_name,
            setting_name,
            vk::LayerSettingTypeEXT::STRING,
            &[value],
        );
    }

    fn add_setting<T: Copy>(
        &mut self,
        layer_name: &CStr,
        setting_name: &CStr,
        ty: vk::LayerSettingTypeEXT,
        values: &[T],
    ) {
        let p_layer_name = self.copy_str(layer_name);
        let p_setting_name = self.copy_str(setting_name);
        let p_values = self.bump.alloc_slice_copy(values).as_ptr().cast();
        // Can't use builder methods, see https://github.com/ash-rs/ash/issues/986
        self.settings.push(vk::LayerSettingEXT {
            p_layer_name,
            p_setting_name,
            ty,
            value_count: values.len() as u32,
            p_values,
            _marker: std::marker::PhantomData,
        });
    }

    fn copy_str(&self, value: &CStr) -> *const i8 {
        // Not a bottleneck, so don't bother with string interning.
        self.bump
            .alloc_slice_copy(value.to_bytes_with_nul())
            .as_ptr()
            .cast()
    }
}
