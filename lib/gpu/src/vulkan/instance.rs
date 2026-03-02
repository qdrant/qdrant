use std::collections::HashMap;
use std::ffi::{CString, c_char};
use std::ops::Deref;
use std::sync::Arc;

use ash::vk;

use super::*;
use crate::{GpuError, GpuResult, Resource};

static APPLICATION_NAME: &std::ffi::CStr = c"qdrant";

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

    /// Debug messenger for the instance. It contains validation error callbacks.
    /// Should be kept alive while the instance is alive because it contains raw pointers to callbacks.
    _debug_messenger: Option<Box<dyn DebugMessenger>>,
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
pub struct InstanceBuilder {
    debug_messenger: Option<Box<dyn DebugMessenger>>,
    allocation_callbacks: Option<Box<dyn AllocationCallbacks>>,
    dump_api: bool,
}

impl InstanceBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set debug messenger for the instance.
    pub fn with_debug_messenger(mut self, debug_messenger: Box<dyn DebugMessenger>) -> Self {
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

    pub fn build(self) -> GpuResult<Arc<Instance>> {
        Instance::new_internal(
            self.debug_messenger,
            self.allocation_callbacks,
            self.dump_api,
        )
    }
}

impl Instance {
    pub fn builder() -> InstanceBuilder {
        InstanceBuilder::new()
    }

    pub(crate) fn new_internal(
        debug_messenger: Option<Box<dyn DebugMessenger>>,
        allocation_callbacks: Option<Box<dyn AllocationCallbacks>>,
        dump_api: bool,
    ) -> GpuResult<Arc<Self>> {
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
        let extensions_cstr: Vec<CString> = extensions
            .iter()
            .filter_map(|s| CString::new(s.clone().into_bytes()).ok())
            .collect();
        let extension_names_raw: Vec<*const c_char> = extensions_cstr
            .iter()
            .map(|raw_name| raw_name.as_ptr())
            .collect();

        // Collect Vulkan API layers and convert it in raw pointers.
        let layers = Self::layers_list(debug_messenger.is_some(), dump_api);
        // Check presence of all required layers.
        Self::check_layers_list(&entry, &layers)?;
        let layers_cstr: Vec<CString> = layers
            .iter()
            .filter_map(|s| CString::new(s.clone().into_bytes()).ok())
            .collect();
        let layers_raw: Vec<*const c_char> = layers_cstr
            .iter()
            .map(|raw_name| raw_name.as_ptr())
            .collect();

        // If we provide debug messenger, we need to create a debug messenger info.
        let mut debug_utils_create_info = debug_messenger
            .as_deref()
            .map(Self::debug_messenger_create_info);

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
            debug_messenger.as_ref()
        {
            let debug_utils_loader = ash::ext::debug_utils::Instance::new(&entry, &vk_instance);
            let messenger_create_info = Self::debug_messenger_create_info(debug_messenger.deref());
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
            _debug_messenger: debug_messenger,
        }))
    }

    fn debug_messenger_create_info(
        debug_messenger: &dyn DebugMessenger,
    ) -> vk::DebugUtilsMessengerCreateInfoEXT<'_> {
        vk::DebugUtilsMessengerCreateInfoEXT::default()
            .flags(vk::DebugUtilsMessengerCreateFlagsEXT::empty())
            .message_severity(debug_messenger.severity_flags())
            .message_type(debug_messenger.message_type_flags())
            .pfn_user_callback(debug_messenger.callback())
    }

    pub fn cpu_allocation_callbacks(&self) -> Option<&vk::AllocationCallbacks<'_>> {
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

    pub fn compile_shader(
        &self,
        shader: &str,
        shader_name: &str,
        defines: Option<&HashMap<String, Option<String>>>,
        includes: Option<&HashMap<String, String>>,
    ) -> GpuResult<Vec<u8>> {
        self.compile_shader_with_extra_args(shader, shader_name, defines, includes, &[])
    }

    pub fn compile_shader_with_extra_args(
        &self,
        shader: &str,
        shader_name: &str,
        defines: Option<&HashMap<String, Option<String>>>,
        includes: Option<&HashMap<String, String>>,
        extra_args: &[&str],
    ) -> GpuResult<Vec<u8>> {
        // Write include files to a unique temporary directory for Slang's search path resolution.
        // Use an atomic counter to ensure uniqueness across concurrent compilations.
        static COMPILE_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let counter = COMPILE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let include_dir =
            std::env::temp_dir().join(format!("qdrant_slang_{}_{counter}", std::process::id()));
        if let Some(includes) = includes {
            std::fs::create_dir_all(&include_dir).map_err(|e| {
                GpuError::Other(format!("Failed to create shader include directory: {e}"))
            })?;
            for (filename, source) in includes {
                std::fs::write(include_dir.join(filename), source).map_err(|e| {
                    GpuError::Other(format!("Failed to write shader include {filename}: {e}"))
                })?;
            }
        }

        let result =
            self.compile_shader_slang(shader, shader_name, defines, &include_dir, extra_args);

        // Clean up temp include files (best-effort).
        let _ = std::fs::remove_dir_all(&include_dir);

        result
    }

    fn compile_shader_slang(
        &self,
        shader: &str,
        shader_name: &str,
        defines: Option<&HashMap<String, Option<String>>>,
        include_dir: &std::path::Path,
        extra_args: &[&str],
    ) -> GpuResult<Vec<u8>> {
        // Defines are now baked into the generated config.slang module,
        // so no preprocessor injection is needed.
        let _ = defines; // Kept in signature for API compatibility.
        let shader_source = shader.to_string();

        // Write the shader source to a file for slangc.
        let module_name = shader_name
            .strip_suffix(".slang")
            .or_else(|| shader_name.strip_suffix(".comp"))
            .unwrap_or(shader_name);
        let shader_file_name = format!("{module_name}.slang");
        let shader_file_path = include_dir.join(&shader_file_name);

        // Ensure the directory exists (shader names may contain path components).
        if let Some(parent) = shader_file_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| GpuError::Other(format!("Failed to create shader directory: {e}")))?;
        }

        std::fs::write(&shader_file_path, &shader_source).map_err(|e| {
            GpuError::Other(format!("Failed to write shader source {shader_name}: {e}"))
        })?;

        // Compile GLSL to SPIR-V using the slangc command-line tool.
        // We use the CLI rather than the C API because the API's load_module
        // function doesn't properly preprocess macros in layout() qualifiers,
        // and loadModuleFromSourceString has COM reference counting issues.
        let output_path = include_dir.join(format!("{module_name}.spv"));
        if let Some(parent) = output_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| GpuError::Other(format!("Failed to create output directory: {e}")))?;
        }
        let mut cmd = std::process::Command::new(Self::slangc_path());
        cmd.arg(&shader_file_path)
            .arg("-target")
            .arg("spirv")
            .arg("-entry")
            .arg("main")
            .arg("-force-glsl-scalar-layout")
            .arg("-emit-spirv-directly")
            .arg("-O2")
            .arg("-I")
            .arg(include_dir)
            .arg("-o")
            .arg(&output_path);
        for arg in extra_args {
            cmd.arg(arg);
        }

        let output = cmd
            .output()
            .map_err(|e| GpuError::Other(format!("Failed to run slangc for {shader_name}: {e}")))?;

        // Check if slangc succeeded. Slang may emit warnings to stderr even on
        // success. We check the output file existence as a secondary signal —
        // if the file was written, the compilation succeeded despite any warnings.
        if !output.status.success() && !output_path.exists() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(GpuError::Other(format!(
                "Failed to compile shader {shader_name}: {stderr}"
            )));
        }

        let spirv_data = std::fs::read(&output_path).map_err(|e| {
            GpuError::Other(format!(
                "Failed to read compiled SPIR-V for {shader_name}: {e}"
            ))
        })?;

        Ok(spirv_data)
    }

    /// Get the path to the slangc compiler binary.
    fn slangc_path() -> &'static str {
        // Check SLANG_DIR env var first, then fall back to system PATH.
        static SLANGC_PATH: std::sync::OnceLock<String> = std::sync::OnceLock::new();
        SLANGC_PATH.get_or_init(|| {
            if let Ok(slang_dir) = std::env::var("SLANG_DIR") {
                let path = format!("{slang_dir}/bin/slangc");
                if std::path::Path::new(&path).exists() {
                    return path;
                }
                // On Windows, check with .exe extension.
                let path_exe = format!("{path}.exe");
                if std::path::Path::new(&path_exe).exists() {
                    return path_exe;
                }
            }
            "slangc".to_string()
        })
    }

    fn layers_list(validation: bool, dump_api: bool) -> Vec<String> {
        let mut result = Vec::new();
        if validation {
            result.push("VK_LAYER_KHRONOS_validation".to_owned());
        }
        if dump_api {
            result.push("VK_LAYER_LUNARG_api_dump".to_owned());
        }
        result
    }

    fn extensions_list(validation: bool) -> Vec<String> {
        let mut extensions_list = Vec::new();
        if validation && let Ok(ext) = ash::ext::debug_utils::NAME.to_str() {
            extensions_list.push(ext.to_string());
        }

        #[cfg(target_os = "macos")]
        {
            if let Ok(ext) = ash::khr::portability_enumeration::NAME.to_str() {
                extensions_list.push(ext.to_string());
            }
            if let Ok(ext) = ash::khr::get_physical_device_properties2::NAME.to_str() {
                extensions_list.push(ext.to_string());
            }
        }
        extensions_list
    }

    fn check_extensions_list(entry: &ash::Entry, extensions: &[String]) -> GpuResult<()> {
        let extension_properties = unsafe { entry.enumerate_instance_extension_properties(None)? };
        for extension in extensions {
            let extension_found = extension_properties.iter().any(|ep| {
                let name = unsafe { ::std::ffi::CStr::from_ptr(ep.extension_name.as_ptr()) };
                name.to_str().unwrap_or("") == extension
            });
            if !extension_found {
                return Err(GpuError::Other(format!("Extension {extension} not found")));
            }
        }
        Ok(())
    }

    fn check_layers_list(entry: &ash::Entry, layers: &[String]) -> GpuResult<()> {
        let layer_properties = unsafe { entry.enumerate_instance_layer_properties()? };
        for layer in layers {
            let layer_found = layer_properties.iter().any(|lp| {
                let name = unsafe { ::std::ffi::CStr::from_ptr(lp.layer_name.as_ptr()) };
                name.to_str().unwrap_or("") == layer
            });
            if !layer_found {
                return Err(GpuError::Other(format!("Layer {layer} not found")));
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
            if let Some(loader) = &self.vk_debug_utils_loader
                && self.vk_debug_messenger != vk::DebugUtilsMessengerEXT::null()
            {
                loader.destroy_debug_utils_messenger(self.vk_debug_messenger, allocation_callbacks);
            }

            // Last step after all drops of all GPU resources: destroy vulkan instance.
            self.vk_instance.destroy_instance(allocation_callbacks);
        }
    }
}
