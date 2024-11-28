use std::collections::HashMap;
use std::ffi::CString;
use std::sync::Arc;

use ash::vk;
use parking_lot::Mutex;

use crate::*;

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

    /// Shader compiler.
    compiler: Mutex<shaderc::Compiler>,
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

impl Instance {
    pub fn new(
        debug_messenger: Option<&dyn DebugMessenger>,
        allocation_callbacks: Option<Box<dyn AllocationCallbacks>>,
        dump_api: bool,
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
                GpuError::Other(format!("Failed to create Vulkan API entry point {:?}", e))
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
        let extension_names_raw: Vec<*const i8> = extensions_cstr
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
        let layers_raw: Vec<*const i8> = layers_cstr
            .iter()
            .map(|raw_name| raw_name.as_ptr())
            .collect();

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

                log::info!("Foung GPU device: {device_name}");
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
                    Err(format!("Include file not found: {}", filename))
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
            .map_err(|e| GpuError::Other(format!("Failed to compile shader: {:?}", e)))?;
        Ok(result.as_binary_u8().to_owned())
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
        if validation {
            if let Ok(ext) = ash::ext::debug_utils::NAME.to_str() {
                extensions_list.push(ext.to_string());
            }
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
                return Err(GpuError::Other(format!(
                    "Extension {} not found",
                    extension
                )));
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
                return Err(GpuError::Other(format!("Layer {} not found", layer)));
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
