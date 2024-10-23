use std::ffi::CString;
use std::sync::Arc;

use ash::extensions::ext::DebugUtils;
use ash::vk;

use crate::*;

static APPLICATION_NAME: &std::ffi::CStr = c"qdrant";

/// `Instance` is a Vulkan instance wrapper.
/// It's a root structure for all Vulkan operations and provides access the API.
/// It also manages all Vulkan API layers and API extensions.
pub struct Instance {
    /// Vulkan API entry point. It should be kept alive while the instance is alive.
    _entry: ash::Entry,

    /// Native Vulkan instance handle.
    pub vk_instance: ash::Instance,

    /// List of physical devices found by the Vulkan instance.
    pub vk_physical_devices: Vec<PhysicalDevice>,

    /// CPU allocator.
    pub allocation_callbacks: Option<Box<dyn AllocationCallbacks>>,

    /// List of Vulkan API layers that are enabled.
    pub layers: Vec<String>,

    /// List of Vulkan API extensions that are enabled.
    pub extensions: Vec<String>,

    /// Validation layer handler.
    vk_debug_utils_loader: Option<ash::extensions::ext::DebugUtils>,

    /// Validation layer messenger.
    vk_debug_messenger: vk::DebugUtilsMessengerEXT,

    /// Shader compiler.
    pub compiler: shaderc::Compiler,
}

#[derive(Clone)]
pub struct PhysicalDevice {
    pub vk_physical_device: vk::PhysicalDevice,
    pub name: String,
}

impl Instance {
    pub fn new(
        debug_messenger: Option<&dyn DebugMessenger>,
        allocation_callbacks: Option<Box<dyn AllocationCallbacks>>,
        dump_api: bool,
    ) -> GpuResult<Arc<Self>> {
        // Create a shader compiler before we start.
        // It's used to compile GLSL into SPIR-V.
        let compiler = shaderc::Compiler::new()
            .ok_or_else(|| GpuError::Other("Failed to create shaderc compiler".to_string()))?;

        // Create Vulkan API entry point.
        let entry = unsafe {
            ash::Entry::load().map_err(|e| {
                GpuError::Other(format!("Failed to create Vulkan API entry point {:?}", e))
            })?
        };

        // Collect Vulkan application info.
        // It contains application name and required Vulkan API version.
        let app_info = vk::ApplicationInfo::builder()
            .application_name(APPLICATION_NAME)
            .application_version(0)
            .engine_name(APPLICATION_NAME)
            .engine_version(0)
            .api_version(vk::make_api_version(0, 1, 3, 0))
            .build();

        // Collect Vulkan API extensions and convert it in raw pointers.
        let extensions = Self::get_extensions_list(debug_messenger.is_some());
        let extensions_cstr: Vec<CString> = extensions
            .iter()
            .filter_map(|s| CString::new(s.clone().into_bytes()).ok())
            .collect();
        let extension_names_raw: Vec<*const i8> = extensions_cstr
            .iter()
            .map(|raw_name| raw_name.as_ptr())
            .collect();

        // Collect Vulkan API layers and convert it in raw pointers.
        let layers = Self::get_layers_list(debug_messenger.is_some(), dump_api);
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
        let mut create_info_builder = vk::InstanceCreateInfo::builder()
            .flags(create_flags)
            .application_info(&app_info)
            .enabled_layer_names(&layers_raw)
            .enabled_extension_names(&extension_names_raw);

        if let Some(debug_utils_create_info) = &mut debug_utils_create_info {
            create_info_builder = create_info_builder.push_next(debug_utils_create_info);
        }

        let create_info = create_info_builder.build();

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
            .map(|vk_physical_device| {
                let device_properties =
                    unsafe { vk_instance.get_physical_device_properties(*vk_physical_device) };
                let device_name =
                    unsafe { ::std::ffi::CStr::from_ptr(device_properties.device_name.as_ptr()) };
                let device_name = device_name.to_str().unwrap_or("Unnamed GPU").to_owned();

                log::info!("Foung GPU device: {device_name}");
                PhysicalDevice {
                    vk_physical_device: *vk_physical_device,
                    name: device_name,
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
            let debug_utils_loader = ash::extensions::ext::DebugUtils::new(&entry, &vk_instance);
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
            layers,
            extensions,
            vk_debug_utils_loader,
            vk_debug_messenger,
            compiler,
        }))
    }

    fn debug_messenger_create_info(
        debug_messenger: &dyn DebugMessenger,
    ) -> vk::DebugUtilsMessengerCreateInfoEXT {
        vk::DebugUtilsMessengerCreateInfoEXT::builder()
            .flags(vk::DebugUtilsMessengerCreateFlagsEXT::empty())
            .message_severity(debug_messenger.get_severity_flags())
            .message_type(debug_messenger.get_message_type_flags())
            .pfn_user_callback(debug_messenger.get_callback())
            .build()
    }

    pub fn is_validation_enable(&self) -> bool {
        self.vk_debug_utils_loader.is_some()
    }

    pub fn cpu_allocation_callbacks(&self) -> Option<&vk::AllocationCallbacks> {
        self.allocation_callbacks
            .as_ref()
            .map(|alloc| alloc.allocation_callbacks())
    }

    fn get_layers_list(validation: bool, dump_api: bool) -> Vec<String> {
        let mut result = Vec::new();
        if validation {
            result.push("VK_LAYER_KHRONOS_validation".to_owned());
        }
        if dump_api {
            result.push("VK_LAYER_LUNARG_api_dump".to_owned());
        }
        result
    }

    fn get_extensions_list(validation: bool) -> Vec<String> {
        let mut extensions_list = Vec::new();
        if validation {
            if let Ok(ext) = DebugUtils::name().to_str() {
                extensions_list.push(ext.to_string());
            }
        }

        #[cfg(target_os = "macos")]
        {
            extensions_list.push(
                if let Ok(ext) = vk::KhrPortabilityEnumerationFn::name().to_str() {
                    extensions_list.push(ext.to_string());
                },
            );
            extensions_list.push(
                if let Ok(ext) = vk::KhrGetPhysicalDeviceProperties2Fn::name().to_str() {
                    extensions_list.push(ext.to_string());
                },
            );
        }
        extensions_list
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
