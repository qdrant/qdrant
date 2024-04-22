use std::ffi::{c_void, CString};
use std::ptr;

use ash::extensions::ext::DebugUtils;
use ash::vk;

use crate::DebugMessenger;

pub struct Instance {
    _entry: ash::Entry,
    pub vk_instance: ash::Instance,
    pub vk_physical_devices: Vec<vk::PhysicalDevice>,
    pub alloc: Option<vk::AllocationCallbacks>,
    pub layers: Vec<String>,
    pub extensions: Vec<String>,
    vk_debug_utils_loader: Option<ash::extensions::ext::DebugUtils>,
    vk_debug_messenger: vk::DebugUtilsMessengerEXT,
}

unsafe impl Send for Instance {}
unsafe impl Sync for Instance {}

#[derive(Debug)]
pub enum DeviceError {}

impl Instance {
    pub fn new(
        name: &str,
        debug_messenger: Option<&dyn DebugMessenger>,
        dump_api: bool,
    ) -> Result<Self, DeviceError> {
        unsafe {
            let entry = ash::Entry::load().unwrap();
            let app_name = CString::new(name).unwrap();
            let engine_name = CString::new(name).unwrap();
            let app_info = vk::ApplicationInfo {
                s_type: vk::StructureType::APPLICATION_INFO,
                p_next: ptr::null(),
                p_application_name: app_name.as_ptr(),
                application_version: 0,
                p_engine_name: engine_name.as_ptr(),
                engine_version: 0,
                api_version: vk::make_api_version(0, 1, 3, 0),
            };

            let extensions = Self::get_extensions_list(debug_messenger.is_some());
            let extensions_cstr: Vec<CString> = extensions
                .iter()
                .map(|s| CString::new(s.clone().into_bytes()).unwrap())
                .collect();
            let extension_names_raw: Vec<*const i8> = extensions_cstr
                .iter()
                .map(|raw_name| raw_name.as_ptr())
                .collect();

            let layers = Self::get_layers_list(debug_messenger.is_some(), dump_api);
            let layers_cstr: Vec<CString> = layers
                .iter()
                .map(|s| CString::new(s.clone().into_bytes()).unwrap())
                .collect();
            let layers_raw: Vec<*const i8> = layers_cstr
                .iter()
                .map(|raw_name| raw_name.as_ptr())
                .collect();

            let debug_utils_create_info = debug_messenger.map(Self::debug_messenger_create_info);
            let create_info_p_next = if let Some(debug_utils_create_info) = &debug_utils_create_info
            {
                debug_utils_create_info as *const vk::DebugUtilsMessengerCreateInfoEXT
                    as *const c_void
            } else {
                ptr::null()
            };

            let create_flags = if cfg!(any(target_os = "macos", target_os = "ios")) {
                vk::InstanceCreateFlags::ENUMERATE_PORTABILITY_KHR
            } else {
                vk::InstanceCreateFlags::default()
            };

            let create_info = vk::InstanceCreateInfo {
                s_type: vk::StructureType::INSTANCE_CREATE_INFO,
                p_next: create_info_p_next,
                flags: create_flags,
                p_application_info: &app_info,
                pp_enabled_layer_names: layers_raw.as_ptr(),
                enabled_layer_count: layers_raw.len() as u32,
                pp_enabled_extension_names: extension_names_raw.as_ptr(),
                enabled_extension_count: extension_names_raw.len() as u32,
            };

            let alloc = None;
            let vk_instance: ash::Instance = entry
                .create_instance(&create_info, alloc.as_ref())
                .expect("Failed to create instance!");

            let (vk_debug_utils_loader, vk_debug_messenger) =
                if let Some(debug_messenger) = debug_messenger {
                    let debug_utils_loader =
                        ash::extensions::ext::DebugUtils::new(&entry, &vk_instance);
                    let messenger_create_info = Self::debug_messenger_create_info(debug_messenger);
                    let utils_messenger = debug_utils_loader
                        .create_debug_utils_messenger(&messenger_create_info, alloc.as_ref())
                        .expect("Debug Utils Callback");
                    (Some(debug_utils_loader), utils_messenger)
                } else {
                    (None, vk::DebugUtilsMessengerEXT::null())
                };

            let vk_physical_devices = vk_instance.enumerate_physical_devices().unwrap();
            Ok(Self {
                _entry: entry,
                vk_instance,
                vk_physical_devices,
                alloc,
                layers,
                extensions,
                vk_debug_utils_loader,
                vk_debug_messenger,
            })
        }
    }

    fn debug_messenger_create_info(
        debug_messenger: &dyn DebugMessenger,
    ) -> vk::DebugUtilsMessengerCreateInfoEXT {
        vk::DebugUtilsMessengerCreateInfoEXT {
            s_type: vk::StructureType::DEBUG_UTILS_MESSENGER_CREATE_INFO_EXT,
            p_next: ptr::null(),
            flags: vk::DebugUtilsMessengerCreateFlagsEXT::empty(),
            message_severity: debug_messenger.get_severity_flags(),
            message_type: debug_messenger.get_message_type_flags(),
            pfn_user_callback: debug_messenger.get_callback(),
            p_user_data: ptr::null_mut(),
        }
    }

    pub fn is_validation_enable(&self) -> bool {
        self.vk_debug_utils_loader.is_some()
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
            extensions_list.push(DebugUtils::name().to_str().unwrap().to_string());
        }

        #[cfg(target_os = "macos")]
        {
            extensions_list.push(
                vk::KhrPortabilityEnumerationFn::name()
                    .to_str()
                    .unwrap()
                    .to_string(),
            );
            extensions_list.push(
                vk::KhrGetPhysicalDeviceProperties2Fn::name()
                    .to_str()
                    .unwrap()
                    .to_string(),
            );
        }
        extensions_list
    }
}

impl Drop for Instance {
    fn drop(&mut self) {
        unsafe {
            if let Some(loader) = &self.vk_debug_utils_loader {
                if self.vk_debug_messenger != vk::DebugUtilsMessengerEXT::null() {
                    loader.destroy_debug_utils_messenger(
                        self.vk_debug_messenger,
                        self.alloc.as_ref(),
                    );
                }
            }
            self.vk_instance.destroy_instance(self.alloc.as_ref());
        }
    }
}
