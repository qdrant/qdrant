use std::ffi::CStr;
use std::os::raw::c_void;

use ash::vk;

pub trait DebugMessenger {
    fn get_callback(&self) -> vk::PFN_vkDebugUtilsMessengerCallbackEXT;

    fn get_severity_flags(&self) -> vk::DebugUtilsMessageSeverityFlagsEXT;

    fn get_message_type_flags(&self) -> vk::DebugUtilsMessageTypeFlagsEXT;
}

pub struct LogAllMessenger {}

impl DebugMessenger for LogAllMessenger {
    fn get_callback(&self) -> vk::PFN_vkDebugUtilsMessengerCallbackEXT {
        Some(vulkan_debug_callback_log)
    }

    fn get_severity_flags(&self) -> vk::DebugUtilsMessageSeverityFlagsEXT {
        vk::DebugUtilsMessageSeverityFlagsEXT::WARNING
            | vk::DebugUtilsMessageSeverityFlagsEXT::VERBOSE
            | vk::DebugUtilsMessageSeverityFlagsEXT::INFO
            | vk::DebugUtilsMessageSeverityFlagsEXT::ERROR
    }

    fn get_message_type_flags(&self) -> vk::DebugUtilsMessageTypeFlagsEXT {
        vk::DebugUtilsMessageTypeFlagsEXT::GENERAL
            | vk::DebugUtilsMessageTypeFlagsEXT::PERFORMANCE
            | vk::DebugUtilsMessageTypeFlagsEXT::VALIDATION
    }
}

unsafe extern "system" fn vulkan_debug_callback_log(
    message_severity: vk::DebugUtilsMessageSeverityFlagsEXT,
    message_type: vk::DebugUtilsMessageTypeFlagsEXT,
    p_callback_data: *const vk::DebugUtilsMessengerCallbackDataEXT,
    _p_user_data: *mut c_void,
) -> vk::Bool32 {
    let message = CStr::from_ptr((*p_callback_data).p_message);
    let message_type_str = match message_type {
        vk::DebugUtilsMessageTypeFlagsEXT::GENERAL => "[General]",
        vk::DebugUtilsMessageTypeFlagsEXT::PERFORMANCE => "[Performance]",
        vk::DebugUtilsMessageTypeFlagsEXT::VALIDATION => "[Validation]",
        _ => "[Unknown]",
    };
    match message_severity {
        vk::DebugUtilsMessageSeverityFlagsEXT::VERBOSE => {
            log::info!("{} {:?}", message_type_str, message)
        }
        vk::DebugUtilsMessageSeverityFlagsEXT::WARNING => {
            log::warn!("{} {:?}", message_type_str, message)
        }
        vk::DebugUtilsMessageSeverityFlagsEXT::ERROR => {
            log::error!("{} {:?}", message_type_str, message)
        }
        vk::DebugUtilsMessageSeverityFlagsEXT::INFO => {
            log::info!("{} {:?}", message_type_str, message)
        }
        _ => log::info!("{} {:?}", message_type_str, message),
    };
    vk::FALSE
}

pub struct PanicIfErrorMessenger {}

impl DebugMessenger for PanicIfErrorMessenger {
    fn get_callback(&self) -> vk::PFN_vkDebugUtilsMessengerCallbackEXT {
        Some(vulkan_debug_callback_panic)
    }

    fn get_severity_flags(&self) -> vk::DebugUtilsMessageSeverityFlagsEXT {
        vk::DebugUtilsMessageSeverityFlagsEXT::WARNING
            | vk::DebugUtilsMessageSeverityFlagsEXT::ERROR
    }

    fn get_message_type_flags(&self) -> vk::DebugUtilsMessageTypeFlagsEXT {
        vk::DebugUtilsMessageTypeFlagsEXT::GENERAL
            | vk::DebugUtilsMessageTypeFlagsEXT::PERFORMANCE
            | vk::DebugUtilsMessageTypeFlagsEXT::VALIDATION
    }
}

unsafe extern "system" fn vulkan_debug_callback_panic(
    message_severity: vk::DebugUtilsMessageSeverityFlagsEXT,
    message_type: vk::DebugUtilsMessageTypeFlagsEXT,
    p_callback_data: *const vk::DebugUtilsMessengerCallbackDataEXT,
    _p_user_data: *mut c_void,
) -> vk::Bool32 {
    if std::thread::panicking() {
        return vk::FALSE;
    }
    let message = CStr::from_ptr((*p_callback_data).p_message);
    let message = message.to_str().unwrap();
    let message_type = match message_type {
        vk::DebugUtilsMessageTypeFlagsEXT::GENERAL => "General",
        vk::DebugUtilsMessageTypeFlagsEXT::PERFORMANCE => "Performance",
        vk::DebugUtilsMessageTypeFlagsEXT::VALIDATION => "Validation",
        _ => "Unknown",
    };
    let severity = match message_severity {
        vk::DebugUtilsMessageSeverityFlagsEXT::WARNING => "warning",
        vk::DebugUtilsMessageSeverityFlagsEXT::ERROR => "error",
        _ => "info",
    };
    let backtrace = std::backtrace::Backtrace::force_capture().to_string();
    panic!(
        "Vulkan panic ({} {}) \nWith message: {}, \nBackrace: {}",
        message_type, severity, message, backtrace,
    )
}
