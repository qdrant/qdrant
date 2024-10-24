use std::ffi::CStr;
use std::os::raw::c_void;

use ash::vk;

/// Trait for debug messenger.
/// Debug messenger is used to handle Vulkan debug messages.
/// If presented, vulkan instance will be created with validation layers and debug messenger.
/// Validation layer has a large performance cost, so it should be used only for tests and debugging.
pub trait DebugMessenger {
    fn callback(&self) -> vk::PFN_vkDebugUtilsMessengerCallbackEXT;

    fn severity_flags(&self) -> vk::DebugUtilsMessageSeverityFlagsEXT;

    fn message_type_flags(&self) -> vk::DebugUtilsMessageTypeFlagsEXT;
}

/// Log all messages from the Vulkan validation layer.
pub struct LogAllMessenger {}

impl DebugMessenger for LogAllMessenger {
    fn callback(&self) -> vk::PFN_vkDebugUtilsMessengerCallbackEXT {
        Some(vulkan_debug_callback_log)
    }

    fn severity_flags(&self) -> vk::DebugUtilsMessageSeverityFlagsEXT {
        vk::DebugUtilsMessageSeverityFlagsEXT::WARNING
            | vk::DebugUtilsMessageSeverityFlagsEXT::VERBOSE
            | vk::DebugUtilsMessageSeverityFlagsEXT::INFO
            | vk::DebugUtilsMessageSeverityFlagsEXT::ERROR
    }

    fn message_type_flags(&self) -> vk::DebugUtilsMessageTypeFlagsEXT {
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

/// Panic if some message from the Vulkan validation layer.
pub struct PanicIfErrorMessenger {}

impl DebugMessenger for PanicIfErrorMessenger {
    fn callback(&self) -> vk::PFN_vkDebugUtilsMessengerCallbackEXT {
        Some(vulkan_debug_callback_panic)
    }

    fn severity_flags(&self) -> vk::DebugUtilsMessageSeverityFlagsEXT {
        vk::DebugUtilsMessageSeverityFlagsEXT::ERROR
    }

    fn message_type_flags(&self) -> vk::DebugUtilsMessageTypeFlagsEXT {
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
        vk::DebugUtilsMessageSeverityFlagsEXT::ERROR => "error",
        _ => "info",
    };
    let backtrace = std::backtrace::Backtrace::force_capture().to_string();
    panic!(
        "Vulkan panic ({} {}) \nWith message: {}, \nBackrace: {}",
        message_type, severity, message, backtrace,
    )
}
