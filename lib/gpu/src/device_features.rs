use std::ffi::CStr;

use ash::vk;

use crate::cooperative_matrix::EnabledShaderTypes;
use crate::memory_budget::MemoryBudgetSupport;
use crate::*;

/// Optional device features. Each is enabled where the physical device supports it and is never
/// required: a device without it is created as before, and its getter on `Device` says it's off.
pub(crate) struct OptionalFeatures {
    /// `shaderIntegerDotProduct` (Vulkan 1.3, from `VK_KHR_shader_integer_dot_product`),
    /// with the device's acceleration properties.
    pub integer_dot_product: Option<IntegerDotProductProperties>,

    /// `VK_KHR_cooperative_matrix`, with the configurations the device as created can use
    /// (never empty). Enabled only together with the Vulkan memory model, which shaders using
    /// cooperative matrices need, only if the device supports them in compute shaders, and only
    /// if at least one reported configuration is usable.
    pub cooperative_matrix: Option<Vec<CooperativeMatrixProperties>>,

    /// `bufferDeviceAddress` (Vulkan 1.2).
    pub buffer_device_address: bool,

    /// `shaderInt64` (Vulkan 1.0), for 64-bit address arithmetic in shaders.
    pub shader_int64: bool,

    /// `vulkanMemoryModel` and `vulkanMemoryModelDeviceScope` (Vulkan 1.2), enabled as a pair.
    pub vulkan_memory_model: bool,

    /// `VK_EXT_memory_budget`.
    pub memory_budget: MemoryBudgetSupport,
}

/// Which 8-bit integer dot products the device accelerates, as reported in
/// `VkPhysicalDeviceShaderIntegerDotProductProperties`. "Accelerated" means the dot product
/// instruction is faster than the same arithmetic written out in the shader.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IntegerDotProductProperties {
    /// `integerDotProduct8BitUnsignedAccelerated`.
    pub dot_8bit_unsigned_accelerated: bool,
    /// `integerDotProduct8BitSignedAccelerated`.
    pub dot_8bit_signed_accelerated: bool,
    /// `integerDotProduct8BitMixedSignednessAccelerated`.
    pub dot_8bit_mixed_signedness_accelerated: bool,
    /// `integerDotProduct4x8BitPackedUnsignedAccelerated`.
    pub dot_4x8bit_packed_unsigned_accelerated: bool,
    /// `integerDotProduct4x8BitPackedSignedAccelerated`, the `dotPacked4x8EXT` path.
    pub dot_4x8bit_packed_signed_accelerated: bool,
    /// `integerDotProduct4x8BitPackedMixedSignednessAccelerated`.
    pub dot_4x8bit_packed_mixed_signedness_accelerated: bool,
}

/// Whether the device is created with `shaderFloat16` (half precision).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HalfPrecision {
    Enabled,
    Disabled,
}

impl OptionalFeatures {
    /// Find which optional features `vk_physical_device` supports, for a device created with or
    /// without half precision.
    pub(crate) fn detect(
        instance: &Instance,
        vk_physical_device: vk::PhysicalDevice,
        available_extensions: &[vk::ExtensionProperties],
        half_precision: HalfPrecision,
    ) -> Self {
        let vk_instance = instance.vk_instance();
        let has_cooperative_matrix_extension =
            has_extension(available_extensions, ash::khr::cooperative_matrix::NAME);
        let has_memory_budget_extension =
            has_extension(available_extensions, ash::ext::memory_budget::NAME);

        // Supported features. Extension structures are chained only if the extension is there.
        let mut features_1_2 = vk::PhysicalDeviceVulkan12Features::default();
        let mut features_1_3 = vk::PhysicalDeviceVulkan13Features::default();
        let mut cooperative_matrix_features =
            vk::PhysicalDeviceCooperativeMatrixFeaturesKHR::default();
        let mut features = vk::PhysicalDeviceFeatures2::default()
            .push_next(&mut features_1_2)
            .push_next(&mut features_1_3);
        if has_cooperative_matrix_extension {
            features = features.push_next(&mut cooperative_matrix_features);
        }
        unsafe { vk_instance.get_physical_device_features2(vk_physical_device, &mut features) };
        let shader_int64 = features.features.shader_int64 == vk::TRUE;

        // Supported properties.
        let mut properties_1_3 = vk::PhysicalDeviceVulkan13Properties::default();
        let mut cooperative_matrix_properties =
            vk::PhysicalDeviceCooperativeMatrixPropertiesKHR::default();
        let mut properties =
            vk::PhysicalDeviceProperties2::default().push_next(&mut properties_1_3);
        if has_cooperative_matrix_extension {
            properties = properties.push_next(&mut cooperative_matrix_properties);
        }
        unsafe { vk_instance.get_physical_device_properties2(vk_physical_device, &mut properties) };

        let integer_dot_product = (features_1_3.shader_integer_dot_product == vk::TRUE).then_some(
            IntegerDotProductProperties {
                dot_8bit_unsigned_accelerated: properties_1_3
                    .integer_dot_product8_bit_unsigned_accelerated
                    == vk::TRUE,
                dot_8bit_signed_accelerated: properties_1_3
                    .integer_dot_product8_bit_signed_accelerated
                    == vk::TRUE,
                dot_8bit_mixed_signedness_accelerated: properties_1_3
                    .integer_dot_product8_bit_mixed_signedness_accelerated
                    == vk::TRUE,
                dot_4x8bit_packed_unsigned_accelerated: properties_1_3
                    .integer_dot_product4x8_bit_packed_unsigned_accelerated
                    == vk::TRUE,
                dot_4x8bit_packed_signed_accelerated: properties_1_3
                    .integer_dot_product4x8_bit_packed_signed_accelerated
                    == vk::TRUE,
                dot_4x8bit_packed_mixed_signedness_accelerated: properties_1_3
                    .integer_dot_product4x8_bit_packed_mixed_signedness_accelerated
                    == vk::TRUE,
            },
        );

        let vulkan_memory_model = features_1_2.vulkan_memory_model == vk::TRUE
            && features_1_2.vulkan_memory_model_device_scope == vk::TRUE;

        let cooperative_matrix = if has_cooperative_matrix_extension
            && cooperative_matrix_features.cooperative_matrix == vk::TRUE
            && cooperative_matrix_properties
                .cooperative_matrix_supported_stages
                .contains(vk::ShaderStageFlags::COMPUTE)
            && vulkan_memory_model
        {
            let loader = ash::khr::cooperative_matrix::Instance::new(instance.entry(), vk_instance);
            match unsafe {
                loader.get_physical_device_cooperative_matrix_properties(vk_physical_device)
            } {
                Ok(configurations) => {
                    let enabled = EnabledShaderTypes {
                        float16: match half_precision {
                            HalfPrecision::Enabled => true,
                            HalfPrecision::Disabled => false,
                        },
                        int64: shader_int64,
                    };
                    let usable: Vec<_> = configurations
                        .iter()
                        .filter_map(|configuration| {
                            CooperativeMatrixProperties::usable(configuration, &enabled)
                        })
                        .collect();
                    // Without a usable configuration, cooperative matrices aren't enabled.
                    (!usable.is_empty()).then_some(usable)
                }
                Err(err) => {
                    log::warn!(
                        "Failed to query cooperative matrix properties, cooperative matrices disabled: {err:?}"
                    );
                    None
                }
            }
        } else {
            None
        };

        Self {
            integer_dot_product,
            cooperative_matrix,
            buffer_device_address: features_1_2.buffer_device_address == vk::TRUE,
            shader_int64,
            vulkan_memory_model,
            memory_budget: if has_memory_budget_extension {
                MemoryBudgetSupport::Extension
            } else {
                MemoryBudgetSupport::HeapSizeOnly
            },
        }
    }

    /// Device extensions to enable for the detected features.
    pub(crate) fn extensions(&self) -> Vec<&'static CStr> {
        let Self {
            integer_dot_product: _, // Core in Vulkan 1.3.
            cooperative_matrix,
            buffer_device_address: _, // Core in Vulkan 1.2.
            shader_int64: _,          // Core in Vulkan 1.0.
            vulkan_memory_model: _,   // Core in Vulkan 1.2.
            memory_budget,
        } = self;
        let mut extensions = Vec::new();
        if cooperative_matrix.is_some() {
            extensions.push(ash::khr::cooperative_matrix::NAME);
        }
        match memory_budget {
            MemoryBudgetSupport::Extension => extensions.push(ash::ext::memory_budget::NAME),
            MemoryBudgetSupport::HeapSizeOnly => {}
        }
        extensions
    }
}

/// Whether `name` is among the extensions a physical device reports.
pub(crate) fn has_extension(available_extensions: &[vk::ExtensionProperties], name: &CStr) -> bool {
    available_extensions
        .iter()
        .any(|extension| extension.extension_name_as_c_str() == Ok(name))
}
