//! Cooperative matrix configurations (`VK_KHR_cooperative_matrix`), filtered to the ones the
//! device, as created, can use in a shader.

use ash::vk;

/// Component type of a cooperative matrix that a device, as created, can use in a shader.
///
/// Only the types whose features the crate enables are here. A reported configuration with any
/// other type is left out of `Device::cooperative_matrix_properties`, so no variant for unknown
/// types is needed:
/// - `FLOAT64` needs `shaderFloat64`, `SINT16` and `UINT16` need `shaderInt16`: never enabled.
/// - Every type beyond the 11 base types of `VK_KHR_cooperative_matrix` (bf16, the 8-, 6-, and
///   4-bit floats, the packed types of `VK_NV_cooperative_vector`) comes with an extension of
///   its own, which the crate doesn't enable. Types newer than this code are treated the same.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MatrixComponentType {
    /// 16-bit float. Needs `shaderFloat16`: only on a device with `Device::has_half_precision`.
    Float16,
    /// 32-bit float. Core.
    Float32,
    /// Signed 8-bit integer. Needs `shaderInt8` and `storageBuffer8BitAccess`, which every device
    /// of this crate has.
    Sint8,
    /// Unsigned 8-bit integer. Needs the same as `Sint8`.
    Uint8,
    /// Signed 32-bit integer. Core.
    Sint32,
    /// Unsigned 32-bit integer. Core.
    Uint32,
    /// Signed 64-bit integer. Needs `shaderInt64`: only on a device with `Device::has_shader_int64`.
    Sint64,
    /// Unsigned 64-bit integer. Needs the same as `Sint64`.
    Uint64,
}

/// Shader types a device is created with, beyond the ones every device of this crate has.
pub(crate) struct EnabledShaderTypes {
    /// `shaderFloat16`.
    pub float16: bool,
    /// `shaderInt64`.
    pub int64: bool,
}

impl MatrixComponentType {
    /// `raw` as this crate names it, if a device created with `enabled` can use it.
    fn usable(raw: vk::ComponentTypeKHR, enabled: &EnabledShaderTypes) -> Option<Self> {
        match raw {
            vk::ComponentTypeKHR::FLOAT16 => enabled.float16.then_some(Self::Float16),
            vk::ComponentTypeKHR::FLOAT32 => Some(Self::Float32),
            vk::ComponentTypeKHR::SINT8 => Some(Self::Sint8),
            vk::ComponentTypeKHR::UINT8 => Some(Self::Uint8),
            vk::ComponentTypeKHR::SINT32 => Some(Self::Sint32),
            vk::ComponentTypeKHR::UINT32 => Some(Self::Uint32),
            vk::ComponentTypeKHR::SINT64 => enabled.int64.then_some(Self::Sint64),
            vk::ComponentTypeKHR::UINT64 => enabled.int64.then_some(Self::Uint64),
            // `FLOAT64` (`shaderFloat64`), `SINT16` and `UINT16` (`shaderInt16`), and the types of
            // other extensions: never enabled by this crate.
            _ => None,
        }
    }
}

/// One cooperative matrix configuration a device, as created, can use in a shader: the product
/// `A (M x K) * B (K x N) + C (M x N)`, from `vkGetPhysicalDeviceCooperativeMatrixPropertiesKHR`.
///
/// A reported configuration is kept only if:
/// - its scope is subgroup (`gl_ScopeSubgroup`). Workgroup scope needs
///   `cooperativeMatrixWorkgroupScope` of `VK_NV_cooperative_matrix2`, which the crate doesn't
///   enable, so every configuration here has subgroup scope;
/// - each of its four component types is a `MatrixComponentType` the device can use, see there.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CooperativeMatrixProperties {
    /// Rows of A, C, and the result.
    pub m_size: u32,
    /// Columns of B, C, and the result.
    pub n_size: u32,
    /// Columns of A and rows of B.
    pub k_size: u32,
    pub a_type: MatrixComponentType,
    pub b_type: MatrixComponentType,
    pub c_type: MatrixComponentType,
    pub result_type: MatrixComponentType,
    /// Whether the multiply-add must saturate (`saturatingAccumulation`).
    pub saturating_accumulation: bool,
}

impl CooperativeMatrixProperties {
    /// `properties` as this crate describes it, if a device created with `enabled` can use it.
    pub(crate) fn usable(
        properties: &vk::CooperativeMatrixPropertiesKHR<'_>,
        enabled: &EnabledShaderTypes,
    ) -> Option<Self> {
        let vk::CooperativeMatrixPropertiesKHR {
            s_type: _,
            p_next: _,
            m_size,
            n_size,
            k_size,
            a_type,
            b_type,
            c_type,
            result_type,
            saturating_accumulation,
            scope,
            _marker: _,
        } = *properties;
        if scope != vk::ScopeKHR::SUBGROUP {
            return None;
        }
        Some(Self {
            m_size,
            n_size,
            k_size,
            a_type: MatrixComponentType::usable(a_type, enabled)?,
            b_type: MatrixComponentType::usable(b_type, enabled)?,
            c_type: MatrixComponentType::usable(c_type, enabled)?,
            result_type: MatrixComponentType::usable(result_type, enabled)?,
            saturating_accumulation: saturating_accumulation == vk::TRUE,
        })
    }
}
