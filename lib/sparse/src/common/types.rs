use std::fmt::Debug;

use half::slice::HalfFloatSliceExt;

pub type DimOffset = u32;
pub type DimId = u32;
pub type DimWeight = f32;

pub trait Weight: PartialEq + Copy + Debug + Into<DimWeight> + 'static {
    fn from_f32(value: f32) -> Self;

    fn into_f32_slice<'a>(weights: &'a [Self], buffer: &'a mut [f32]) -> &'a [f32];
}

impl Weight for f32 {
    #[inline]
    fn from_f32(value: f32) -> Self {
        value
    }

    #[inline]
    fn into_f32_slice<'a>(weights: &'a [Self], _buffer: &'a mut [f32]) -> &'a [f32] {
        // Zero-copy conversion, ignore buffer
        weights
    }
}

impl Weight for half::f16 {
    #[inline]
    fn from_f32(value: f32) -> Self {
        half::f16::from_f32(value)
    }

    #[inline]
    fn into_f32_slice<'a>(weights: &'a [Self], buffer: &'a mut [f32]) -> &'a [f32] {
        weights.convert_to_f32_slice(buffer);
        buffer
    }
}
