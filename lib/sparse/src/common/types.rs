use std::fmt::Debug;

use half::slice::HalfFloatSliceExt;
use itertools::{Itertools, MinMaxResult};

pub type DimOffset = u32;
pub type DimId = u32;
pub type DimId64 = u64;
pub type DimWeight = f32;

pub trait Weight: PartialEq + Copy + Debug + 'static {
    type QuantizationParams: Copy + PartialEq + Debug;

    fn quantization_params_for(
        values: impl ExactSizeIterator<Item = DimWeight> + Clone,
    ) -> Self::QuantizationParams;

    fn from_f32(params: Self::QuantizationParams, value: f32) -> Self;

    fn to_f32(self, params: Self::QuantizationParams) -> f32;

    fn into_f32_slice<'a>(
        params: Self::QuantizationParams,
        weights: &'a [Self],
        buffer: &'a mut [f32],
    ) -> &'a [f32];
}

impl Weight for f32 {
    type QuantizationParams = ();

    #[inline]
    fn quantization_params_for(_values: impl ExactSizeIterator<Item = DimWeight> + Clone) {}

    #[inline]
    fn from_f32(_: (), value: f32) -> Self {
        value
    }

    #[inline]
    fn to_f32(self, _: ()) -> f32 {
        self
    }

    #[inline]
    fn into_f32_slice<'a>(_: (), weights: &'a [Self], _buffer: &'a mut [f32]) -> &'a [f32] {
        // Zero-copy conversion, ignore buffer
        weights
    }
}

impl Weight for half::f16 {
    type QuantizationParams = ();

    #[inline]
    fn quantization_params_for(_values: impl ExactSizeIterator<Item = DimWeight> + Clone) {}

    #[inline]
    fn from_f32(_: (), value: f32) -> Self {
        half::f16::from_f32(value)
    }

    fn to_f32(self, _: ()) -> f32 {
        self.to_f32()
    }

    #[inline]
    fn into_f32_slice<'a>(_: (), weights: &'a [Self], buffer: &'a mut [f32]) -> &'a [f32] {
        weights.convert_to_f32_slice(buffer);
        buffer
    }
}

#[cfg(feature = "testing")]
impl Weight for u8 {
    type QuantizationParams = ();

    #[inline]
    fn quantization_params_for(_values: impl ExactSizeIterator<Item = DimWeight> + Clone) {}

    #[inline]
    fn from_f32(_: (), value: f32) -> Self {
        value as u8
    }

    #[inline]
    fn to_f32(self, _: ()) -> f32 {
        f32::from(self)
    }

    #[inline]
    fn into_f32_slice<'a>(_: (), weights: &'a [Self], buffer: &'a mut [f32]) -> &'a [f32] {
        for (i, &weight) in weights.iter().enumerate() {
            buffer[i] = f32::from(weight);
        }
        buffer
    }
}

#[derive(PartialEq, Copy, Clone, Debug)]
pub struct QuantizedU8(u8);

impl From<QuantizedU8> for DimWeight {
    fn from(val: QuantizedU8) -> Self {
        f32::from(val.0)
    }
}

#[derive(PartialEq, Default, Copy, Clone, Debug)]
pub struct QuantizedU8Params {
    /// Minimum value in the range
    min: f32,
    /// Difference divided by 256, aka `(max - min) / 255`
    diff256: f32,
}

impl Weight for QuantizedU8 {
    type QuantizationParams = QuantizedU8Params;

    #[inline]
    fn quantization_params_for(
        values: impl Iterator<Item = DimWeight>,
    ) -> Self::QuantizationParams {
        let (min, max) = match values.minmax() {
            MinMaxResult::NoElements => return QuantizedU8Params::default(),
            MinMaxResult::OneElement(e) => (e, e),
            MinMaxResult::MinMax(min, max) => (min, max),
        };
        QuantizedU8Params {
            min,
            diff256: (max - min) / 255.0,
        }
    }

    #[inline]
    fn from_f32(params: QuantizedU8Params, value: f32) -> Self {
        QuantizedU8(
            ((value - params.min) / params.diff256)
                .round()
                .clamp(0.0, 255.0) as u8,
        )
    }

    #[inline]
    fn to_f32(self, params: QuantizedU8Params) -> f32 {
        params.min + f32::from(self.0) * params.diff256
    }

    #[inline]
    fn into_f32_slice<'a>(
        params: QuantizedU8Params,
        weights: &'a [Self],
        buffer: &'a mut [f32],
    ) -> &'a [f32] {
        assert_eq!(weights.len(), buffer.len());
        for (i, &weight) in weights.iter().enumerate() {
            buffer[i] = weight.to_f32(params);
        }
        buffer
    }
}
