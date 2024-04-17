use std::borrow::Cow;

use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::data_types::vectors::{VectorElementType, VectorElementTypeByte};
use crate::types::QuantizationConfig;

pub trait PrimitiveVectorElement:
    Copy + Clone + Default + Serialize + for<'a> Deserialize<'a>
{
    fn from_dense_vector(vector: &[VectorElementType]) -> Cow<[Self]>;

    fn slice_to_float_cow(vector: &[Self]) -> Cow<[VectorElementType]>;

    fn quantization_preprocess<'a>(
        quantization_config: &QuantizationConfig,
        vector: &'a [Self],
    ) -> Cow<'a, [f32]>;
}

impl PrimitiveVectorElement for VectorElementType {
    fn from_dense_vector(vector: &[VectorElementType]) -> Cow<[Self]> {
        Cow::Borrowed(vector)
    }

    fn slice_to_float_cow(vector: &[Self]) -> Cow<[VectorElementType]> {
        vector.into()
    }

    fn quantization_preprocess<'a>(
        _quantization_config: &QuantizationConfig,
        vector: &'a [Self],
    ) -> Cow<'a, [f32]> {
        vector.into()
    }
}

impl PrimitiveVectorElement for VectorElementTypeByte {
    fn from_dense_vector(vector: &[VectorElementType]) -> Cow<[Self]> {
        Cow::Owned(vector.iter().map(|&x| x as u8).collect())
    }

    fn slice_to_float_cow(vector: &[Self]) -> Cow<[VectorElementType]> {
        Cow::from(vector.iter().map(|&x| x as VectorElementType).collect_vec())
    }

    fn quantization_preprocess<'a>(
        quantization_config: &QuantizationConfig,
        vector: &'a [Self],
    ) -> Cow<'a, [f32]> {
        if let QuantizationConfig::Binary(_) = quantization_config {
            Cow::from(
                vector
                    .iter()
                    .map(|&x| (x as VectorElementType) - 127.0)
                    .collect_vec(),
            )
        } else {
            Cow::from(vector.iter().map(|&x| x as VectorElementType).collect_vec())
        }
    }
}
