use std::borrow::Cow;

use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::common::operation_error::OperationResult;
use crate::data_types::vectors::{VectorElementType, VectorElementTypeByte, VectorRef};
use crate::types::QuantizationConfig;

pub trait PrimitiveVectorElement:
    Copy + Clone + Default + Serialize + for<'a> Deserialize<'a>
{
    fn from_vector_ref(vector: VectorRef) -> OperationResult<Cow<[Self]>>;

    fn slice_to_float_cow(vector: &[Self]) -> Cow<[f32]>;

    fn quantization_preprocess<'a>(
        quantization_config: &QuantizationConfig,
        vector: &'a [Self],
    ) -> Cow<'a, [f32]>;
}

impl PrimitiveVectorElement for VectorElementType {
    fn from_vector_ref(vector: VectorRef) -> OperationResult<Cow<[Self]>> {
        let vector_ref: &[Self] = vector.try_into()?;
        Ok(Cow::from(vector_ref))
    }

    fn slice_to_float_cow(vector: &[Self]) -> Cow<[f32]> {
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
    fn from_vector_ref(vector: VectorRef) -> OperationResult<Cow<[Self]>> {
        let vector_ref: &[VectorElementType] = vector.try_into()?;
        let byte_vector = vector_ref.iter().map(|&x| x as u8).collect::<Vec<u8>>();
        Ok(Cow::from(byte_vector))
    }

    fn slice_to_float_cow(vector: &[Self]) -> Cow<[f32]> {
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
