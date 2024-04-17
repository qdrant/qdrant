use std::borrow::Cow;

#[cfg(feature = "f16")]
use half::f16;
use serde::{Deserialize, Serialize};

use crate::common::operation_error::OperationResult;
use crate::data_types::named_vectors::CowVector;
use crate::data_types::vectors::{VectorElementType, VectorElementTypeByte, VectorRef};

pub trait PrimitiveVectorElement:
    Copy + Clone + Default + Serialize + for<'a> Deserialize<'a>
{
    fn from_vector_ref(vector: VectorRef) -> OperationResult<Cow<[Self]>>;

    fn vector_to_cow(vector: &[Self]) -> CowVector;
}
impl PrimitiveVectorElement for VectorElementType {
    fn from_vector_ref(vector: VectorRef) -> OperationResult<Cow<[Self]>> {
        let vector_ref: &[Self] = vector.try_into()?;
        Ok(Cow::from(vector_ref))
    }

    fn vector_to_cow(vector: &[Self]) -> CowVector {
        vector.into()
    }
}

#[cfg(feature = "f16")]
impl PrimitiveVectorElement for f16 {
    fn from_vector_ref(vector: VectorRef) -> OperationResult<Cow<[Self]>> {
        use half::vec::HalfFloatVecExt;

        let vector_ref: &[VectorElementType] = vector.try_into()?;
        let f16_vector = Vec::from_f32_slice(vector_ref);
        Ok(Cow::from(f16_vector))
    }

    fn vector_to_cow(vector: &[Self]) -> CowVector {
        use half::slice::HalfFloatSliceExt;

        vector.to_f32_vec().into()
    }
}

impl PrimitiveVectorElement for VectorElementTypeByte {
    fn from_vector_ref(vector: VectorRef) -> OperationResult<Cow<[Self]>> {
        let vector_ref: &[VectorElementType] = vector.try_into()?;
        let byte_vector = vector_ref.iter().map(|&x| x as u8).collect::<Vec<u8>>();
        Ok(Cow::from(byte_vector))
    }

    fn vector_to_cow(vector: &[Self]) -> CowVector {
        vector
            .iter()
            .map(|&x| x as VectorElementType)
            .collect::<Vec<VectorElementType>>()
            .into()
    }
}
