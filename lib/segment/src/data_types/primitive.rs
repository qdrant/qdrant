use std::borrow::Cow;

use serde::{Deserialize, Serialize};

use super::vectors::IntoDenseVector;
use crate::common::operation_error::OperationResult;
use crate::data_types::named_vectors::CowVector;
use crate::data_types::vectors::{
    FromVectorElementSlice, VectorElementType, VectorElementTypeByte, VectorRef,
};

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

impl PrimitiveVectorElement for VectorElementTypeByte {
    fn from_vector_ref(vector: VectorRef) -> OperationResult<Cow<[Self]>> {
        let vector_ref: &[VectorElementType] = vector.try_into()?;
        let byte_vector = Vec::<u8>::from_vector_element_slice(vector_ref);
        Ok(Cow::from(byte_vector))
    }

    fn vector_to_cow(vector: &[Self]) -> CowVector {
        vector.into_dense_vector().into()
    }
}
