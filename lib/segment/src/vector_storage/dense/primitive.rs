use serde::{Deserialize, Serialize};
use crate::common::operation_error::OperationResult;
use crate::data_types::named_vectors::CowVector;
use crate::data_types::vectors::{VectorElementType, VectorRef};

pub trait PrimitiveVectorElement:
Copy + Clone + Default + Serialize + for<'a> Deserialize<'a>
{
    fn from_vector_ref(vector: VectorRef) -> OperationResult<&[Self]>;

    fn vector_to_cow(vector: &[Self]) -> CowVector;
}
impl PrimitiveVectorElement for VectorElementType {
    fn from_vector_ref(vector: VectorRef) -> OperationResult<&[Self]> {
        vector.try_into()
    }

    fn vector_to_cow(vector: &[Self]) -> CowVector {
        vector.into()
    }
}
