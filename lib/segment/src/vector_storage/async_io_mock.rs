use std::fs::File;

use crate::common::operation_error::OperationResult;
use crate::vector_storage::primitive::PrimitiveVectorElement;

// This is a mock implementation of the async_io module for those platforms that don't support io_uring.
#[allow(dead_code)]
pub struct UringReader<T: PrimitiveVectorElement> {
    _phantom: std::marker::PhantomData<T>,
}

#[allow(dead_code)]
impl<T: PrimitiveVectorElement> UringReader<T> {
    pub fn new(_file: File, _raw_size: usize, _header_size: usize) -> OperationResult<Self> {
        Ok(Self {
            _phantom: std::marker::PhantomData,
        })
    }
}
