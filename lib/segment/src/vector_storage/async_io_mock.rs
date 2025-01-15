#![allow(dead_code)] // The mock is unused on Linux, and so produces dead code warnings

use std::fs::File;

use crate::common::operation_error::OperationResult;
use crate::data_types::primitive::PrimitiveVectorElement;

// This is a mock implementation of the async_io module for those platforms that don't support io_uring.
#[derive(Debug)]
pub struct UringReader<T: PrimitiveVectorElement> {
    _phantom: std::marker::PhantomData<T>,
}

#[allow(clippy::unnecessary_wraps)] // Mock `new` have to follow the same signature as real `UringReader`
impl<T: PrimitiveVectorElement> UringReader<T> {
    pub fn new(_file: File, _raw_size: usize, _header_size: usize) -> OperationResult<Self> {
        Ok(Self {
            _phantom: std::marker::PhantomData,
        })
    }
}
