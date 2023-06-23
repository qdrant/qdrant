use std::fs::File;

use crate::entry::entry_point::OperationResult;

// This is a mock implementation of the async_io module for those platforms that don't support io_uring.
#[allow(dead_code)]
#[derive(Debug)]
pub struct UringReader;

#[allow(dead_code)]
impl UringReader {
    pub fn new(_file: File, _raw_size: usize, _header_size: usize) -> OperationResult<Self> {
        Ok(Self {})
    }
}
