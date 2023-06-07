// This is a mock implementation of the async_io module for those platforms that don't support io_uring.
pub struct UringBufferedReader;

impl UringBufferedReader {
    pub fn new(_file: File, _raw_size: usize, _header_size: usize) -> OperationResult<Self> {
        Ok(Self {})
    }
}
