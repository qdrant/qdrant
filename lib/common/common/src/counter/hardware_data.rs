/// Contains all hardware metrics. Only serves as value holding structure without any semantics.
#[derive(Copy, Clone)]
pub struct HardwareData {
    pub cpu: usize,
    pub payload_io_read: usize,
    pub payload_io_write: usize,
    pub vector_io_read: usize,
    pub vector_io_write: usize,
    pub payload_index_io_read: usize,
}
