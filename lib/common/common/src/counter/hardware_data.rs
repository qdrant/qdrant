use std::ops::Add;

/// Contains all hardware metrics. Only serves as value holding structure without any semantics.
#[derive(Copy, Clone)]
pub struct HardwareData {
    pub cpu: usize,
    pub payload_io_read: usize,
    pub payload_io_write: usize,
    pub vector_io_read: usize,
    pub vector_io_write: usize,
    pub payload_index_io_read: usize,
    pub payload_index_io_write: usize,
}

impl Add for HardwareData {
    type Output = HardwareData;

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            cpu: self.cpu + rhs.cpu,
            payload_io_read: self.payload_io_read + rhs.payload_io_read,
            payload_io_write: self.payload_io_write + rhs.payload_io_write,
            vector_io_read: self.vector_io_read + rhs.vector_io_read,
            vector_io_write: self.vector_io_write + rhs.vector_io_write,
            payload_index_io_read: self.payload_index_io_read + rhs.payload_index_io_read,
            payload_index_io_write: self.payload_index_io_write + rhs.payload_index_io_write,
        }
    }
}
