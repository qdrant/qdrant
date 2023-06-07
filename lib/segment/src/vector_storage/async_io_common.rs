use crate::types::PointOffsetType;

pub const DISK_PARALLELISM: usize = 16; // TODO: benchmark it better, or make it configurable

pub struct BufferStore {
    /// Stores the buffer for the point vectors
    pub buffers: Vec<Vec<u8>>,
    /// Stores the point ids that are currently being processed in each buffer.
    pub processing_ids: Vec<PointOffsetType>,
}

impl BufferStore {
    pub fn new(num_buffers: usize, buffer_raw_size: usize) -> Self {
        Self {
            buffers: (0..num_buffers).map(|_| vec![0; buffer_raw_size]).collect(),
            processing_ids: vec![0; num_buffers],
        }
    }

    #[allow(dead_code)]
    pub fn new_empty() -> Self {
        Self {
            buffers: vec![],
            processing_ids: vec![],
        }
    }
}
