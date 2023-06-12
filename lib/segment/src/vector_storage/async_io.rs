use std::fs::File;
use std::os::fd::AsRawFd;

use io_uring::{opcode, types, IoUring};

use crate::common::mmap_ops::transmute_from_u8_to_slice;
use crate::data_types::vectors::VectorElementType;
use crate::entry::entry_point::{OperationError, OperationResult};
use crate::types::PointOffsetType;

const DISK_PARALLELISM: usize = 16; // TODO: benchmark it better, or make it configurable

struct BufferStore {
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

pub struct UringBufferedReader {
    file: File,
    buffers: BufferStore,
    io_uring: IoUring,
    raw_size: usize,
    header_size: usize,
}

impl UringBufferedReader {
    pub fn new(file: File, raw_size: usize, header_size: usize) -> OperationResult<Self> {
        let buffers = BufferStore::new(DISK_PARALLELISM, raw_size);
        let io_uring = IoUring::new(DISK_PARALLELISM as _)?;

        Ok(Self {
            file,
            buffers,
            io_uring,
            raw_size,
            header_size,
        })
    }

    fn encode_user_data(buffer_id: usize, entry_num: usize) -> u64 {
        ((buffer_id as u64) << 32) | (entry_num as u64)
    }

    fn decode_user_data(user_data: u64) -> (usize, usize) {
        (
            (user_data >> 32) as usize,
            (user_data & 0xFFFFFFFF) as usize,
        )
    }

    /// Takes in iterator of point offsets, reads it, and yields a callback with the read data.
    pub fn read_stream(
        &mut self,
        points: impl IntoIterator<Item = PointOffsetType>,
        mut callback: impl FnMut(usize, PointOffsetType, &[VectorElementType]),
    ) -> OperationResult<()> {
        let buffers_count = self.buffers.buffers.len();

        let mut unused_buffer_ids = (0..buffers_count).collect::<Vec<_>>();

        for item in points.into_iter().enumerate() {
            let (idx, point): (usize, PointOffsetType) = item;

            if unused_buffer_ids.is_empty() {
                // Wait for at least one buffer to become available
                self.io_uring.submit_and_wait(buffers_count)?;

                let cqe = self.io_uring.completion();
                for entry in cqe {
                    let (buffer_id, idx) = Self::decode_user_data(entry.user_data());
                    let point_id = self.buffers.processing_ids[buffer_id];
                    let buffer = &self.buffers.buffers[buffer_id];
                    let vector = transmute_from_u8_to_slice(buffer);
                    callback(idx, point_id, vector);
                    unused_buffer_ids.push(buffer_id);
                }
            }
            // Assume there is at least one buffer available at this point
            let buffer_id = unused_buffer_ids.pop().unwrap();

            self.buffers.processing_ids[buffer_id] = point;
            let buffer = &mut self.buffers.buffers[buffer_id];
            let offset = self.header_size + self.raw_size * point as usize;

            let user_data = Self::encode_user_data(buffer_id, idx);

            let read_e = opcode::Read::new(
                types::Fd(self.file.as_raw_fd()),
                buffer.as_mut_ptr(),
                buffer.len() as _,
            )
            .offset(offset as _)
            .build()
            .user_data(user_data);

            unsafe {
                // self.io_uring.submission().push(&read_e).unwrap();
                self.io_uring.submission().push(&read_e).map_err(|err| {
                    OperationError::service_error(format!("Failed using io-uring: {}", err))
                })?;
            }
        }

        let mut operations_to_wait_for = self.buffers.buffers.len() - unused_buffer_ids.len();

        while operations_to_wait_for > 0 {
            self.io_uring.submit_and_wait(operations_to_wait_for)?;
            let cqe = self.io_uring.completion();
            for entry in cqe {
                let (buffer_id, idx) = Self::decode_user_data(entry.user_data());
                let point = self.buffers.processing_ids[buffer_id];
                let buffer = &self.buffers.buffers[buffer_id];
                let vector = transmute_from_u8_to_slice(buffer);
                callback(idx, point, vector);
                unused_buffer_ids.push(buffer_id);
            }

            operations_to_wait_for = self.buffers.buffers.len() - unused_buffer_ids.len();
        }
        Ok(())
    }
}
