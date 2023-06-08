use std::os::fd::AsRawFd as _;
use std::{fs, io};

use crate::common::mmap_ops::transmute_from_u8_to_slice;
use crate::data_types::vectors::VectorElementType;
use crate::entry::entry_point::OperationResult;
use crate::types::PointOffsetType;

pub struct UringReader {
    io_uring: Option<io_uring::IoUring>,
    buffers: Vec<Buffer>,
    file: fs::File,
    header_size_bytes: usize,
    vector_size_bytes: usize,
    disk_parallelism: usize,
}

#[derive(Clone, Debug)]
struct Buffer {
    buffer: Vec<u8>,
    point: Option<Point>,
}

impl Buffer {
    pub fn new(len: usize) -> Self {
        Self {
            buffer: vec![0; len],
            point: None,
        }
    }
}

#[derive(Copy, Clone, Debug)]
struct Point {
    index: usize,
    offset: PointOffsetType,
}

impl Point {
    pub fn new(index: usize, offset: PointOffsetType) -> Self {
        Self { index, offset }
    }
}

impl UringReader {
    pub fn new(
        file: fs::File,
        header_size_bytes: usize,
        vector_size_bytes: usize,
        disk_parallelism: usize,
    ) -> OperationResult<Self> {
        let reader = Self {
            io_uring: Some(io_uring::IoUring::new(disk_parallelism as _)?),
            buffers: Vec::new(),
            file,
            header_size_bytes,
            vector_size_bytes,
            disk_parallelism,
        };

        Ok(reader)
    }

    /// Takes in iterator of point offsets, reads it, and yields a callback with the read data.
    pub fn read_stream(
        &mut self,
        points: impl IntoIterator<Item = PointOffsetType>,
        mut callback: impl FnMut(usize, PointOffsetType, &[VectorElementType]),
    ) -> OperationResult<()> {
        let mut io_uring = match self.io_uring.take() {
            Some(io_uring) => io_uring,
            None => io_uring::IoUring::new(self.disk_parallelism as _)?,
        };

        let mut uring = IoUringView::from_io_uring(&mut io_uring);

        let mut points = points
            .into_iter()
            .enumerate()
            .map(|(index, offset)| Point::new(index, offset));

        let mut submitted = 0;

        let cq_capacity = uring.cq.capacity();

        while submitted < self.disk_parallelism.min(cq_capacity) {
            while uring.sq.len() < self.disk_parallelism.min(cq_capacity) && !uring.sq.is_full() {
                let Some(point) = points.next() else {
                    break;
                };

                let sq_len = uring.sq.len();

                push_sqe(
                    &mut uring.sq,
                    &mut self.buffers,
                    submitted + sq_len,
                    &self.file,
                    self.header_size_bytes,
                    self.vector_size_bytes,
                    point,
                );
            }

            submitted += uring.submit_sq()?;
        }

        uring.probe_cq_or_wait_cq()?;

        while submitted > 0 {
            let mut submit_sq = false;

            for cqe in (&mut uring.cq).filter(|cqe| cqe.user_data() != u64::MAX) {
                submitted -= 1;

                let (buffer_index, point, vector) = consume_cqe(&mut self.buffers, cqe)?;

                callback(point.index, point.offset, vector);

                let Some(point) = points.next() else {
                    continue;
                };

                push_sqe(
                    &mut uring.sq,
                    &mut self.buffers,
                    buffer_index,
                    &self.file,
                    self.header_size_bytes,
                    self.vector_size_bytes,
                    point,
                );

                submit_sq =
                    uring.sq.len() >= self.disk_parallelism.min(cq_capacity) || uring.sq.is_full();

                if submit_sq {
                    break;
                }
            }

            if !submit_sq {
                submitted += uring.probe_cq_or_wait_cq_and_maybe_submit_sq()?;
            } else {
                submitted += uring.submit_sq_and_maybe_wait_cq()?;
            }
        }

        drop(uring);

        self.io_uring = Some(io_uring);

        Ok(())
    }
}

struct IoUringView<'a> {
    submitter: io_uring::Submitter<'a>,
    sq: io_uring::SubmissionQueue<'a>,
    cq: io_uring::CompletionQueue<'a>,
}

impl<'a> IoUringView<'a> {
    pub fn from_io_uring(io_uring: &'a mut io_uring::IoUring) -> Self {
        let (submitter, sq, cq) = io_uring.split();
        Self { submitter, sq, cq }
    }

    pub fn submit_sq(&mut self) -> io::Result<usize> {
        self.submit_sq_and_wait_cq(0)
    }

    pub fn probe_cq_or_wait_cq(&mut self) -> io::Result<()> {
        let submitted = self.probe_cq_or_wait_cq_and_maybe_submit_sq()?;

        // Assert that no SQEs have been submitted
        debug_assert_eq!(
            submitted, 0,
            "{submitted} SQEs have been submitted during `probe_cq_or_wait_cq`!",
        );

        Ok(())
    }

    pub fn probe_cq_or_wait_cq_and_maybe_submit_sq(&mut self) -> io::Result<usize> {
        if self.probe_cq_is_empty() {
            self.submit_sq_and_wait_cq(1)
        } else {
            Ok(0)
        }
    }

    pub fn submit_sq_and_maybe_wait_cq(&mut self) -> io::Result<usize> {
        let want = if self.probe_cq_is_empty() { 1 } else { 0 };
        self.submit_sq_and_wait_cq(want)
    }

    fn check_cq_is_empty(&mut self) -> bool {
        self.cq.sync();
        self.cq.is_empty()
    }

    fn probe_cq_is_empty(&mut self) -> bool {
        for _ in 0..3 {
            if !self.check_cq_is_empty() {
                return false;
            }
        }

        true
    }

    fn submit_sq_and_wait_cq(&mut self, want: usize) -> io::Result<usize> {
        // Sync SQ (so that kernel will see pushed SQEs)
        self.sq.sync();

        let submit_nop = want > 0 && self.sq.is_empty();

        if submit_nop {
            let sqe = io_uring::opcode::Nop::new().build().user_data(u64::MAX);
            unsafe { self.sq.push(&sqe).expect("SQ is not full") };
            self.sq.sync();
        }

        // Submit SQEs (if any) and wait for `want` CQEs
        let mut submitted = self.submitter.submit_and_wait(want)?;

        // Assert that all (and no more than expected) SQEs have been submitted.
        //
        // Kernel should consume SQEs from SQ during submit, but `self.sq` state is not updated
        // until `sync` call, so `self.sq` should still hold pre-`submit` state at this point.
        debug_assert_eq!(
            submitted,
            self.sq.len(),
            "Not all (or more than expected) SQEs have been submitted!",
        );

        if submitted > 0 {
            // Sync SQ (so that we will see SQEs consumed by the kernel)
            self.sq.sync();

            // Assert that all SQEs have been consumed during submit (SQ is empty)
            debug_assert!(
                self.sq.is_empty(),
                "Not all SQEs have been consumed during submit (SQ is not empty)!",
            );
        }

        if want > 0 {
            // Sync CQ (so that we will see CQEs pushed by the kernel).
            self.cq.sync();

            // Assert that CQ is not empty after `submit_and_wait`.
            debug_assert!(!self.cq.is_empty(), "CQ is empty after `submit_and_wait`!");
        }

        if submit_nop {
            submitted = submitted.saturating_sub(1);
        }

        Ok(submitted)
    }
}

fn push_sqe(
    sq: &mut io_uring::SubmissionQueue,
    buffers: &mut Vec<Buffer>,
    buffer_index: usize,
    file: &fs::File,
    header_size_bytes: usize,
    vector_size_bytes: usize,
    point: Point,
) {
    debug_assert!(buffers.len() >= buffer_index); // TODO!

    if buffers.len() == buffer_index {
        buffers.push(Buffer::new(vector_size_bytes));
    }

    let buffer = &mut buffers[buffer_index];

    let vector_offset_bytes = header_size_bytes + vector_size_bytes * point.offset as usize;

    let sqe = io_uring::opcode::Read::new(
        io_uring::types::Fd(file.as_raw_fd()),
        buffer.buffer.as_mut_ptr(),
        buffer.buffer.len() as _,
    )
    .offset(vector_offset_bytes as _)
    .build()
    .user_data(buffer_index as _);

    buffer.point = Some(point);

    unsafe {
        sq.push(&sqe).expect("SQ is not full");
    }
}

fn consume_cqe(
    buffers: &mut [Buffer],
    cqe: io_uring::cqueue::Entry,
) -> io::Result<(usize, Point, &[f32])> {
    let buffer_index = cqe.user_data() as usize;

    let buffer = &mut buffers[buffer_index];
    let point = buffer.point.take().expect("point data is associated with the buffer");

    let result = cqe.result();
    let expected = buffer.buffer.len();

    if result < 0 {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!("io_uring operation failed with {} error", -result),
        ));
    } else if (result as usize) != expected {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!(
                "io_uring operation read {} bytes, which is {} than expected {} bytes",
                result,
                if (result as usize) < expected { "less" } else { "more" },
                buffer.buffer.len(),
            ),
        ));
    }

    let vector = transmute_from_u8_to_slice(&buffer.buffer);

    Ok((buffer_index, point, vector))
}
