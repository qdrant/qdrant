//! Alternative Tokio-based IO implementation using io-uring.

use std::io;
use std::ops::Range;
use std::sync::LazyLock;

use aligned_vec::{AVec, RuntimeAlign};
use tokio::sync::mpsc;

use super::KERNEL_PAGE_SIZE;
use crate::universal_io::{IoUringFile, UioResult, UniversalIoError};
static URING_BRIDGE: LazyLock<UringBridge> = LazyLock::new(UringBridge::spawn);

struct UringRequest {
    file: fs_err::File,
    offset: u64,
    len: usize,
    align: usize,
    direct_io: bool,
    reply: mpsc::Sender<UioResult<AVec<u8, RuntimeAlign>>>,
}

struct UringBridge {
    tx: tokio::sync::mpsc::UnboundedSender<UringRequest>,
}

impl UringBridge {
    fn spawn() -> Self {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<UringRequest>();
        std::thread::Builder::new()
            .name("tokio-uring-bridge".into())
            .spawn(move || {
                tokio_uring::start(async move {
                    while let Some(req) = rx.recv().await {
                        tokio_uring::spawn(async move {
                            let _ = Self::execute(req).await;
                        });
                    }
                });
            })
            .expect("spawn uring bridge thread");
        Self { tx }
    }

    async fn execute(req: UringRequest) {
        let UringRequest {
            file,
            offset,
            len,
            align,
            direct_io,
            reply,
        } = req;

        let file = tokio_uring::fs::File::from_std(file.into_file());

        let read = || async {
            if !direct_io {
                let buf = ABuf(AVec::with_capacity(align, len));

                let (res, buf) = file.read_exact_at(buf, offset).await;

                res?;

                return Ok(buf.0);
            }

            // Mirror `IoUringState::read`: `O_DIRECT` requires a page-aligned
            // offset and buffer, and a page-multiple submitted length.
            assert!(
                align.is_multiple_of(KERNEL_PAGE_SIZE),
                "O_DIRECT read buffer must be aligned to {KERNEL_PAGE_SIZE} bytes (alignment: {align})",
            );
            assert!(
                offset.is_multiple_of(KERNEL_PAGE_SIZE as u64),
                "O_DIRECT read offset must be aligned to {KERNEL_PAGE_SIZE} bytes (offset: {offset})",
            );

            let kernel_len = len
                .checked_next_multiple_of(KERNEL_PAGE_SIZE)
                .expect("rounded read length fit within usize");

            let buf = ABuf(AVec::with_capacity(align, kernel_len));
            let (res, buf) = file.read_at(buf, offset).await;
            let bytes_read = res?;

            // A short count only happens when the range crosses EOF: the
            // EOF-clamped tail block still yields all `len` requested bytes.
            if bytes_read < len {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("O_DIRECT read at {offset} returned {bytes_read} of {len} bytes"),
                )
                .into());
            }

            // Drop the over-read up to the page boundary.
            let mut buffer = buf.0;
            buffer.truncate(len);
            Ok(buffer)
        };

        match reply.send(read().await).await {
            Ok(()) => {}
            Err(_err) => {
                // The requester was dropped or closed the channel, nothing to do here
            }
        }
    }
}

pub async fn read_bytes_async(
    file: &IoUringFile,
    range: Range<u64>,
    align: usize,
) -> UioResult<AVec<u8, RuntimeAlign>> {
    let (tx, mut rx) = mpsc::channel(1);

    let direct_io = file.direct_io;
    let file = file.file.try_clone()?;

    let Some(len) = range.end.checked_sub(range.start) else {
        return Err(UniversalIoError::OutOfBounds {
            start: range.start,
            end: range.end,
            elements: 0,
        });
    };

    let req = UringRequest {
        file,
        offset: range.start,
        len: len as usize,
        align,
        direct_io,
        reply: tx,
    };

    URING_BRIDGE
        .tx
        .send(req)
        .map_err(|_err| UniversalIoError::Uninitialized {
            description: "uring bridge receiver has been closed".to_owned(),
        })?;

    let Some(result) = rx.recv().await else {
        return Err(UniversalIoError::Uninitialized {
            description: "uring request been dropped".to_owned(),
        });
    };

    result
}

struct ABuf(AVec<u8, RuntimeAlign>);

unsafe impl tokio_uring::buf::IoBuf for ABuf {
    fn stable_ptr(&self) -> *const u8 {
        self.0.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.0.len()
    }

    fn bytes_total(&self) -> usize {
        self.0.capacity()
    }
}

unsafe impl tokio_uring::buf::IoBufMut for ABuf {
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        self.0.as_mut_ptr()
    }

    unsafe fn set_init(&mut self, init_len: usize) {
        if self.0.len() < init_len {
            unsafe { self.0.set_len(init_len) };
        }
    }
}
