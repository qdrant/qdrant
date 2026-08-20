//! Alternative Tokio-based IO implementation using io-uring.

use std::ops::Range;
use std::sync::LazyLock;

use aligned_vec::{AVec, RuntimeAlign};
use itertools::Itertools;
use tokio::sync::mpsc;

use crate::universal_io::{IoUringFile, UioResult, UniversalIoError};
static URING_BRIDGE: LazyLock<UringBridge> = LazyLock::new(UringBridge::spawn);

struct UringRequest {
    file: fs_err::File,
    offset: u64,
    len: usize,
    align: usize,
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
        let file = tokio_uring::fs::File::from_std(req.file.into_file());

        let read = || async {
            let buf = ABuf(AVec::with_capacity(req.align, req.len));

            let (res, buf) = file.read_exact_at(buf, req.offset).await;

            res?;

            Ok(buf.0)
        };

        let _ = req.reply.send(read().await);
    }
}

pub async fn read_bytes_async(
    file: &IoUringFile,
    range: Range<u64>,
    align: usize,
) -> UioResult<AVec<u8, RuntimeAlign>> {
    let (tx, mut rx) = mpsc::channel(1);

    let file = file.file.try_clone()?;

    let Ok(len) = range.try_len() else {
        return Err(UniversalIoError::OutOfBounds {
            start: range.start,
            end: range.end,
            elements: 0,
        });
    };

    let req = UringRequest {
        file,
        offset: range.start,
        len,
        align,
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
