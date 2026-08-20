//! Alternative Tokio-based IO implementation using io-uring.

use std::ops::Range;
use std::sync::LazyLock;

use aligned_vec::{AVec, RuntimeAlign};
use itertools::Itertools;
use tokio::sync::mpsc;

use crate::universal_io::{IoUringFile, UioResult, UniversalIoError};
static URING_BRIDGE: LazyLock<UringBridge> = LazyLock::new(UringBridge::spawn);

struct UringRequest {
    file: std::fs::File,
    offset: u64,
    len: usize,
    reply: mpsc::Sender<UioResult<Vec<u8>>>,
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
        let file = tokio_uring::fs::File::from_std(req.file);

        let read = || async {
            // TODO(uio): impl tokio_uring::buf::bounded::BoundedBufMut for an AVec newtype
            let buf = Vec::with_capacity(req.len);
            let (res, buf) = file.read_exact_at(buf, req.offset).await;

            let _ = res?;

            Ok(buf)
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

    let file = file.file.try_clone()?.into_file();

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
        reply: tx,
    };

    URING_BRIDGE
        .tx
        .send(req)
        .map_err(|_err| UniversalIoError::Uninitialized {
            description: "uring bridge receiver has been closed".to_owned(),
        })?;

    let Some(buf) = rx.recv().await else {
        return Err(UniversalIoError::Uninitialized {
            description: "uring request been dropped".to_owned(),
        });
    };

    let avec = AVec::from_slice(align, &buf?);

    Ok(avec)
}
