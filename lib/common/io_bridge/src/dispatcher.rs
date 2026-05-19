use std::sync::Arc;

use common::universal_io::{Result, UniversalIoError};
use tokio::runtime::Handle;
use tokio::sync::mpsc;

use crate::backend::AsyncReadBackend;

/// Internal message dispatched from a pipeline to a backend worker. Carries the
/// per-request correlation tag (`user_data_tag`) and the sender the worker
/// writes the result back into. The bridge speaks bytes — element-to-byte
/// translation happens in the pipeline.
pub(crate) struct ReadRequest<B: AsyncReadBackend> {
    pub(crate) location: B::Location,
    pub(crate) byte_offset: u64,
    pub(crate) byte_length: u64,
    pub(crate) user_data_tag: u64,
    pub(crate) result_tx: mpsc::UnboundedSender<DispatchResult>,
}

/// Internal result a backend worker hands back to the requesting pipeline.
pub(crate) struct DispatchResult {
    pub(crate) user_data_tag: u64,
    pub(crate) bytes: Result<Vec<u8>>,
}

/// Long-lived async runtime adapter for an [`AsyncReadBackend`].
///
/// Owns the caller-provided `tokio::runtime::Handle`, spawns a single loop task
/// that drains an MPSC of [`ReadRequest`]s and fans each one out as its own
/// `handle.spawn` so multiple in-flight reads can overlap. Pipelines share
/// dispatchers via [`Arc`].
pub struct AsyncDispatcher<B: AsyncReadBackend> {
    work_tx: mpsc::UnboundedSender<ReadRequest<B>>,
}

impl<B: AsyncReadBackend> AsyncDispatcher<B> {
    pub fn new(handle: Handle, backend: B) -> Self {
        let backend = Arc::new(backend);
        let (work_tx, mut work_rx) = mpsc::unbounded_channel::<ReadRequest<B>>();

        let backend_for_loop = Arc::clone(&backend);
        handle.spawn(async move {
            while let Some(item) = work_rx.recv().await {
                let backend = Arc::clone(&backend_for_loop);
                tokio::spawn(async move {
                    let bytes = backend
                        .read_bytes(item.location, item.byte_offset, item.byte_length)
                        .await;
                    let _ = item.result_tx.send(DispatchResult {
                        user_data_tag: item.user_data_tag,
                        bytes,
                    });
                });
            }
        });

        Self { work_tx }
    }

    pub(crate) fn submit(&self, item: ReadRequest<B>) -> Result<()> {
        self.work_tx
            .send(item)
            .map_err(|_| Self::dispatcher_closed_error())
    }

    fn dispatcher_closed_error() -> UniversalIoError {
        UniversalIoError::uninitialized("async dispatcher closed before request completed")
    }
}
