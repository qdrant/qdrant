use std::future::Future;
use std::sync::{Arc, LazyLock};

use aligned_vec::{AVec, RuntimeAlign};
use common::universal_io::UniversalIoError;

/// Reply produced by a spawned read task and shipped back to the originating
/// pipeline over its reply channel. The slot is the correlation id; the
/// destination buffer is the future's output, moved into the response so the
/// pipeline doesn't need to share mutable buffer state with the worker thread.
pub(crate) struct BridgeResponse {
    pub slot: usize,
    pub result: Result<AVec<u8, RuntimeAlign>, UniversalIoError>,
}

impl BridgeResponse {
    /// Build a reply for the given slot with the future's output. Provided so
    /// the spawned task doesn't have to reach into the struct layout when
    /// constructing the response.
    pub(crate) fn new(
        slot: usize,
        result: Result<AVec<u8, RuntimeAlign>, UniversalIoError>,
    ) -> Self {
        Self { slot, result }
    }
}

pub(crate) struct BridgeRuntimeInner(tokio::runtime::Runtime);

/// Cheap-to-clone owner of a dedicated Tokio runtime. Construct one explicitly
/// with [`Self::new`] for an isolated execution domain, or call [`Self::global`]
/// for the process-wide singleton. Internally an `Arc<Inner>`, so clones share
/// the same runtime — and keep it alive for as long as any clone (or any
/// [`BlobFile`](crate::BlobFile) holding one) exists.
///
/// Work reaches the runtime in one of two ways, both routed through the runtime
/// [`Handle`](tokio::runtime::Handle):
/// - single reads / metadata block the caller via [`Self::block_on`];
/// - batched reads are dispatched with [`Handle::spawn`] (see [`Self::handle`]).
#[derive(Clone)]
pub struct BridgeRuntime(Arc<BridgeRuntimeInner>);

impl std::fmt::Debug for BridgeRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BridgeRuntime").finish_non_exhaustive()
    }
}

static GLOBAL: LazyLock<BridgeRuntime> =
    LazyLock::new(|| BridgeRuntime::new().expect("build global BridgeRuntime"));

impl BridgeRuntime {
    pub fn new() -> Result<Self, UniversalIoError> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .thread_name("io-bridge")
            .enable_all()
            .build()
            .map_err(|err| UniversalIoError::S3Config {
                description: format!("build tokio runtime: {err}"),
            })?;

        Ok(Self(Arc::new(BridgeRuntimeInner(runtime))))
    }

    pub fn global() -> Self {
        GLOBAL.clone()
    }

    /// Drive `fut` to completion on the calling thread, using this runtime as
    /// the reactor/executor. Used for the synchronous single-read and metadata
    /// paths. Must not be called from within the runtime's own worker threads.
    pub(crate) fn block_on<F: Future>(&self, fut: F) -> F::Output {
        self.0.0.block_on(fut)
    }

    /// Runtime handle for spawning detached tasks (the batched-read path).
    pub(crate) fn handle(&self) -> &tokio::runtime::Handle {
        self.0.0.handle()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::mpsc;

    use super::*;

    #[test]
    fn global_returns_same_handle() {
        let a = BridgeRuntime::global();
        let b = BridgeRuntime::global();
        assert!(Arc::ptr_eq(&a.0, &b.0));
    }

    #[test]
    fn spawn_runs_future_on_runtime() {
        let rt = BridgeRuntime::new().expect("new runtime");
        let (tx, mut rx) = mpsc::channel(1);
        rt.handle().spawn(async move {
            let _ = tx.send(7u32).await;
        });

        let value = rt
            .block_on(async {
                tokio::time::timeout(Duration::from_secs(2), rx.recv())
                    .await
                    .expect("response within timeout")
            })
            .expect("response channel still open");

        assert_eq!(value, 7);
    }
}
