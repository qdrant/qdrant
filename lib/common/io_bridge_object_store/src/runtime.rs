use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};

use common::universal_io::UniversalIoError;
use tokio::sync::mpsc;

const REQUEST_CHANNEL_CAPACITY: usize = 1024;

/// Envelope shipped from a pipeline (or single-shot caller) to the bridge
/// worker. Carries the async work to perform, the return-address sender on
/// the pipeline's reply channel, and the slot id used to reunite the response
/// with the pipeline's pending-bookkeeping entry.
pub struct BridgeRequest {
    /// The boxed async operation the worker will `.await`. Type-erased so that
    /// futures from different backends can share the same request channel.
    /// Boxing happens at the channel boundary because struct fields cannot
    /// hold `impl Future` directly; the trait surface (`AsyncRead::read_range`)
    /// returns an unboxed `impl Future`.
    ///
    /// The future writes its bytes directly into a caller-owned destination
    /// buffer that lives in the originating pipeline's `pending` map (accessed
    /// via a `SendBytePtr` captured in the future's closure). Completion is
    /// signalled by the `Result<()>` here; the bytes are not shipped through
    /// the channel.
    pub future: Pin<Box<dyn Future<Output = Result<(), UniversalIoError>> + Send>>,
    /// Reply-channel sender cloned from the originating pipeline. The worker
    /// uses this to ship the [`BridgeResponse`] back, so the request itself
    /// carries its own return address — no global routing table is needed.
    pub tx: mpsc::Sender<BridgeResponse>,
    /// Slot id assigned by the pipeline at schedule time. Echoed unchanged in
    /// the [`BridgeResponse`] so the pipeline can look up the matching
    /// `user_data` even when responses arrive out of order.
    pub slot: u64,
}

impl BridgeRequest {
    /// Build a request. The caller passes an unboxed `impl Future`; this
    /// constructor performs the `Box::pin` type-erasure required to put the
    /// future into the [`Self::future`] struct field. See the field doc for
    /// why boxing happens at the channel boundary.
    pub fn new<F>(future: F, tx: mpsc::Sender<BridgeResponse>, slot: u64) -> Self
    where
        F: Future<Output = Result<(), UniversalIoError>> + Send + 'static,
    {
        Self {
            future: Box::pin(future),
            tx,
            slot,
        }
    }
}

/// Reply shipped from the worker back to the originating pipeline. The slot
/// is the correlation id; `result` is the future's status — the actual bytes
/// have already been written into the pipeline-owned destination buffer.
#[derive(Debug)]
pub struct BridgeResponse {
    pub slot: u64,
    pub result: Result<(), UniversalIoError>,
}

impl BridgeResponse {
    /// Build a reply for the given slot with the future's status. Symmetric to
    /// [`BridgeRequest::new`]; provided so the worker thread doesn't have to
    /// reach into the struct layout when constructing the response.
    pub fn new(slot: u64, result: Result<(), UniversalIoError>) -> Self {
        Self { slot, result }
    }
}

pub(crate) struct BridgeRuntimeInner {
    tx: mpsc::Sender<BridgeRequest>,
    runtime: tokio::runtime::Runtime,
    _worker: std::thread::JoinHandle<()>,
}

/// Cheap-to-clone handle to a dedicated Tokio runtime plus a worker thread
/// driving the request channel. Construct one explicitly with [`Self::new`]
/// for an isolated execution domain, or call [`Self::global`] for the
/// process-wide singleton. Internally an `Arc<Inner>`, so clones share the
/// same runtime, worker thread, and request channel.
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

        let (tx, rx) = mpsc::channel::<BridgeRequest>(REQUEST_CHANNEL_CAPACITY);
        let handle = runtime.handle().clone();

        let worker_loop = async |mut rx: mpsc::Receiver<BridgeRequest>| {
            while let Some(req) = rx.recv().await {
                tokio::spawn(async move {
                    let result = req.future.await;
                    std::mem::drop(req.tx.send(BridgeResponse::new(req.slot, result)).await);
                });
            }
        };

        let worker = std::thread::Builder::new()
            .name("io-bridge-dispatcher".into())
            .spawn(move || {
                handle.block_on(worker_loop(rx));
            })
            .map_err(|err| UniversalIoError::S3Config {
                description: format!("spawn worker thread: {err}"),
            })?;

        let inner = BridgeRuntimeInner {
            tx,
            runtime,
            _worker: worker,
        };
        Ok(Self(Arc::new(inner)))
    }

    pub fn global() -> Self {
        GLOBAL.clone()
    }

    pub(crate) fn block_on<F: Future>(&self, fut: F) -> F::Output {
        self.0.runtime.handle().block_on(fut)
    }

    pub(crate) fn tx(&self) -> mpsc::Sender<BridgeRequest> {
        self.0.tx.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn global_returns_same_handle() {
        let a = BridgeRuntime::global();
        let b = BridgeRuntime::global();
        assert!(Arc::ptr_eq(&a.0, &b.0));
    }

    #[test]
    fn runtime_executes_future_via_worker() {
        let rt = BridgeRuntime::new().expect("new runtime");
        let (tx, mut rx) = mpsc::channel(1);
        let req = BridgeRequest::new(async { Ok(()) }, tx, 7);
        rt.tx().try_send(req).expect("enqueue");

        let resp = rt
            .block_on(async {
                tokio::time::timeout(Duration::from_secs(2), rx.recv())
                    .await
                    .expect("response within timeout")
            })
            .expect("response channel still open");

        assert_eq!(resp.slot, 7);
        resp.result.expect("future succeeded");
    }
}
