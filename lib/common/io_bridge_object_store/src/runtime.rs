use std::sync::{Arc, LazyLock};

use common::universal_io::UniversalIoError;
use object_store::{ObjectStore, ObjectStoreExt};
use tokio::sync::mpsc;

const REQUEST_CHANNEL_CAPACITY: usize = 1024;

pub struct S3Request {
    pub store: Arc<dyn ObjectStore>,
    pub key: object_store::path::Path,
    pub range: std::ops::Range<u64>,
    pub tx: mpsc::Sender<S3Response>,
    pub slot: u64,
}

#[derive(Debug)]
pub struct S3Response {
    pub slot: u64,
    pub bytes: Result<bytes::Bytes, UniversalIoError>,
}

pub(crate) struct S3Runtime {
    tx: mpsc::Sender<S3Request>,
    runtime: tokio::runtime::Runtime,
    _worker: std::thread::JoinHandle<()>,
}

#[derive(Clone)]
pub struct S3RuntimeHandle(Arc<S3Runtime>);

impl std::fmt::Debug for S3RuntimeHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3RuntimeHandle").finish_non_exhaustive()
    }
}

static GLOBAL: LazyLock<S3RuntimeHandle> =
    LazyLock::new(|| S3RuntimeHandle::new().expect("build global S3RuntimeHandle"));

impl S3RuntimeHandle {
    pub fn new() -> Result<Self, UniversalIoError> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .thread_name("s3-bridge")
            .enable_all()
            .build()
            .map_err(|err| UniversalIoError::S3Config {
                description: format!("build tokio runtime: {err}"),
            })?;

        let (tx, request_rx) = mpsc::channel::<S3Request>(REQUEST_CHANNEL_CAPACITY);
        let handle = runtime.handle().clone();

        let worker_loop = async |mut rx: mpsc::Receiver<S3Request>| {
            while let Some(req) = rx.recv().await {
                tokio::spawn(async move {
                    let result = req.store.get_range(&req.key, req.range.clone()).await;
                    let bytes = result.map_err(|err| match err {
                        object_store::Error::NotFound { .. } => UniversalIoError::NotFound {
                            path: std::path::PathBuf::from(req.key.to_string()),
                        },
                        other => UniversalIoError::s3(other),
                    });

                    std::mem::drop(
                        req.tx
                            .send(S3Response {
                                slot: req.slot,
                                bytes,
                            })
                            .await,
                    );
                });
            }
        };

        let worker = std::thread::Builder::new()
            .name("s3-bridge-dispatcher".into())
            .spawn(move || {
                handle.block_on(worker_loop(request_rx));
            })
            .map_err(|err| UniversalIoError::S3Config {
                description: format!("spawn worker thread: {err}"),
            })?;

        let inner = S3Runtime {
            tx,
            runtime,
            _worker: worker,
        };
        Ok(Self(Arc::new(inner)))
    }

    pub fn global() -> Self {
        GLOBAL.clone()
    }

    pub(crate) fn block_on<F: std::future::Future>(&self, fut: F) -> F::Output {
        self.0.runtime.handle().block_on(fut)
    }

    pub(crate) fn tx(&self) -> mpsc::Sender<S3Request> {
        self.0.tx.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::Bytes;
    use object_store::ObjectStore;
    use object_store::memory::InMemory;

    use super::*;

    #[test]
    fn runtime_global_returns_same_handle() {
        let a = S3RuntimeHandle::global();
        let b = S3RuntimeHandle::global();
        assert!(Arc::ptr_eq(&a.0, &b.0));
    }

    #[test]
    fn runtime_executes_request_via_worker() {
        let rt = S3RuntimeHandle::new().expect("new runtime");
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        rt.block_on(async {
            store
                .put(
                    &object_store::path::Path::from("k"),
                    Bytes::from_static(b"hello").into(),
                )
                .await
                .expect("put");
        });

        let (tx, mut rx) = mpsc::channel(1);
        let req = S3Request {
            store: store.clone(),
            key: object_store::path::Path::from("k"),
            range: 0..5,
            tx,
            slot: 7,
        };
        rt.tx().try_send(req).expect("enqueue");

        let resp = rt
            .block_on(async {
                tokio::time::timeout(Duration::from_secs(2), rx.recv())
                    .await
                    .expect("response within timeout")
            })
            .expect("response channel still open");

        assert_eq!(resp.slot, 7);
        let bytes = resp.bytes.expect("get succeeds");
        assert_eq!(&bytes[..], b"hello");
    }
}
