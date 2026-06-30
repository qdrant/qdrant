//! `uio-client` backend for the [`io_bridge`] sync ↔ async bridge.
//!
//! [`UioSource`] is an [`AsyncRead`] handle that talks to a Qdrant peer's
//! `StorageRead` gRPC service (via [`uio_client::Client`]) instead of an object
//! store. Plugging it into [`io_bridge`] yields a synchronous
//! [`UniversalRead`](common::universal_io::UniversalRead) over the remote shard,
//! exactly like the object-store backends — see [`UioFile`] / [`UioFs`].
//!
//! # Addressing
//!
//! The `StorageRead` service is addressed by `(collection, shard_id, path)`,
//! where `path` is a file path relative to the shard directory. The
//! [`AsyncRead`] surface is path-only, so a [`UioSource`] is pinned to a single
//! `(collection, shard_id)` — i.e. one shard replica on one peer — carried in
//! its [`UioConfig`]. The per-call `path` maps to the in-shard file path. This
//! matches the service's contract that the caller must target the peer that owns
//! the desired replica.
//!
//! # Connection lifecycle
//!
//! [`AsyncRead::open`] is synchronous and runtime-free, but a tonic
//! [`Channel`](https://docs.rs/tonic/latest/tonic/transport/struct.Channel.html)
//! must be constructed from within a Tokio runtime (even a lazy one grabs the
//! reactor handle on construction). So `open` only records the connection
//! parameters; the actual (lazy) client is built once, on first use, inside the
//! bridge runtime via a shared [`OnceCell`](tokio::sync::OnceCell). Every clone
//! of a `UioSource` shares that one client, hence one multiplexed HTTP/2
//! connection.
//!
//! # Read-only
//!
//! `StorageRead` is a read-only service. The mutating [`AsyncRead`] methods
//! ([`create`](AsyncRead::create), [`remove`](AsyncRead::remove),
//! [`remove_dir`](AsyncRead::remove_dir), [`atomic_save`](AsyncRead::atomic_save))
//! therefore return an `Unsupported` IO error; wrap a [`UioFile`] in
//! [`ReadOnly`](common::universal_io::ReadOnly) when a read-only handle is what
//! the caller expects.

use std::future::Future;
use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use common::universal_io::{Result, UniversalIoError, UniversalKind};
use futures::stream::{self, BoxStream, StreamExt as _};
use io_bridge::{AsyncRead, BlobFile, BlobFs};
use tokio::sync::OnceCell;
use uio_client::Client;

/// Connection parameters for a [`UioSource`]: the peer endpoint plus the shard
/// replica `(collection, shard_id)` this handle is pinned to.
#[derive(Clone, Debug)]
pub struct UioConfig {
    /// gRPC endpoint of the Qdrant peer owning the replica, e.g.
    /// `http://peer-1:6335`.
    pub endpoint: String,
    /// Collection name.
    pub collection: String,
    /// Shard id within the collection.
    pub shard_id: u32,
    /// Qdrant API key sent on every request to authenticate to the peer.
    /// `None` for an unauthenticated peer.
    pub api_key: Option<String>,
}

/// Shared state behind a [`UioSource`]. Holds the lazily-built client so that
/// all clones of a source share one connection.
struct Inner {
    endpoint: String,
    collection: Arc<str>,
    shard_id: u32,
    api_key: Option<String>,
    /// Built on first use, inside the bridge runtime (see the crate docs).
    client: OnceCell<Client>,
}

impl Inner {
    /// The shared gRPC client, building it on first use. Must be awaited from
    /// within the runtime that drives the reads (the tonic channel captures the
    /// current reactor on construction).
    async fn client(&self) -> Result<&Client> {
        self.client
            .get_or_try_init(|| async {
                Client::connect_lazy(self.endpoint.clone(), self.api_key.clone())
            })
            .await
    }
}

/// [`AsyncRead`] handle backed by a Qdrant peer's `StorageRead` gRPC service.
///
/// Cheap to clone: clones share one lazily-built [`Client`] (a single
/// multiplexed HTTP/2 connection) and the `Arc`-shared connection parameters.
#[derive(Clone)]
pub struct UioSource {
    inner: Arc<Inner>,
}

impl UioSource {
    /// Record the connection parameters for a shard replica. Performs no IO and
    /// does not touch any runtime — the client is built on first read.
    pub fn new(
        endpoint: impl Into<String>,
        collection: impl Into<Arc<str>>,
        shard_id: u32,
        api_key: Option<String>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                endpoint: endpoint.into(),
                collection: collection.into(),
                shard_id,
                api_key,
                client: OnceCell::new(),
            }),
        }
    }
}

fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn read_only_error(op: &str) -> UniversalIoError {
    UniversalIoError::Io(io::Error::new(
        io::ErrorKind::Unsupported,
        format!("uio StorageRead backend is read-only: {op} is not supported"),
    ))
}

impl AsyncRead for UioSource {
    type Config = UioConfig;

    fn open(config: &Self::Config) -> Result<Self> {
        Ok(Self::new(
            config.endpoint.clone(),
            config.collection.as_str(),
            config.shard_id,
            config.api_key.clone(),
        ))
    }

    fn list_files(
        &self,
        prefix: &Path,
    ) -> impl Future<Output = Result<Vec<PathBuf>>> + Send + 'static {
        let inner = self.inner.clone();
        let prefix = path_to_string(prefix);
        async move {
            let client = inner.client().await?;
            client
                .list_files(&inner.collection, inner.shard_id, &prefix)
                .await
        }
    }

    fn exists(&self, path: &Path) -> impl Future<Output = Result<bool>> + Send + 'static {
        let inner = self.inner.clone();
        let path = path_to_string(path);
        async move {
            let client = inner.client().await?;
            client
                .file_exists(&inner.collection, inner.shard_id, &path)
                .await
        }
    }

    fn create(&self, _path: &Path) -> impl Future<Output = Result<()>> + Send + 'static {
        std::future::ready(Err(read_only_error("create")))
    }

    fn remove(&self, _path: &Path) -> impl Future<Output = Result<()>> + Send + 'static {
        std::future::ready(Err(read_only_error("remove")))
    }

    fn remove_dir(&self, _path: &Path) -> impl Future<Output = Result<()>> + Send + 'static {
        std::future::ready(Err(read_only_error("remove_dir")))
    }

    fn atomic_save(
        &self,
        _path: &Path,
        _bytes: Bytes,
    ) -> impl Future<Output = Result<()>> + Send + 'static {
        std::future::ready(Err(read_only_error("atomic_save")))
    }

    fn read_range(
        &self,
        path: &Path,
        range: Range<u64>,
    ) -> impl Future<Output = Result<BoxStream<'static, Result<Bytes>>>> + Send + 'static {
        let inner = self.inner.clone();
        let path = path_to_string(path);
        let length = range.end - range.start;
        async move {
            let client = inner.client().await?;
            client
                .read_bytes_stream_raw(
                    &inner.collection,
                    inner.shard_id,
                    &path,
                    range.start,
                    length,
                )
                .await
        }
    }

    fn read_from(
        &self,
        path: &Path,
        from: u64,
    ) -> impl Future<Output = Result<(u64, BoxStream<'static, Result<Bytes>>)>> + Send + 'static
    {
        let inner = self.inner.clone();
        let path = path_to_string(path);
        async move {
            let client = inner.client().await?;
            // Unlike a single object-store GET, `StorageRead` has no open-ended
            // "from offset to EOF" RPC that also reports the total size, so we
            // learn the size first (one `FileLength`) and then stream the tail.
            let total = client
                .file_length(&inner.collection, inner.shard_id, &path)
                .await?;
            let length = total.saturating_sub(from);
            if length == 0 {
                // Nothing to read past `from`; yield the size with an empty body
                // rather than issuing a zero-length range request.
                let empty: BoxStream<'static, Result<Bytes>> = stream::empty().boxed();
                return Ok((total, empty));
            }
            let stream = client
                .read_bytes_stream_raw(&inner.collection, inner.shard_id, &path, from, length)
                .await?;
            Ok((total, stream))
        }
    }

    fn len(&self, path: &Path) -> impl Future<Output = Result<u64>> + Send + 'static {
        let inner = self.inner.clone();
        let path = path_to_string(path);
        async move {
            let client = inner.client().await?;
            client
                .file_length(&inner.collection, inner.shard_id, &path)
                .await
        }
    }

    fn kind() -> UniversalKind {
        UniversalKind::Uio
    }
}

/// Sync `UniversalRead` handle over a remote shard file.
pub type UioFile = BlobFile<UioSource>;

/// Sync `UniversalReadFs` over a remote shard.
pub type UioFs = BlobFs<UioSource>;

#[cfg(test)]
mod tests {
    use common::universal_io::UniversalReadFileOps;
    use io_bridge::BridgeRuntime;

    use super::*;

    fn offline_source() -> UioSource {
        UioSource::open(&UioConfig {
            endpoint: "http://127.0.0.1:1".into(),
            collection: "test-col".into(),
            shard_id: 0,
            api_key: None,
        })
        .expect("open records params without dialing")
    }

    #[test]
    fn kind_is_uio() {
        assert_eq!(<UioSource as AsyncRead>::kind(), UniversalKind::Uio);
    }

    #[test]
    fn open_is_offline_and_runtime_free() {
        // `open` must not touch the network or a runtime (the endpoint here is
        // unreachable, and we are not inside a Tokio runtime).
        let _source = offline_source();
    }

    #[test]
    fn writes_are_unsupported() {
        let fs = UioFs::new(offline_source(), BridgeRuntime::global());

        // Mutating ops resolve to a ready `Unsupported` error without any IO, so
        // they can be checked offline.
        let err = fs
            .create(Path::new("f"), 0)
            .expect_err("create is read-only");
        assert!(matches!(err, UniversalIoError::Io(e) if e.kind() == io::ErrorKind::Unsupported));

        let err = fs
            .atomic_save(Path::new("f"), b"x")
            .expect_err("atomic_save is read-only");
        assert!(matches!(err, UniversalIoError::Io(e) if e.kind() == io::ErrorKind::Unsupported));

        let err = fs.remove(Path::new("f")).expect_err("remove is read-only");
        assert!(matches!(err, UniversalIoError::Io(e) if e.kind() == io::ErrorKind::Unsupported));
    }
}
