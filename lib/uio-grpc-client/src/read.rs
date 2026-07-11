use std::path::PathBuf;

use bytes::Bytes;
use common::universal_io::{ListedFile, ReadRange, Result, UniversalIoError};
use futures::StreamExt as _;
use futures::stream::BoxStream;
use tonic::codegen::InterceptedService;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::Interceptor;
use tonic::transport::Channel;

use crate::StorageReadClient;
use crate::generated::qdrant;

/// gRPC metadata header carrying the Qdrant API key (matches the public API).
const HTTP_HEADER_API_KEY: &str = "api-key";

fn tonic_err(e: tonic::Status) -> UniversalIoError {
    UniversalIoError::Io(std::io::Error::other(e))
}

/// Interceptor that injects the Qdrant API key into every outgoing request, so
/// the remote peer can authenticate the connection. A no-op when no key is set.
#[derive(Clone)]
pub struct AuthInterceptor {
    api_key: Option<MetadataValue<Ascii>>,
}

impl AuthInterceptor {
    /// Parse the optional API key into a (sensitive) metadata value up front, so
    /// an invalid key is reported at connect time rather than on every request.
    fn new(api_key: Option<String>) -> Result<Self> {
        let api_key = api_key
            .map(|key| {
                let mut value: MetadataValue<Ascii> = key.parse().map_err(|err| {
                    UniversalIoError::Io(std::io::Error::other(format!(
                        "API key contains invalid characters for gRPC metadata: {err}"
                    )))
                })?;
                value.set_sensitive(true);
                Ok::<_, UniversalIoError>(value)
            })
            .transpose()?;
        Ok(Self { api_key })
    }
}

impl Interceptor for AuthInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> std::result::Result<tonic::Request<()>, tonic::Status> {
        if let Some(api_key) = &self.api_key
            && request.metadata().get(HTTP_HEADER_API_KEY).is_none()
        {
            request
                .metadata_mut()
                .insert(HTTP_HEADER_API_KEY, api_key.clone());
        }
        Ok(request)
    }
}

/// Like [`tonic_err`], but maps a `NOT_FOUND` status onto the structured
/// [`UniversalIoError::NotFound`] so callers can match on it (e.g. the
/// empty-tail disambiguation in the `io_bridge` read pipeline) without
/// inspecting an opaque `io::Error`.
fn status_to_universal(e: tonic::Status, path: &str) -> UniversalIoError {
    if e.code() == tonic::Code::NotFound {
        UniversalIoError::NotFound {
            path: PathBuf::from(path),
        }
    } else {
        tonic_err(e)
    }
}

/// Shared gRPC connection to a remote StorageRead service.
///
/// Every public method maps 1-to-1 to a server RPC.
///
/// Cheap to clone — wraps a tonic [`Channel`] which multiplexes over a
/// single HTTP/2 connection.  Concurrent requests are fully supported
/// without any internal locking.
#[derive(Clone)]
pub struct Client {
    inner: StorageReadClient<InterceptedService<Channel, AuthInterceptor>>,
}

impl Client {
    /// Connect to a remote StorageRead gRPC service, authenticating with the
    /// optional Qdrant API key.
    pub async fn connect(endpoint: impl Into<String>, api_key: Option<String>) -> Result<Self> {
        let interceptor = AuthInterceptor::new(api_key)?;
        let channel = Channel::from_shared(endpoint.into())
            .map_err(|e| UniversalIoError::Io(std::io::Error::other(e)))?
            .connect()
            .await
            .map_err(|e| UniversalIoError::Io(std::io::Error::other(e)))?;

        Ok(Self {
            inner: StorageReadClient::with_interceptor(channel, interceptor),
        })
    }

    /// Build a client without establishing the connection yet, authenticating
    /// with the optional Qdrant API key.
    ///
    /// Unlike [`connect`](Self::connect), this is synchronous and does not need a
    /// Tokio runtime: the underlying HTTP/2 connection is opened lazily on the
    /// first request (driven by whatever runtime executes that request). This is
    /// what lets the `io_bridge_uio_grpc` backend satisfy the synchronous,
    /// runtime-free `AsyncRead::open` contract.
    pub fn connect_lazy(endpoint: impl Into<String>, api_key: Option<String>) -> Result<Self> {
        let interceptor = AuthInterceptor::new(api_key)?;
        let channel = Channel::from_shared(endpoint.into())
            .map_err(|e| UniversalIoError::Io(std::io::Error::other(e)))?
            .connect_lazy();

        Ok(Self {
            inner: StorageReadClient::with_interceptor(channel, interceptor),
        })
    }

    /// `ListFiles` RPC.
    pub async fn list_files(
        &self,
        collection_name: &str,
        shard_id: u32,
        prefix_path: &str,
    ) -> Result<Vec<ListedFile>> {
        let response = self
            .inner
            .clone()
            .list_files(qdrant::ListFilesRequest {
                collection_name: collection_name.to_string(),
                shard_id,
                prefix_path: prefix_path.to_string(),
            })
            .await
            .map_err(|e| status_to_universal(e, prefix_path))?;

        Ok(response
            .into_inner()
            .files
            .into_iter()
            .map(|entry| ListedFile {
                path: PathBuf::from(entry.path),
                size: entry.size,
                last_modified: entry
                    .last_modified
                    .and_then(|ts| std::time::SystemTime::try_from(ts).ok()),
            })
            .collect())
    }

    /// `FileExists` RPC.
    pub async fn file_exists(
        &self,
        collection_name: &str,
        shard_id: u32,
        path: &str,
    ) -> Result<bool> {
        let response = self
            .inner
            .clone()
            .file_exists(qdrant::FileExistsRequest {
                collection_name: collection_name.to_string(),
                shard_id,
                path: path.to_string(),
            })
            .await
            .map_err(tonic_err)?;

        Ok(response.into_inner().exists)
    }

    /// `FileLength` RPC.
    pub async fn file_length(
        &self,
        collection_name: &str,
        shard_id: u32,
        path: &str,
    ) -> Result<u64> {
        let response = self
            .inner
            .clone()
            .file_length(qdrant::FileLengthRequest {
                collection_name: collection_name.to_string(),
                shard_id,
                path: path.to_string(),
            })
            .await
            .map_err(|e| status_to_universal(e, path))?;

        Ok(response.into_inner().length)
    }

    /// `ReadBytes` RPC — single range from a single file.
    pub async fn read_bytes(
        &self,
        collection_name: &str,
        shard_id: u32,
        path: &str,
        byte_offset: u64,
        length: u64,
    ) -> Result<Vec<u8>> {
        let response = self
            .inner
            .clone()
            .read_bytes(qdrant::ReadBytesRequest {
                collection_name: collection_name.to_string(),
                shard_id,
                path: path.to_string(),
                byte_offset,
                length,
            })
            .await
            .map_err(|e| status_to_universal(e, path))?;

        Ok(response.into_inner().data)
    }

    /// `ReadBytesStream` RPC — streaming variant for large ranges (~1 MB chunks).
    pub async fn read_bytes_stream(
        &self,
        collection_name: &str,
        shard_id: u32,
        path: &str,
        byte_offset: u64,
        length: u64,
    ) -> Result<Vec<u8>> {
        let mut stream = self
            .inner
            .clone()
            .read_bytes_stream(qdrant::ReadBytesStreamRequest {
                collection_name: collection_name.to_string(),
                shard_id,
                path: path.to_string(),
                byte_offset,
                length,
            })
            .await
            .map_err(|e| status_to_universal(e, path))?
            .into_inner();

        let mut data = Vec::with_capacity(length as usize);
        while let Some(chunk) = stream.message().await.map_err(tonic_err)? {
            data.extend_from_slice(&chunk.data);
        }
        Ok(data)
    }

    /// `ReadBytesStream` RPC, exposed as a stream of raw chunks rather than a
    /// reassembled `Vec`.
    ///
    /// The returned stream yields the server's ~1 MB chunks as [`Bytes`] in
    /// order. This lets a consumer fold the chunks straight into a destination
    /// buffer without the intermediate `Vec` that [`read_bytes_stream`] builds —
    /// it is the entry point the `io_bridge_uio_grpc` backend uses to satisfy
    /// `AsyncRead::read_range`.
    pub async fn read_bytes_stream_raw(
        &self,
        collection_name: &str,
        shard_id: u32,
        path: &str,
        byte_offset: u64,
        length: u64,
    ) -> Result<BoxStream<'static, Result<Bytes>>> {
        let path = path.to_string();
        let stream = self
            .inner
            .clone()
            .read_bytes_stream(qdrant::ReadBytesStreamRequest {
                collection_name: collection_name.to_string(),
                shard_id,
                path: path.clone(),
                byte_offset,
                length,
            })
            .await
            .map_err(|e| status_to_universal(e, &path))?
            .into_inner();

        Ok(stream
            .map(move |message| match message {
                Ok(chunk) => Ok(Bytes::from(chunk.data)),
                Err(status) => Err(status_to_universal(status, &path)),
            })
            .boxed())
    }

    /// `ReadWhole` RPC — read an entire file.
    pub async fn read_whole(
        &self,
        collection_name: &str,
        shard_id: u32,
        path: &str,
    ) -> Result<Vec<u8>> {
        let response = self
            .inner
            .clone()
            .read_whole(qdrant::ReadWholeRequest {
                collection_name: collection_name.to_string(),
                shard_id,
                path: path.to_string(),
            })
            .await
            .map_err(|e| status_to_universal(e, path))?;

        Ok(response.into_inner().data)
    }

    /// `ReadBatch` RPC — multiple ranges from a single file.
    pub async fn read_batch(
        &self,
        collection_name: &str,
        shard_id: u32,
        path: &str,
        ranges: &[ReadRange],
    ) -> Result<Vec<Vec<u8>>> {
        let proto_ranges: Vec<_> = ranges
            .iter()
            .map(|r| qdrant::ReadBatchRange {
                byte_offset: r.byte_offset,
                length: r.length,
            })
            .collect();

        let response = self
            .inner
            .clone()
            .read_batch(qdrant::ReadBatchRequest {
                collection_name: collection_name.to_string(),
                shard_id,
                path: path.to_string(),
                ranges: proto_ranges,
            })
            .await
            .map_err(|e| status_to_universal(e, path))?;

        Ok(response.into_inner().data)
    }
}
