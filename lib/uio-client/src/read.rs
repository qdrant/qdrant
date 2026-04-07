use std::path::PathBuf;

use common::universal_io::{ReadRange, Result, UniversalIoError};
use tonic::transport::Channel;

use crate::StorageReadClient;
use crate::generated::qdrant;

fn tonic_err(e: tonic::Status) -> UniversalIoError {
    UniversalIoError::Io(std::io::Error::other(e))
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
    inner: StorageReadClient<Channel>,
}

impl Client {
    /// Connect to a remote StorageRead gRPC service.
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self> {
        let client = StorageReadClient::connect(endpoint.into())
            .await
            .map_err(|e| UniversalIoError::Io(std::io::Error::other(e)))?;

        Ok(Self { inner: client })
    }

    /// `ListFiles` RPC.
    pub async fn list_files(
        &self,
        collection_name: &str,
        prefix_path: &str,
    ) -> Result<Vec<PathBuf>> {
        let response = self
            .inner
            .clone()
            .list_files(qdrant::ListFilesRequest {
                collection_name: collection_name.to_string(),
                prefix_path: prefix_path.to_string(),
            })
            .await
            .map_err(tonic_err)?;

        Ok(response
            .into_inner()
            .paths
            .into_iter()
            .map(PathBuf::from)
            .collect())
    }

    /// `FileExists` RPC.
    pub async fn file_exists(&self, collection_name: &str, path: &str) -> Result<bool> {
        let response = self
            .inner
            .clone()
            .file_exists(qdrant::FileExistsRequest {
                collection_name: collection_name.to_string(),
                path: path.to_string(),
            })
            .await
            .map_err(tonic_err)?;

        Ok(response.into_inner().exists)
    }

    /// `FileLength` RPC.
    pub async fn file_length(&self, collection_name: &str, path: &str) -> Result<u64> {
        let response = self
            .inner
            .clone()
            .file_length(qdrant::FileLengthRequest {
                collection_name: collection_name.to_string(),
                path: path.to_string(),
            })
            .await
            .map_err(tonic_err)?;

        Ok(response.into_inner().length)
    }

    /// `ReadBytes` RPC — single range from a single file.
    pub async fn read_bytes(
        &self,
        collection_name: &str,
        path: &str,
        byte_offset: u64,
        length: u64,
    ) -> Result<Vec<u8>> {
        let response = self
            .inner
            .clone()
            .read_bytes(qdrant::ReadBytesRequest {
                collection_name: collection_name.to_string(),
                path: path.to_string(),
                byte_offset,
                length,
            })
            .await
            .map_err(tonic_err)?;

        Ok(response.into_inner().data)
    }

    /// `ReadBytesStream` RPC — streaming variant for large ranges (~1 MB chunks).
    pub async fn read_bytes_stream(
        &self,
        collection_name: &str,
        path: &str,
        byte_offset: u64,
        length: u64,
    ) -> Result<Vec<u8>> {
        let mut stream = self
            .inner
            .clone()
            .read_bytes_stream(qdrant::ReadBytesStreamRequest {
                collection_name: collection_name.to_string(),
                path: path.to_string(),
                byte_offset,
                length,
            })
            .await
            .map_err(tonic_err)?
            .into_inner();

        let mut data = Vec::with_capacity(length as usize);
        while let Some(chunk) = stream.message().await.map_err(tonic_err)? {
            data.extend_from_slice(&chunk.data);
        }
        Ok(data)
    }

    /// `ReadWhole` RPC — read an entire file.
    pub async fn read_whole(&self, collection_name: &str, path: &str) -> Result<Vec<u8>> {
        let response = self
            .inner
            .clone()
            .read_whole(qdrant::ReadWholeRequest {
                collection_name: collection_name.to_string(),
                path: path.to_string(),
            })
            .await
            .map_err(tonic_err)?;

        Ok(response.into_inner().data)
    }

    /// `ReadBatch` RPC — multiple ranges from a single file.
    pub async fn read_batch(
        &self,
        collection_name: &str,
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
                path: path.to_string(),
                ranges: proto_ranges,
            })
            .await
            .map_err(tonic_err)?;

        Ok(response.into_inner().data)
    }

    /// `ReadMulti` RPC — ranges across multiple files.
    pub async fn read_multi(
        &self,
        collection_name: &str,
        reads: &[(impl AsRef<str>, ReadRange)],
    ) -> Result<Vec<Vec<u8>>> {
        let proto_reads: Vec<_> = reads
            .iter()
            .map(|(path, range)| qdrant::ReadMultiEntry {
                path: path.as_ref().to_string(),
                byte_offset: range.byte_offset,
                length: range.length,
            })
            .collect();

        let response = self
            .inner
            .clone()
            .read_multi(qdrant::ReadMultiRequest {
                collection_name: collection_name.to_string(),
                reads: proto_reads,
            })
            .await
            .map_err(tonic_err)?;

        Ok(response.into_inner().data)
    }
}
