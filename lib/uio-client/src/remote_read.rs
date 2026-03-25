use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use common::generic_consts::AccessPattern;
use common::universal_io::file_ops::UniversalReadFileOps;
use common::universal_io::{
    FileIndex, OpenOptions, ReadRange, Result, UniversalIoError, UniversalRead,
};
use tokio::runtime::Handle;
use tonic::transport::Channel;

use crate::StorageReadClient;
use crate::generated::qdrant;

/// Shared gRPC connection to a remote StorageRead service.
///
/// Cheap to clone (wraps an `Arc`). Use [`open_file`](Self::open_file) to
/// create [`RemoteUniversalRead`] handles over this connection.
#[derive(Clone)]
pub struct RemoteClient {
    inner: Arc<tokio::sync::Mutex<StorageReadClient<Channel>>>,
}

impl RemoteClient {
    /// Connect to a remote StorageRead gRPC service.
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self> {
        let client = StorageReadClient::connect(endpoint.into())
            .await
            .map_err(|e| UniversalIoError::Io(std::io::Error::other(e)))?;

        Ok(Self {
            inner: Arc::new(tokio::sync::Mutex::new(client)),
        })
    }

    /// Open a file on the remote node, fetching its length.
    pub async fn open_file<T: Copy + 'static>(
        &self,
        collection_name: impl Into<String>,
        path: impl Into<String>,
    ) -> Result<RemoteUniversalRead<T>> {
        let collection_name = collection_name.into();
        let path = path.into();

        let length = self
            .inner
            .lock()
            .await
            .file_length(qdrant::FileLengthRequest {
                collection_name: collection_name.clone(),
                path: path.clone(),
            })
            .await
            .map_err(|e| UniversalIoError::Io(std::io::Error::other(e)))?
            .into_inner()
            .length;

        Ok(RemoteUniversalRead {
            client: self.clone(),
            collection_name,
            path,
            length,
            _phantom: std::marker::PhantomData,
        })
    }

    /// List files on the remote node.
    pub async fn list_files(
        &self,
        collection_name: &str,
        prefix_path: &str,
    ) -> Result<Vec<PathBuf>> {
        let response = self
            .inner
            .lock()
            .await
            .list_files(qdrant::ListFilesRequest {
                collection_name: collection_name.to_string(),
                prefix_path: prefix_path.to_string(),
            })
            .await
            .map_err(|e| UniversalIoError::Io(std::io::Error::other(e)))?;

        Ok(response
            .into_inner()
            .paths
            .into_iter()
            .map(PathBuf::from)
            .collect())
    }

    /// Check if a file exists on the remote node.
    pub async fn file_exists(&self, collection_name: &str, path: &str) -> Result<bool> {
        let response = self
            .inner
            .lock()
            .await
            .file_exists(qdrant::FileExistsRequest {
                collection_name: collection_name.to_string(),
                path: path.to_string(),
            })
            .await
            .map_err(|e| UniversalIoError::Io(std::io::Error::other(e)))?;

        Ok(response.into_inner().exists)
    }
}

/// Universal I/O read implementation backed by a remote Qdrant node over gRPC.
///
/// Stateless design: every request carries `collection_name` + `path`.
/// Wire-compatible with the `StorageRead` gRPC service.
pub struct RemoteUniversalRead<T: Copy + 'static> {
    client: RemoteClient,
    collection_name: String,
    path: String,
    length: u64,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: Copy + 'static> RemoteUniversalRead<T> {
    /// Connect to a remote StorageRead service and open a file in one step.
    pub async fn connect_and_open(
        endpoint: impl Into<String>,
        collection_name: impl Into<String>,
        path: impl Into<String>,
    ) -> Result<Self> {
        let client = RemoteClient::connect(endpoint).await?;
        client.open_file(collection_name, path).await
    }

    /// Wrap an already-connected client with known file metadata.
    pub fn from_parts(
        client: RemoteClient,
        collection_name: String,
        path: String,
        length: u64,
    ) -> Self {
        Self {
            client,
            collection_name,
            path,
            length,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Get a reference to the underlying shared client.
    pub fn client(&self) -> &RemoteClient {
        &self.client
    }

    fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        Handle::current().block_on(future)
    }
}

impl<T: Copy + 'static> UniversalReadFileOps for RemoteUniversalRead<T> {
    fn list_files(_prefix_path: &Path) -> Result<Vec<PathBuf>> {
        Err(UniversalIoError::Io(std::io::Error::other(
            "RemoteUniversalRead::list_files requires an async context; use RemoteClient::list_files()",
        )))
    }

    fn exists(_path: &Path) -> Result<bool> {
        Err(UniversalIoError::Io(std::io::Error::other(
            "RemoteUniversalRead::exists requires an async context; use RemoteClient::file_exists()",
        )))
    }
}

impl<T: Copy + 'static> UniversalRead<T> for RemoteUniversalRead<T> {
    fn open(_path: impl AsRef<Path>, _options: OpenOptions) -> Result<Self>
    where
        Self: Sized,
    {
        Err(UniversalIoError::Io(std::io::Error::other(
            "RemoteUniversalRead::open is not supported; use RemoteClient::open_file()",
        )))
    }

    fn read<P: AccessPattern>(&self, range: ReadRange) -> Result<Cow<'_, [T]>> {
        let data = self
            .block_on(async {
                self.client
                    .inner
                    .lock()
                    .await
                    .read_bytes(qdrant::ReadBytesRequest {
                        collection_name: self.collection_name.clone(),
                        path: self.path.clone(),
                        offset: range.byte_offset,
                        length: range.length,
                    })
                    .await
            })
            .map_err(|e| UniversalIoError::Io(std::io::Error::other(e)))?
            .into_inner()
            .data;

        Ok(Cow::Owned(bytes_to_elements(&data)))
    }

    fn read_whole(&self) -> Result<Cow<'_, [T]>> {
        let data = self
            .block_on(async {
                self.client
                    .inner
                    .lock()
                    .await
                    .read_whole(qdrant::ReadWholeRequest {
                        collection_name: self.collection_name.clone(),
                        path: self.path.clone(),
                    })
                    .await
            })
            .map_err(|e| UniversalIoError::Io(std::io::Error::other(e)))?
            .into_inner()
            .data;

        Ok(Cow::Owned(bytes_to_elements(&data)))
    }

    fn read_batch<P: AccessPattern>(
        &self,
        ranges: impl IntoIterator<Item = ReadRange>,
        mut callback: impl FnMut(usize, &[T]) -> Result<()>,
    ) -> Result<()> {
        let proto_ranges: Vec<_> = ranges
            .into_iter()
            .map(|r| qdrant::ReadBatchRange {
                offset: r.byte_offset,
                length: r.length,
            })
            .collect();

        let response = self
            .block_on(async {
                self.client
                    .inner
                    .lock()
                    .await
                    .read_batch(qdrant::ReadBatchRequest {
                        collection_name: self.collection_name.clone(),
                        path: self.path.clone(),
                        ranges: proto_ranges,
                    })
                    .await
            })
            .map_err(|e| UniversalIoError::Io(std::io::Error::other(e)))?;

        for (index, chunk) in response.into_inner().data.into_iter().enumerate() {
            let elements = bytes_to_elements::<T>(&chunk);
            callback(index, &elements)?;
        }

        Ok(())
    }

    fn len(&self) -> Result<u64> {
        Ok(self.length)
    }

    fn populate(&self) -> Result<()> {
        Ok(())
    }

    fn clear_ram_cache(&self) -> Result<()> {
        Ok(())
    }

    fn read_multi<P: AccessPattern>(
        files: &[Self],
        reads: impl IntoIterator<Item = (FileIndex, ReadRange)>,
        mut callback: impl FnMut(usize, FileIndex, &[T]) -> Result<()>,
    ) -> Result<()>
    where
        Self: Sized,
    {
        let first = files.first().ok_or_else(|| {
            UniversalIoError::Io(std::io::Error::other("read_multi called with no files"))
        })?;

        let entries: Vec<_> = reads
            .into_iter()
            .map(|(file_index, range)| {
                let file = files
                    .get(file_index)
                    .ok_or(UniversalIoError::InvalidFileIndex {
                        file_index,
                        files: files.len(),
                    })?;
                Ok((
                    file_index,
                    qdrant::ReadMultiEntry {
                        path: file.path.clone(),
                        offset: range.byte_offset,
                        length: range.length,
                    },
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        let file_indices: Vec<_> = entries.iter().map(|(fi, _)| *fi).collect();
        let proto_entries: Vec<_> = entries.into_iter().map(|(_, e)| e).collect();

        let response = first
            .block_on(async {
                first
                    .client
                    .inner
                    .lock()
                    .await
                    .read_multi(qdrant::ReadMultiRequest {
                        collection_name: first.collection_name.clone(),
                        reads: proto_entries,
                    })
                    .await
            })
            .map_err(|e| UniversalIoError::Io(std::io::Error::other(e)))?;

        for (op_index, chunk) in response.into_inner().data.into_iter().enumerate() {
            let elements = bytes_to_elements::<T>(&chunk);
            callback(op_index, file_indices[op_index], &elements)?;
        }

        Ok(())
    }
}

/// Reinterpret raw bytes as a `Vec<T>`.
fn bytes_to_elements<T: Copy + 'static>(bytes: &[u8]) -> Vec<T> {
    let element_size = std::mem::size_of::<T>();
    if element_size == 0 {
        return Vec::new();
    }
    debug_assert_eq!(
        bytes.len() % element_size,
        0,
        "byte count not a multiple of element size"
    );
    let count = bytes.len() / element_size;
    let mut result = Vec::with_capacity(count);
    // SAFETY: T is Copy, result has capacity for `count` elements,
    // and we copy as raw bytes so the source pointer only needs u8 alignment.
    unsafe {
        std::ptr::copy_nonoverlapping(
            bytes.as_ptr(),
            result.as_mut_ptr() as *mut u8,
            count * element_size,
        );
        result.set_len(count);
    }
    result
}
