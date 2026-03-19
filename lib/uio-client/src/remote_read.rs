use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use common::universal_io::file_ops::UniversalReadFileOps;
use common::universal_io::{
    ElementsRange, FileIndex, OpenOptions, Result, UniversalIoError, UniversalRead,
};
use tokio::runtime::Handle;
use tonic::transport::Channel;

use crate::StorageReadClient;
use crate::generated::qdrant;

/// Universal I/O read implementation backed by a remote Qdrant node over gRPC.
///
/// Stateless design: every request carries `collection_name` + `path`.
/// Wire-compatible with the `StorageRead` gRPC service.
pub struct RemoteUniversalRead<T: Copy + 'static> {
    client: Arc<tokio::sync::Mutex<StorageReadClient<Channel>>>,
    collection_name: String,
    path: String,
    length: u64,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: Copy + 'static> RemoteUniversalRead<T> {
    /// Connect to a remote StorageRead service and resolve file length.
    pub async fn connect_and_open(
        endpoint: impl Into<String>,
        collection_name: impl Into<String>,
        path: impl Into<String>,
    ) -> Result<Self> {
        todo!()
    }

    /// Wrap an already-connected client.
    pub fn from_parts(
        client: Arc<tokio::sync::Mutex<StorageReadClient<Channel>>>,
        collection_name: String,
        path: String,
        length: u64,
    ) -> Self {
        todo!()
    }

    /// Get a reference to the shared client.
    pub fn client(&self) -> &Arc<tokio::sync::Mutex<StorageReadClient<Channel>>> {
        todo!()
    }

    /// List files on the remote node.
    pub async fn list_files_async(
        client: &Arc<tokio::sync::Mutex<StorageReadClient<Channel>>>,
        collection_name: &str,
        prefix_path: &str,
    ) -> Result<Vec<PathBuf>> {
        todo!()
    }

    /// Check if a file exists on the remote node.
    pub async fn file_exists_async(
        client: &Arc<tokio::sync::Mutex<StorageReadClient<Channel>>>,
        collection_name: &str,
        path: &str,
    ) -> Result<bool> {
        todo!()
    }

    fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        Handle::current().block_on(future)
    }
}

impl<T: Copy + 'static> UniversalReadFileOps for RemoteUniversalRead<T> {
    fn list_files(_prefix_path: &Path) -> Result<Vec<PathBuf>> {
        todo!()
    }

    fn exists(_path: &Path) -> Result<bool> {
        todo!()
    }
}

impl<T: Copy + 'static> UniversalRead<T> for RemoteUniversalRead<T> {
    fn open(_path: impl AsRef<Path>, _options: OpenOptions) -> Result<Self>
    where
        Self: Sized,
    {
        todo!()
    }

    fn read<const SEQUENTIAL: bool>(&self, range: ElementsRange) -> Result<Cow<'_, [T]>> {
        todo!()
    }

    fn read_whole(&self) -> Result<Cow<'_, [T]>> {
        todo!()
    }

    fn read_batch<const SEQUENTIAL: bool>(
        &self,
        ranges: impl IntoIterator<Item = ElementsRange>,
        mut callback: impl FnMut(usize, &[T]) -> Result<()>,
    ) -> Result<()> {
        todo!()
    }

    fn len(&self) -> Result<u64> {
        todo!()
    }

    fn populate(&self) -> Result<()> {
        todo!()
    }

    fn clear_ram_cache(&self) -> Result<()> {
        todo!()
    }

    fn read_multi<const SEQUENTIAL: bool>(
        files: &[Self],
        reads: impl IntoIterator<Item = (FileIndex, ElementsRange)>,
        mut callback: impl FnMut(usize, FileIndex, &[T]) -> Result<()>,
    ) -> Result<()>
    where
        Self: Sized,
    {
        todo!()
    }
}

/// Reinterpret raw bytes as a `Vec<T>`.
fn bytes_to_elements<T: Copy + 'static>(bytes: &[u8]) -> Vec<T> {
    todo!()
}
