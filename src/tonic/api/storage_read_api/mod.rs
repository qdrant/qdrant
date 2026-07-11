use std::pin::Pin;
use std::sync::Arc;

use api::grpc::qdrant::storage_read_server::StorageRead;
use api::grpc::qdrant::{
    FileExistsRequest, FileExistsResponse, FileLengthRequest, FileLengthResponse, ListFilesEntry,
    ListFilesRequest, ListFilesResponse, ReadBatchRequest, ReadBatchResponse, ReadBytesRequest,
    ReadBytesResponse, ReadBytesStreamRequest, ReadBytesStreamResponse, ReadWholeRequest,
    ReadWholeResponse,
};
use common::generic_consts::Random;
use common::mmap::{Advice, AdviceSetting};
use common::universal_io::{
    ListedFile, MmapFile, OpenOptions, Populate, ReadRange, UniversalIoError, UniversalRead,
    UniversalReadFileOps, UniversalReadFs,
};
use futures::Stream;
use storage::dispatcher::Dispatcher;
use tonic::{Request, Response, Status, async_trait};

use crate::tonic::api::storage_read_api::helpers::{io_error_to_status, validate_range};
use crate::tonic::api::validate;
use crate::tonic::auth::extract_auth;

mod helpers;
#[cfg(test)]
mod tests;

/// Chunk size for streaming reads (~1 MB).
const STREAM_CHUNK_SIZE: u64 = 1024 * 1024;

pub struct StorageReadService<S: UniversalRead + Send + Sync + 'static = MmapFile> {
    pub(super) dispatcher: Arc<Dispatcher>,
    pub(super) fs: Arc<S::Fs>,
}

#[async_trait]
impl<S: UniversalRead + Send + Sync + 'static> StorageRead for StorageReadService<S>
where
    S::Fs: Send + Sync + 'static,
{
    // Check if a file exists via UniversalRead::open(), catch NotFound → false.
    async fn file_exists(
        &self,
        mut request: Request<FileExistsRequest>,
    ) -> Result<Response<FileExistsResponse>, Status> {
        validate(request.get_ref())?;
        let auth = extract_auth(&mut request);
        let FileExistsRequest {
            collection_name,
            shard_id,
            path,
        } = request.into_inner();
        let (base, collections_root) = self
            .check_and_resolve_shard(&auth, &collection_name, shard_id, "file_exists")
            .await?;
        let path = Self::resolve_path(&base, &collections_root, &path)?;

        let fs = Arc::clone(&self.fs);
        let exists = tokio::task::spawn_blocking(move || match fs.exists(&path) {
            Ok(exists) => Ok(exists),
            Err(UniversalIoError::NotFound { .. }) => Ok(false),
            Err(UniversalIoError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(io_error_to_status(e)),
        })
        .await
        .map_err(|e| Status::internal(format!("Task join error: {e}")))??;

        Ok(Response::new(FileExistsResponse { exists }))
    }

    // List files via UniversalReadFileOps::list_files(prefix_path).
    // Return paths relative to the shard directory.
    async fn list_files(
        &self,
        mut request: Request<ListFilesRequest>,
    ) -> Result<Response<ListFilesResponse>, Status> {
        validate(request.get_ref())?;
        let auth = extract_auth(&mut request);
        let ListFilesRequest {
            collection_name,
            shard_id,
            prefix_path,
        } = request.into_inner();
        let (base, collections_root) = self
            .check_and_resolve_shard(&auth, &collection_name, shard_id, "list_files")
            .await?;
        let prefix_path = Self::resolve_path(&base, &collections_root, &prefix_path)?;

        let fs = Arc::clone(&self.fs);
        let files = tokio::task::spawn_blocking(move || fs.list_files(&prefix_path))
            .await
            .map_err(|e| Status::internal(format!("Task join error: {e}")))?
            .map_err(io_error_to_status)?;

        let files = files
            .into_iter()
            .filter_map(
                |ListedFile {
                     path,
                     size,
                     last_modified: _,
                 }| {
                    path.strip_prefix(&base).ok().map(|rel| {
                        // Always use forward slashes in gRPC responses regardless of OS.
                        let components = rel
                            .components()
                            .filter_map(|c| c.as_os_str().to_str())
                            .collect::<Vec<_>>();
                        ListFilesEntry {
                            path: components.join("/"),
                            size,
                        }
                    })
                },
            )
            .collect::<Vec<_>>();

        Ok(Response::new(ListFilesResponse { files }))
    }

    // Get file length via UniversalRead::open() → .len().
    async fn file_length(
        &self,
        mut request: Request<FileLengthRequest>,
    ) -> Result<Response<FileLengthResponse>, Status> {
        validate(request.get_ref())?;
        let auth = extract_auth(&mut request);
        let FileLengthRequest {
            collection_name,
            shard_id,
            path,
        } = request.into_inner();
        let (base, collections_root) = self
            .check_and_resolve_shard(&auth, &collection_name, shard_id, "file_length")
            .await?;
        let path = Self::resolve_path(&base, &collections_root, &path)?;

        let open_options = OpenOptions {
            writeable: false,
            need_sequential: false,
            populate: Populate::No,
            advice: AdviceSetting::Advice(Advice::Normal),
        };
        let fs = Arc::clone(&self.fs);
        let length = tokio::task::spawn_blocking(move || {
            let storage = fs
                .open(&path, open_options, Default::default())
                .map_err(io_error_to_status)?;
            storage.len::<u8>().map_err(io_error_to_status)
        })
        .await
        .map_err(|e| Status::internal(format!("Task join error: {e}")))??;

        Ok(Response::new(FileLengthResponse { length }))
    }

    // Maps to UniversalRead::read() — single range from a single file.
    async fn read_bytes(
        &self,
        mut request: Request<ReadBytesRequest>,
    ) -> Result<Response<ReadBytesResponse>, Status> {
        validate(request.get_ref())?;
        let auth = extract_auth(&mut request);
        let ReadBytesRequest {
            collection_name,
            shard_id,
            path,
            byte_offset,
            length,
        } = request.into_inner();

        let (base, collections_root) = self
            .check_and_resolve_shard(&auth, &collection_name, shard_id, "read_bytes")
            .await?;
        let path = Self::resolve_path(&base, &collections_root, &path)?;
        let open_options = OpenOptions {
            writeable: false,
            need_sequential: false,
            populate: Populate::No,
            advice: AdviceSetting::Advice(Advice::Normal),
        };

        let fs = Arc::clone(&self.fs);
        let data = tokio::task::spawn_blocking(move || {
            let storage = fs
                .open(&path, open_options, Default::default())
                .map_err(io_error_to_status)?;
            let cow = storage
                .read::<Random, u8>(ReadRange::new(byte_offset, length))
                .map_err(io_error_to_status)?;
            Ok::<_, Status>(cow.into_owned())
        })
        .await
        .map_err(|e| Status::internal(format!("Task join error: {e}")))??;

        Ok(Response::new(ReadBytesResponse { data }))
    }

    type ReadBytesStreamStream =
        Pin<Box<dyn Stream<Item = Result<ReadBytesStreamResponse, Status>> + Send>>;

    // Streaming variant of read() for large files.
    // Only individual chunk reads run on the blocking pool; backpressure does not pin a worker.
    async fn read_bytes_stream(
        &self,
        mut request: Request<ReadBytesStreamRequest>,
    ) -> Result<Response<Self::ReadBytesStreamStream>, Status> {
        validate(request.get_ref())?;
        let auth = extract_auth(&mut request);
        let ReadBytesStreamRequest {
            collection_name,
            shard_id,
            path,
            byte_offset,
            length,
        } = request.into_inner();

        let (base, collections_root) = self
            .check_and_resolve_shard(&auth, &collection_name, shard_id, "read_bytes_stream")
            .await?;
        let path = Self::resolve_path(&base, &collections_root, &path)?;
        let open_options = OpenOptions {
            writeable: false,
            need_sequential: false,
            populate: Populate::No,
            advice: AdviceSetting::Advice(Advice::Sequential),
        };
        let range = ReadRange::new(byte_offset, length);
        let fs = Arc::clone(&self.fs);
        let (storage, range) = tokio::task::spawn_blocking(move || {
            let s = fs
                .open(&path, open_options, Default::default())
                .map_err(io_error_to_status)?;
            let file_len = s.len::<u8>().map_err(io_error_to_status)?;
            validate_range(range, file_len).map_err(io_error_to_status)?;
            Ok::<_, Status>((s, range))
        })
        .await
        .map_err(|e| Status::internal(format!("Task join error: {e}")))??;

        let storage = Arc::new(storage);

        let stream = futures::stream::try_unfold(
            (storage, range.byte_offset, range.length),
            move |(storage, current_offset, remaining)| async move {
                if remaining == 0 {
                    return Ok(None);
                }

                let chunk_size = remaining.min(STREAM_CHUNK_SIZE);
                let storage_for_read = Arc::clone(&storage);

                let data = tokio::task::spawn_blocking(move || {
                    storage_for_read
                        .read::<Random, u8>(ReadRange::new(current_offset, chunk_size))
                        .map(|cow| cow.into_owned())
                        .map_err(io_error_to_status)
                })
                .await
                .map_err(|e| Status::internal(format!("Task join error: {e}")))??;

                Ok(Some((
                    ReadBytesStreamResponse { data },
                    (storage, current_offset + chunk_size, remaining - chunk_size),
                )))
            },
        );

        Ok(Response::new(Box::pin(stream)))
    }

    // Maps to UniversalRead::read_whole() — read an entire file.
    async fn read_whole(
        &self,
        mut request: Request<ReadWholeRequest>,
    ) -> Result<Response<ReadWholeResponse>, Status> {
        validate(request.get_ref())?;
        let auth = extract_auth(&mut request);
        let ReadWholeRequest {
            collection_name,
            shard_id,
            path,
        } = request.into_inner();
        let (base, collections_root) = self
            .check_and_resolve_shard(&auth, &collection_name, shard_id, "read_whole")
            .await?;
        let path = Self::resolve_path(&base, &collections_root, &path)?;
        let open_options = OpenOptions {
            writeable: false,
            need_sequential: false,
            populate: Populate::No,
            advice: AdviceSetting::Advice(Advice::Sequential),
        };

        let fs = Arc::clone(&self.fs);
        let data = tokio::task::spawn_blocking(move || {
            let storage = fs
                .open(&path, open_options, Default::default())
                .map_err(io_error_to_status)?;
            let cow = storage.read_whole::<u8>().map_err(io_error_to_status)?;
            Ok::<_, Status>(cow.into_owned())
        })
        .await
        .map_err(|e| Status::internal(format!("read_whole error: {e}")))??;

        Ok(Response::new(ReadWholeResponse { data }))
    }

    // Maps to UniversalRead::read_batch() — multiple ranges from a single file.
    async fn read_batch(
        &self,
        mut request: Request<ReadBatchRequest>,
    ) -> Result<Response<ReadBatchResponse>, Status> {
        validate(request.get_ref())?;
        let auth = extract_auth(&mut request);
        let ReadBatchRequest {
            collection_name,
            shard_id,
            path,
            ranges,
        } = request.into_inner();
        let (base, collections_root) = self
            .check_and_resolve_shard(&auth, &collection_name, shard_id, "read_batch")
            .await?;
        let path = Self::resolve_path(&base, &collections_root, &path)?;

        let open_options = OpenOptions {
            writeable: false,
            need_sequential: false,
            populate: Populate::No,
            advice: AdviceSetting::Advice(Advice::Normal),
        };
        let ranges = ranges
            .iter()
            .map(|r| ReadRange::new(r.byte_offset, r.length))
            .collect::<Vec<_>>();

        let fs = Arc::clone(&self.fs);
        let data = tokio::task::spawn_blocking(move || {
            let storage = fs
                .open(&path, open_options, Default::default())
                .map_err(io_error_to_status)?;
            let mut results = ranges.iter().map(|_| Vec::new()).collect::<Vec<_>>();
            storage
                .read_batch::<Random, u8, _>(ranges.into_iter().enumerate(), |idx, chunk| {
                    results[idx].extend_from_slice(chunk);
                    Ok(())
                })
                .map_err(io_error_to_status)?;

            Ok::<_, Status>(results)
        })
        .await
        .map_err(|e| Status::internal(format!("Task join error: {e}")))??;

        Ok(Response::new(ReadBatchResponse { data }))
    }
}
