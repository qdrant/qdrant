use std::collections::HashMap;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use api::grpc::qdrant::storage_read_server::StorageRead;
use api::grpc::qdrant::{
    FileExistsRequest, FileExistsResponse, FileLengthRequest, FileLengthResponse, ListFilesRequest,
    ListFilesResponse, ReadBatchRequest, ReadBatchResponse, ReadBytesRequest, ReadBytesResponse,
    ReadBytesStreamRequest, ReadBytesStreamResponse, ReadMultiRequest, ReadMultiResponse,
    ReadWholeRequest, ReadWholeResponse,
};
use common::generic_consts::Random;
use common::universal_io::mmap::MmapFile;
use common::universal_io::{FileIndex, OpenOptions, ReadRange, UniversalIoError, UniversalRead};
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

pub struct StorageReadService<S: UniversalRead<u8> + Send + Sync + 'static = MmapFile> {
    dispatcher: Arc<Dispatcher>,
    _marker: PhantomData<S>,
}

#[async_trait]
impl<S: UniversalRead<u8> + Send + Sync + 'static> StorageRead for StorageReadService<S> {
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

        let exists = tokio::task::spawn_blocking(move || match S::exists(&path) {
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

        let paths = tokio::task::spawn_blocking(move || S::list_files(&prefix_path))
            .await
            .map_err(|e| Status::internal(format!("Task join error: {e}")))?
            .map_err(io_error_to_status)?;

        let relative_paths = paths
            .into_iter()
            .filter_map(|p| {
                p.strip_prefix(&base).ok().map(|rel| {
                    // Always use forward slashes in gRPC responses regardless of OS.
                    let components = rel
                        .components()
                        .filter_map(|c| c.as_os_str().to_str())
                        .collect::<Vec<_>>();
                    components.join("/")
                })
            })
            .collect::<Vec<_>>();

        Ok(Response::new(ListFilesResponse {
            paths: relative_paths,
        }))
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

        let open_options = OpenOptions::default();
        let length = tokio::task::spawn_blocking(move || {
            let storage = S::open(&path, open_options).map_err(io_error_to_status)?;
            storage.len().map_err(io_error_to_status)
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
        let open_options = OpenOptions::default();

        let data = tokio::task::spawn_blocking(move || {
            let storage = S::open(&path, open_options).map_err(io_error_to_status)?;
            let cow = storage
                .read::<Random>(ReadRange::new(byte_offset, length))
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
        let open_options = OpenOptions::default();
        let range = ReadRange::new(byte_offset, length);
        let (storage, range) = tokio::task::spawn_blocking(move || {
            let s = S::open(&path, open_options).map_err(io_error_to_status)?;
            let file_len = s.len().map_err(io_error_to_status)?;
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
                        .read::<Random>(ReadRange::new(current_offset, chunk_size))
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
        let open_options = OpenOptions::default();

        let data = tokio::task::spawn_blocking(move || {
            let storage = S::open(&path, open_options).map_err(io_error_to_status)?;
            let cow = storage.read_whole().map_err(io_error_to_status)?;
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

        let open_options = OpenOptions::default();
        let ranges = ranges
            .iter()
            .map(|r| ReadRange::new(r.byte_offset, r.length))
            .collect::<Vec<_>>();

        let data = tokio::task::spawn_blocking(move || {
            let storage = S::open(&path, open_options).map_err(io_error_to_status)?;
            let mut results = ranges.iter().map(|_| Vec::new()).collect::<Vec<_>>();
            storage
                .read_batch::<Random, _>(ranges.into_iter().enumerate(), |idx, chunk| {
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

    // Maps to UniversalRead::read_multi() — ranges across multiple files.
    // Deduplicate paths into a file index, open each unique file once, then call read_multi.
    async fn read_multi(
        &self,
        mut request: Request<ReadMultiRequest>,
    ) -> Result<Response<ReadMultiResponse>, Status> {
        validate(request.get_ref())?;
        let auth = extract_auth(&mut request);
        let ReadMultiRequest {
            collection_name,
            shard_id,
            reads,
        } = request.into_inner();
        let (base, collections_root) = self
            .check_and_resolve_shard(&auth, &collection_name, shard_id, "read_multi")
            .await?;
        let open_options = OpenOptions::default();

        // Resolve all paths and deduplicate into a file index.
        let mut path_to_index = HashMap::<PathBuf, FileIndex>::new();
        let mut unique_paths = Vec::<PathBuf>::new();
        let mut reads_ = Vec::<(FileIndex, _)>::with_capacity(reads.len());

        for entry in &reads {
            let resolved = Self::resolve_path(&base, &collections_root, &entry.path)?;
            let file_index = *path_to_index.entry(resolved.clone()).or_insert_with(|| {
                let idx = unique_paths.len();
                unique_paths.push(resolved);
                idx
            });
            reads_.push((file_index, ReadRange::new(entry.byte_offset, entry.length)));
        }

        let data = tokio::task::spawn_blocking(move || {
            let files = unique_paths
                .iter()
                .map(|p| S::open(p, open_options))
                .collect::<common::universal_io::Result<Vec<_>>>()
                .map_err(io_error_to_status)?;

            let mut results = vec![Vec::new(); reads_.len()];

            let reads = reads_
                .into_iter()
                .enumerate()
                .map(|(op_idx, (file_idx, range))| (op_idx, &files[file_idx], range));

            S::read_multi::<Random, _>(reads, |op_idx, chunk| {
                results[op_idx].extend_from_slice(chunk);
                Ok(())
            })
            .map_err(io_error_to_status)?;
            Ok::<Vec<Vec<u8>>, Status>(results)
        })
        .await
        .map_err(|e| Status::internal(format!("Task join error: {e}")))??;

        Ok(Response::new(ReadMultiResponse { data }))
    }
}
