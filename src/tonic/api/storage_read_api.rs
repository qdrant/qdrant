use std::marker::PhantomData;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use api::grpc::ReadBatchRange;
use api::grpc::qdrant::storage_read_server::StorageRead;
use api::grpc::qdrant::{
    FileExistsRequest, FileExistsResponse, FileLengthRequest, FileLengthResponse, ListFilesRequest,
    ListFilesResponse, MmapAdvice, ReadBatchRequest, ReadBatchResponse, ReadBytesRequest,
    ReadBytesResponse, ReadBytesStreamRequest, ReadBytesStreamResponse, ReadMultiRequest,
    ReadMultiResponse, ReadWholeRequest, ReadWholeResponse, StorageOpenOptions,
};
use collection::operations::verification::new_unchecked_verification_pass;
use common::mmap::AdviceSetting;
use common::universal_io::mmap::MmapUniversal;
use common::universal_io::{
    ElementsRange, FileIndex, OpenOptions, UniversalIoError, UniversalRead,
};
use futures::Stream;
use storage::content_manager::toc::COLLECTIONS_DIR;
use storage::dispatcher::Dispatcher;
use tokio::sync::mpsc;
use tonic::{Request, Response, Status, async_trait};

use crate::tonic::api::validate;
use crate::tonic::auth::extract_auth;

/// Chunk size for streaming reads (~1 MB).
const STREAM_CHUNK_SIZE: u64 = 1024 * 1024;

pub struct StorageReadService<S: UniversalRead<u8> + Send + Sync + 'static = MmapUniversal<u8>> {
    dispatcher: Arc<Dispatcher>,
    _marker: PhantomData<S>,
}

impl<S: UniversalRead<u8> + Send + Sync + 'static> StorageReadService<S> {
    pub fn new(dispatcher: Arc<Dispatcher>) -> Self {
        Self {
            dispatcher,
            _marker: PhantomData,
        }
    }

    /// Resolve a collection-scoped relative path to an absolute path,
    /// rejecting any traversal attempts.
    fn resolve_path(
        &self,
        auth: &storage::rbac::Auth,
        collection_name: &str,
        relative_path: &str,
    ) -> Result<PathBuf, Status> {
        todo!()
    }
}

/// Convert UniversalIoError to tonic Status.
fn io_error_to_status(e: UniversalIoError) -> Status {
    match e {
        UniversalIoError::Io(e) => Status::internal(format!("I/O error: {e}")),
        UniversalIoError::Mmap(e) => Status::internal(format!("Mmap error: {e}")),
        UniversalIoError::OutOfBounds {
            start,
            end,
            data_length,
        } => Status::out_of_range(format!(
            "Range {start}..{end} out of bounds (size: {data_length}"
        )),
        UniversalIoError::SerdeJson(e) => Status::internal(format!("Serialization error: {e}")),
        UniversalIoError::NotFound { path } => {
            Status::not_found(format!("File not found: {}", path.display()))
        }
        UniversalIoError::InvalidFileIndex {
            file_index,
            num_files,
        } => Status::internal(format!(
            "Invalid file index: {file_index} (num_files: {num_files})"
        )),
    }
}

/// Convert proto `StorageOpenOptions` to Rust `OpenOptions`.
fn convert_open_options(proto: Option<StorageOpenOptions>) -> OpenOptions {
    let Some(opts) = proto else {
        return OpenOptions::default();
    };
    OpenOptions {
        need_sequential: opts.need_sequential,
        disk_parallel: opts.disk_parallel.map(|v| v as usize),
        populate: opts.populate,
        advice: opts.advice.and_then(|v| {
            MmapAdvice::try_from(v).ok().map(|a| {
                AdviceSetting::Advice(match a {
                    MmapAdvice::Normal => common::mmap::Advice::Normal,
                    MmapAdvice::Random => common::mmap::Advice::Random,
                    MmapAdvice::Sequential => common::mmap::Advice::Sequential,
                })
            })
        }),
    }
}

/// Dispatch a read call on `MmapU8` based on a runtime `sequential` flag.
/// Needed because `UniversalRead::read` uses a const generic parameter.
fn dispatch_read<S: UniversalRead<u8>>(
    storage: &S,
    range: ElementsRange,
    sequential: bool,
) -> common::universal_io::Result<std::borrow::Cow<'_, [u8]>> {
    if sequential {
        storage.read::<true>(range)
    } else {
        storage.read::<false>(range)
    }
}

/// Dispatch a read_batch call based on a runtime `sequential` flag.
fn dispatch_read_batch<S: UniversalRead<u8>>(
    storage: &S,
    ranges: impl IntoIterator<Item = ElementsRange>,
    sequential: bool,
    callback: impl FnMut(usize, &[u8]) -> common::universal_io::Result<()>,
) -> common::universal_io::Result<()> {
    if sequential {
        storage.read_batch::<true>(ranges, callback)
    } else {
        storage.read_batch::<false>(ranges, callback)
    }
}

/// Dispatch a read_multi call based on a runtime `sequential` flag.
fn dispatch_read_multi<S: UniversalRead<u8>>(
    files: &[S],
    reads: impl IntoIterator<Item = (FileIndex, ElementsRange)>,
    sequential: bool,
    callback: impl FnMut(usize, FileIndex, &[u8]) -> common::universal_io::Result<()>,
) -> common::universal_io::Result<()> {
    if sequential {
        S::read_multi::<true>(files, reads, callback)
    } else {
        S::read_multi::<false>(files, reads, callback)
    }
}

/// A wrapper around `mpsc::Receiver` that implements `Stream`.
struct MpscStream<T>(mpsc::Receiver<T>);

impl<T> Stream for MpscStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_recv(cx)
    }
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
            path,
            open_options,
        } = request.into_inner();
        let path = self.resolve_path(&auth, &collection_name, &path)?;

        let open_options = convert_open_options(open_options);

        let exists = tokio::task::spawn_blocking(move || match S::open(&path, open_options) {
            Ok(_) => true,
            Err(UniversalIoError::NotFound { .. }) => false,
            _ => false,
        })
        .await
        .map_err(|e| Status::internal(format!("Task join error: {e}")))?;

        Ok(Response::new(FileExistsResponse { exists }))
    }

    // List files via UniversalReadFileOps::list_files(prefix_path).
    // Return paths relative to the collection directory.
    async fn list_files(
        &self,
        mut request: Request<ListFilesRequest>,
    ) -> Result<Response<ListFilesResponse>, Status> {
        validate(request.get_ref())?;
        let auth = extract_auth(&mut request);
        let ListFilesRequest {
            collection_name,
            prefix_path,
        } = request.into_inner();
        let prefix_path = self.resolve_path(&auth, &collection_name, &prefix_path)?;

        // Compute the base collection path to make results relative.
        let pass = new_unchecked_verification_pass();
        let toc = self.dispatcher.toc(&auth, &pass);
        let base = toc
            .storage_path()
            .join(COLLECTIONS_DIR)
            .join(&collection_name);

        let paths = tokio::task::spawn_blocking(move || S::list_files(&prefix_path))
            .await
            .map_err(|e| Status::internal(format!("Task join error: {e}")))?
            .map_err(io_error_to_status)?;

        let relative_paths = paths
            .into_iter()
            .filter_map(|p| {
                p.strip_prefix(&base)
                    .ok()
                    .and_then(|p| p.to_str())
                    .map(String::from)
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
            path,
            open_options,
        } = request.into_inner();
        let path = self.resolve_path(&auth, &collection_name, &path)?;

        let open_options = convert_open_options(open_options);
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
            path,
            offset,
            length,
            open_options,
        } = request.into_inner();

        let path = self.resolve_path(&auth, &collection_name, &path)?;
        let open_options = convert_open_options(open_options);

        let data = tokio::task::spawn_blocking(move || {
            let storage = S::open(&path, open_options).map_err(io_error_to_status)?;
            let cow = dispatch_read(
                &storage,
                ElementsRange {
                    start: offset,
                    length,
                },
                open_options.need_sequential,
            )
            .map_err(io_error_to_status)?;
            Ok::<_, Status>(cow.into_owned())
        })
        .await
        .map_err(|e| Status::internal(format!("Task join error: {e}")))??;

        Ok(Response::new(ReadBytesResponse { data }))
    }

    type ReadBytesStreamStream =
        Pin<Box<dyn Stream<Item = Result<ReadBytesStreamResponse, Status>> + Send>>;

    // Streaming variant of read() for large files, sends ~1MB chunks via mpsc channel.
    // Use spawn_blocking + MpscStream adapter.
    async fn read_bytes_stream(
        &self,
        mut request: Request<ReadBytesStreamRequest>,
    ) -> Result<Response<Self::ReadBytesStreamStream>, Status> {
        validate(request.get_ref())?;
        let auth = extract_auth(&mut request);
        let ReadBytesStreamRequest {
            collection_name,
            path,
            offset,
            length,
            open_options,
        } = request.into_inner();

        let path = self.resolve_path(&auth, &collection_name, &path)?;
        let open_options = convert_open_options(open_options);

        let (tx, rx) = mpsc::channel(4);

        tokio::task::spawn_blocking(move || {
            let storage = match S::open(&path, open_options) {
                Ok(storage) => storage,
                Err(e) => {
                    let _ = tx.blocking_send(Err(io_error_to_status(e)));
                    return;
                }
            };

            let mut remaining = length;
            let mut current_offset = offset;

            while remaining > 0 {
                let chunk_size = remaining.min(STREAM_CHUNK_SIZE);
                let result = dispatch_read(
                    &storage,
                    ElementsRange {
                        start: current_offset,
                        length: chunk_size,
                    },
                    open_options.need_sequential,
                );

                match result {
                    Ok(cow) => {
                        let data = cow.into_owned();
                        if tx
                            .blocking_send(Ok(ReadBytesStreamResponse { data }))
                            .is_err()
                        {
                            return; // Client disconnected
                        }
                    }
                    Err(e) => {
                        let _ = tx.blocking_send(Err(io_error_to_status(e)));
                        return;
                    }
                }

                current_offset += chunk_size;
                remaining -= chunk_size;
            }
        });

        let stream = MpscStream(rx);
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
            path,
            open_options,
        } = request.into_inner();
        let path = self.resolve_path(&auth, &collection_name, &path)?;
        let open_options = convert_open_options(open_options);

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
            path,
            ranges,
            open_options,
        } = request.into_inner();
        let path = self.resolve_path(&auth, &collection_name, &path)?;

        let open_options = convert_open_options(open_options);
        let ranges = ranges
            .iter()
            .map(|r| ElementsRange {
                start: r.offset,
                length: r.length,
            })
            .collect::<Vec<_>>();

        let data = tokio::task::spawn_blocking(move || {
            let storage = S::open(&path, open_options).map_err(io_error_to_status)?;
            let mut results = vec![Vec::<u8>::new(); ranges.len()];
            dispatch_read_batch(
                &storage,
                ranges,
                open_options.need_sequential,
                |idx, chunk| {
                    results[idx].extend_from_slice(chunk);
                    Ok(())
                },
            )
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
        let inner = request.into_inner();

        todo!()
    }
}
