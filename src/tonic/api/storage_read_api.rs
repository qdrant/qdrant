use std::marker::PhantomData;
use std::path::{Component, Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

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
use common::universal_io::{ElementsRange, OpenOptions, UniversalIoError, UniversalRead};
use futures::Stream;
use storage::content_manager::toc::COLLECTIONS_DIR;
use storage::dispatcher::Dispatcher;
use tokio::sync::mpsc;
use tokio_util::io::simplex::new;
use tonic::{Request, Response, Status, async_trait};

use crate::tonic::api::validate;
use crate::tonic::auth::extract_auth;

/// Chunk size for streaming reads (~1 MB).
const STREAM_CHUNK_SIZE: usize = 1024 * 1024;

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
fn universal_io_error_to_status(e: UniversalIoError) -> Status {
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

#[async_trait]
impl<S: UniversalRead<u8> + Send + Sync + 'static> StorageRead for StorageReadService<S> {
    // Check if a file exists via UniversalRead::open(), catch NotFound → false.
    async fn file_exists(
        &self,
        mut request: Request<FileExistsRequest>,
    ) -> Result<Response<FileExistsResponse>, Status> {
        validate(request.get_ref())?;
        let auth = extract_auth(&mut request);
        let inner = request.into_inner();
        let path = self.resolve_path(&auth, &inner.collection_name, &inner.path)?;

        let open_options = convert_open_options(inner.open_options);

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
        let inner = request.into_inner();
        let prefix_path = self.resolve_path(&auth, &inner.collection_name, &inner.prefix_path)?;

        // Compute the base collection path to make results relative.
        let pass = new_unchecked_verification_pass();
        let toc = self.dispatcher.toc(&auth, &pass);
        let base = toc
            .storage_path()
            .join(COLLECTIONS_DIR)
            .join(&inner.collection_name);

        let paths = tokio::task::spawn_blocking(move || S::list_files(&prefix_path))
            .await
            .map_err(|e| Status::internal(format!("Task join error: {e}")))?
            .map_err(universal_io_error_to_status)?;

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
        let inner = request.into_inner();
        let path = self.resolve_path(&auth, &inner.collection_name, &inner.path)?;

        let open_options = convert_open_options(inner.open_options);
        let length = tokio::task::spawn_blocking(move || {
            let storage = S::open(&path, open_options).map_err(universal_io_error_to_status)?;
            storage.len().map_err(universal_io_error_to_status)
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
        let inner = request.into_inner();

        let path = self.resolve_path(&auth, &inner.collection_name, &inner.path)?;
        let offset = inner.offset;
        let length = inner.length;
        let open_options = convert_open_options(inner.open_options);

        let data = tokio::task::spawn_blocking(move || {
            let storage = S::open(&path, open_options).map_err(universal_io_error_to_status)?;
            let cow = dispatch_read(
                &storage,
                ElementsRange {
                    start: offset,
                    length,
                },
                open_options.need_sequential,
            )
            .map_err(universal_io_error_to_status)?;
            Ok::<Vec<u8>, Status>(cow.into_owned())
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
        todo!()
    }

    // Maps to UniversalRead::read_whole() — read an entire file.
    async fn read_whole(
        &self,
        mut request: Request<ReadWholeRequest>,
    ) -> Result<Response<ReadWholeResponse>, Status> {
        todo!()
    }

    // Maps to UniversalRead::read_batch() — multiple ranges from a single file.
    async fn read_batch(
        &self,
        mut request: Request<ReadBatchRequest>,
    ) -> Result<Response<ReadBatchResponse>, Status> {
        todo!()
    }

    // Maps to UniversalRead::read_multi() — ranges across multiple files.
    // Deduplicate paths into a file index, open each unique file once, then call read_multi.
    async fn read_multi(
        &self,
        mut request: Request<ReadMultiRequest>,
    ) -> Result<Response<ReadMultiResponse>, Status> {
        todo!()
    }
}
