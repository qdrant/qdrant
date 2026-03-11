use std::collections::HashMap;
use std::marker::PhantomData;
use std::path::{Component, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

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

    /// Return the base directory for a collection.
    fn collection_base_path(&self, auth: &storage::rbac::Auth, collection_name: &str) -> PathBuf {
        let pass = new_unchecked_verification_pass();
        let toc = self.dispatcher.toc(auth, &pass);
        toc.storage_path()
            .join(COLLECTIONS_DIR)
            .join(collection_name)
    }

    /// Resolve a collection-scoped relative path to an absolute path,
    /// rejecting any traversal attempts.
    fn resolve_path(
        &self,
        auth: &storage::rbac::Auth,
        collection_name: &str,
        relative_path: &str,
    ) -> Result<PathBuf, Status> {
        let base = self.collection_base_path(auth, collection_name);

        let rel = std::path::Path::new(relative_path);
        for c in rel.components() {
            match c {
                Component::Normal(_) => {}
                _ => {
                    return Err(Status::invalid_argument(format!(
                        "Invalid path component in '{relative_path}'"
                    )));
                }
            }
        }

        let full = base.join(rel);

        // Double-check: canonicalize if the path exists and verify it's under base.
        // If the file doesn't exist yet (e.g. for exists check), the component check above
        // is sufficient since we rejected all non-Normal components.
        if full.exists() {
            let canonical = fs_err::canonicalize(&full)
                .map_err(|e| Status::internal(format!("Failed to canonicalize path: {e}")))?;
            let canonical_base = fs_err::canonicalize(&base).unwrap_or(base.clone());
            if !canonical.starts_with(&canonical_base) {
                return Err(Status::permission_denied(format!(
                    "Path '{}' is outside the collection directory",
                    full.display()
                )));
            }
        }

        Ok(full)
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
            "Range {start}..{end} out of bounds (size: {data_length})"
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
        disk_parallel: opts.disk_parallel.and_then(|v| usize::try_from(v).ok()),
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
        } = request.into_inner();
        let path = self.resolve_path(&auth, &collection_name, &path)?;

        let exists = tokio::task::spawn_blocking(move || match S::exists(&path) {
            Ok(exists) => Ok(exists),
            Err(UniversalIoError::NotFound { .. }) => Ok(false),
            Err(e) => Err(io_error_to_status(e)),
        })
        .await
        .map_err(|e| Status::internal(format!("Task join error: {e}")))??;

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
        let base = self.collection_base_path(&auth, &collection_name);
        let prefix_path = self.resolve_path(&auth, &collection_name, &prefix_path)?;

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
                ElementsRange::new(offset, length),
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
            path,
            offset,
            length,
            open_options,
        } = request.into_inner();

        let path = self.resolve_path(&auth, &collection_name, &path)?;

        if length == 0 {
            return Ok(Response::new(Box::pin(futures::stream::empty())));
        }

        let open_options = convert_open_options(open_options);
        let sequential = open_options.need_sequential;
        let storage = tokio::task::spawn_blocking(move || {
            S::open(&path, open_options).map_err(io_error_to_status)
        })
        .await
        .map_err(|e| Status::internal(format!("Task join error: {e}")))??;

        let storage = Arc::new(storage);

        let stream = futures::stream::try_unfold(
            (storage, offset, length),
            move |(storage, current_offset, remaining)| async move {
                if remaining == 0 {
                    return Ok(None);
                }

                let chunk_size = remaining.min(STREAM_CHUNK_SIZE);
                let storage_for_read = Arc::clone(&storage);

                let data = tokio::task::spawn_blocking(move || {
                    dispatch_read(
                        &*storage_for_read,
                        ElementsRange::new(current_offset, chunk_size),
                        sequential,
                    )
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
            .map(|r| ElementsRange::new(r.offset, r.length))
            .collect::<Vec<_>>();

        let data = tokio::task::spawn_blocking(move || {
            let storage = S::open(&path, open_options).map_err(io_error_to_status)?;
            let mut results = ranges
                .iter()
                .map(|r| Vec::with_capacity(r.length as usize))
                .collect::<Vec<_>>();
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
        let ReadMultiRequest {
            collection_name,
            reads,
            open_options,
        } = request.into_inner();
        let open_options = convert_open_options(open_options);

        // Resolve all paths and deduplicate into a file index.
        let mut path_to_index = HashMap::<PathBuf, FileIndex>::new();
        let mut unique_paths = Vec::<PathBuf>::new();
        let mut reads_ = Vec::<(FileIndex, _)>::with_capacity(reads.len());

        for entry in &reads {
            let resolved = self.resolve_path(&auth, &collection_name, &entry.path)?;
            let file_index = *path_to_index.entry(resolved.clone()).or_insert_with(|| {
                let idx = unique_paths.len();
                unique_paths.push(resolved);
                idx
            });
            reads_.push((file_index, ElementsRange::new(entry.offset, entry.length)));
        }

        let data = tokio::task::spawn_blocking(move || {
            let files = unique_paths
                .iter()
                .map(|p| S::open(p, open_options))
                .collect::<common::universal_io::Result<Vec<_>>>()
                .map_err(io_error_to_status)?;

            let mut results = reads_
                .iter()
                .map(|(_, r)| Vec::with_capacity(r.length as usize))
                .collect::<Vec<_>>();
            dispatch_read_multi(
                &files,
                reads_,
                open_options.need_sequential,
                |op_idx, _, chunk| {
                    results[op_idx].extend_from_slice(chunk);
                    Ok(())
                },
            )
            .map_err(io_error_to_status)?;
            Ok::<Vec<Vec<u8>>, Status>(results)
        })
        .await
        .map_err(|e| Status::internal(format!("Task join error: {e}")))??;

        Ok(Response::new(ReadMultiResponse { data }))
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use api::grpc::qdrant::{ReadBatchRange, ReadMultiEntry};
    use collection::common::snapshots_manager::SnapshotsConfig;
    use collection::config::WalConfig;
    use collection::optimizers_builder::OptimizersConfig;
    use collection::shards::channel_service::ChannelService;
    use common::budget::ResourceBudget;
    use common::load_concurrency::LoadConcurrencyConfig;
    use common::mmap;
    use futures::StreamExt as _;
    use segment::types::{HnswConfig, HnswGlobalConfig};
    use storage::content_manager::toc::TableOfContent;
    use storage::types::{PerformanceConfig, StorageConfig};
    use tempfile::TempDir;
    use tokio::runtime::Runtime;
    use tonic::Code;

    use super::*;

    const TEST_COLLECTION_NAME: &str = "test-collection";

    fn test_storage_config(storage_path: &Path) -> StorageConfig {
        StorageConfig {
            storage_path: storage_path.to_path_buf(),
            snapshots_path: storage_path.join("snapshots"),
            snapshots_config: SnapshotsConfig::default(),
            temp_path: None,
            on_disk_payload: false,
            optimizers: OptimizersConfig {
                deleted_threshold: 0.5,
                vacuum_min_vector_number: 100,
                default_segment_number: 2,
                max_segment_size: None,
                #[expect(deprecated)]
                memmap_threshold: Some(100),
                indexing_threshold: Some(100),
                flush_interval_sec: 2,
                max_optimization_threads: Some(2),
                prevent_unoptimized: None,
            },
            optimizers_overwrite: None,
            wal: WalConfig::default(),
            performance: PerformanceConfig {
                max_search_threads: 1,
                max_optimization_runtime_threads: 1,
                update_rate_limit: None,
                search_timeout_sec: None,
                optimizer_cpu_budget: 0,
                optimizer_io_budget: 0,
                incoming_shard_transfers_limit: Some(1),
                outgoing_shard_transfers_limit: Some(1),
                async_scorer: None,
                load_concurrency: LoadConcurrencyConfig::default(),
            },
            hnsw_index: HnswConfig::default(),
            hnsw_global_config: HnswGlobalConfig::default(),
            mmap_advice: mmap::Advice::Random,
            node_type: Default::default(),
            update_queue_size: Default::default(),
            handle_collection_load_errors: false,
            recovery_mode: None,
            update_concurrency: Some(NonZeroUsize::new(2).unwrap()),
            shard_transfer_method: None,
            collection: None,
            max_collections: None,
        }
    }

    fn create_service() -> (StorageReadService<MmapUniversal<u8>>, TempDir, PathBuf) {
        let storage_dir = tempfile::tempdir().unwrap();
        let config = test_storage_config(storage_dir.path());
        let toc = Arc::new(TableOfContent::new(
            &config,
            Runtime::new().unwrap(),
            Runtime::new().unwrap(),
            Runtime::new().unwrap(),
            ResourceBudget::default(),
            ChannelService::new(6333, false, None, None),
            0,
            None,
        ));
        let collection_dir = storage_dir
            .path()
            .join(COLLECTIONS_DIR)
            .join(TEST_COLLECTION_NAME);
        fs_err::create_dir_all(&collection_dir).unwrap();

        let dispatcher = Arc::new(Dispatcher::new(toc));
        let service = StorageReadService::new(dispatcher);

        (service, storage_dir, collection_dir)
    }

    fn write_collection_file(
        collection_dir: &Path,
        relative_path: &str,
        contents: &[u8],
    ) -> PathBuf {
        let path = collection_dir.join(relative_path);
        fs_err::create_dir_all(path.parent().unwrap()).unwrap();
        fs_err::write(&path, contents).unwrap();
        path
    }

    #[test]
    fn file_exists_rejects_path_traversal() {
        let (service, _storage_dir, _collection_dir) = create_service();

        Runtime::new().unwrap().block_on(async {
            let err = service
                .file_exists(Request::new(FileExistsRequest {
                    collection_name: TEST_COLLECTION_NAME.to_string(),
                    path: "nested/../secret.bin".to_string(),
                }))
                .await
                .unwrap_err();

            assert_eq!(err.code(), Code::InvalidArgument);
            assert!(
                err.message().contains("Invalid path component"),
                "unexpected error message: {}",
                err.message()
            );
        });
    }

    #[test]
    fn file_exists_reports_true_for_existing_and_false_for_missing_files() {
        let (service, _storage_dir, collection_dir) = create_service();
        write_collection_file(&collection_dir, "exists/present.bin", b"abc");

        Runtime::new().unwrap().block_on(async {
            let existing = service
                .file_exists(Request::new(FileExistsRequest {
                    collection_name: TEST_COLLECTION_NAME.to_string(),
                    path: "exists/present.bin".to_string(),
                }))
                .await
                .unwrap()
                .into_inner();

            let missing = service
                .file_exists(Request::new(FileExistsRequest {
                    collection_name: TEST_COLLECTION_NAME.to_string(),
                    path: "exists/missing.bin".to_string(),
                }))
                .await
                .unwrap()
                .into_inner();

            assert!(existing.exists);
            assert!(!missing.exists);
        });
    }

    #[test]
    fn list_files_returns_paths_relative_to_collection_dir() {
        let (service, _storage_dir, collection_dir) = create_service();
        write_collection_file(&collection_dir, "index/chunk_1.bin", b"123");
        write_collection_file(&collection_dir, "index/chunk_2.bin", b"456");
        write_collection_file(&collection_dir, "index/other.bin", b"789");

        Runtime::new().unwrap().block_on(async {
            let mut paths = service
                .list_files(Request::new(ListFilesRequest {
                    collection_name: TEST_COLLECTION_NAME.to_string(),
                    prefix_path: "index/chunk_".to_string(),
                }))
                .await
                .unwrap()
                .into_inner()
                .paths;

            paths.sort();

            assert_eq!(
                paths,
                vec![
                    "index/chunk_1.bin".to_string(),
                    "index/chunk_2.bin".to_string(),
                ]
            );
        });
    }

    #[test]
    fn file_length_returns_file_size() {
        let (service, _storage_dir, collection_dir) = create_service();
        write_collection_file(&collection_dir, "length/data.bin", b"1234567");

        Runtime::new().unwrap().block_on(async {
            let response = service
                .file_length(Request::new(FileLengthRequest {
                    collection_name: TEST_COLLECTION_NAME.to_string(),
                    path: "length/data.bin".to_string(),
                    open_options: None,
                }))
                .await
                .unwrap()
                .into_inner();

            assert_eq!(response.length, 7);
        });
    }

    #[test]
    fn read_bytes_returns_requested_range() {
        let (service, _storage_dir, collection_dir) = create_service();
        write_collection_file(&collection_dir, "bytes/data.bin", b"abcdefghij");

        Runtime::new().unwrap().block_on(async {
            let response = service
                .read_bytes(Request::new(ReadBytesRequest {
                    collection_name: TEST_COLLECTION_NAME.to_string(),
                    path: "bytes/data.bin".to_string(),
                    offset: 3,
                    length: 4,
                    open_options: None,
                }))
                .await
                .unwrap()
                .into_inner();

            assert_eq!(response.data, b"defg".to_vec());
        });
    }

    #[test]
    fn read_multi_reads_ranges_in_request_order() {
        let (service, _storage_dir, collection_dir) = create_service();
        write_collection_file(&collection_dir, "segments/a.bin", b"abcdefghij");
        write_collection_file(&collection_dir, "segments/b.bin", b"klmnopqrst");

        Runtime::new().unwrap().block_on(async {
            let response = service
                .read_multi(Request::new(ReadMultiRequest {
                    collection_name: TEST_COLLECTION_NAME.to_string(),
                    reads: vec![
                        ReadMultiEntry {
                            path: "segments/a.bin".to_string(),
                            offset: 1,
                            length: 3,
                        },
                        ReadMultiEntry {
                            path: "segments/b.bin".to_string(),
                            offset: 2,
                            length: 4,
                        },
                        ReadMultiEntry {
                            path: "segments/a.bin".to_string(),
                            offset: 6,
                            length: 2,
                        },
                    ],
                    open_options: None,
                }))
                .await
                .unwrap()
                .into_inner();

            assert_eq!(
                response.data,
                vec![b"bcd".to_vec(), b"mnop".to_vec(), b"gh".to_vec()]
            );
        });
    }

    #[test]
    fn read_whole_returns_entire_file() {
        let (service, _storage_dir, collection_dir) = create_service();
        let payload = b"whole file contents";
        write_collection_file(&collection_dir, "whole/data.bin", payload);

        Runtime::new().unwrap().block_on(async {
            let response = service
                .read_whole(Request::new(ReadWholeRequest {
                    collection_name: TEST_COLLECTION_NAME.to_string(),
                    path: "whole/data.bin".to_string(),
                    open_options: None,
                }))
                .await
                .unwrap()
                .into_inner();

            assert_eq!(response.data, payload.to_vec());
        });
    }

    #[test]
    fn read_bytes_stream_splits_large_reads_into_chunks() {
        let (service, _storage_dir, collection_dir) = create_service();
        let total_len = STREAM_CHUNK_SIZE as usize + 17;
        let payload = (0..total_len)
            .map(|idx| (idx % 251) as u8)
            .collect::<Vec<_>>();
        write_collection_file(&collection_dir, "stream/data.bin", &payload);

        Runtime::new().unwrap().block_on(async {
            let mut stream = service
                .read_bytes_stream(Request::new(ReadBytesStreamRequest {
                    collection_name: TEST_COLLECTION_NAME.to_string(),
                    path: "stream/data.bin".to_string(),
                    offset: 0,
                    length: total_len as u64,
                    open_options: None,
                }))
                .await
                .unwrap()
                .into_inner();

            let mut chunks = Vec::new();
            let mut reconstructed = Vec::new();
            while let Some(item) = stream.next().await {
                let chunk = item.unwrap().data;
                chunks.push(chunk.len());
                reconstructed.extend_from_slice(&chunk);
            }

            assert_eq!(
                chunks,
                vec![
                    STREAM_CHUNK_SIZE as usize,
                    total_len - STREAM_CHUNK_SIZE as usize
                ]
            );
            assert_eq!(reconstructed, payload);
        });
    }

    #[test]
    fn read_batch_returns_each_requested_slice() {
        let (service, _storage_dir, collection_dir) = create_service();
        write_collection_file(&collection_dir, "batch/data.bin", b"0123456789");

        Runtime::new().unwrap().block_on(async {
            let response = service
                .read_batch(Request::new(ReadBatchRequest {
                    collection_name: TEST_COLLECTION_NAME.to_string(),
                    path: "batch/data.bin".to_string(),
                    ranges: vec![
                        ReadBatchRange {
                            offset: 0,
                            length: 2,
                        },
                        ReadBatchRange {
                            offset: 4,
                            length: 3,
                        },
                        ReadBatchRange {
                            offset: 9,
                            length: 1,
                        },
                    ],
                    open_options: None,
                }))
                .await
                .unwrap()
                .into_inner();

            assert_eq!(
                response.data,
                vec![b"01".to_vec(), b"456".to_vec(), b"9".to_vec()]
            );
        });
    }
}
