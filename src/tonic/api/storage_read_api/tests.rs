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
use storage::content_manager::toc::{COLLECTIONS_DIR, TableOfContent};
use storage::types::{PerformanceConfig, StorageConfig};
use tempfile::TempDir;
use tonic::Code;

use super::*;
use crate::common::helpers::{
    create_general_purpose_runtime, create_search_runtime, create_update_runtime,
};

const OTHER_COLLECTION_NAME: &str = "other-collection";
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

/// Create service on a blocking thread to avoid nested-runtime panics.
/// `TableOfContent::new` internally calls `block_on`, and dropping the
/// `Runtime` instances it owns also requires a non-async context.
async fn create_service_async() -> (StorageReadService<MmapUniversal<u8>>, TempDir, PathBuf) {
    tokio::task::spawn_blocking(create_service).await.unwrap()
}

/// Drop service (and its `TempDir`) on a blocking thread so the `Runtime`
/// instances inside `TableOfContent` are not dropped from async context.
async fn drop_service(service: StorageReadService<MmapUniversal<u8>>, storage_dir: TempDir) {
    tokio::task::spawn_blocking(move || drop((service, storage_dir)))
        .await
        .unwrap();
}

fn create_service() -> (StorageReadService<MmapUniversal<u8>>, TempDir, PathBuf) {
    let storage_dir = tempfile::tempdir().unwrap();
    let config = test_storage_config(storage_dir.path());
    let toc = Arc::new(TableOfContent::new(
        &config,
        create_search_runtime(1).unwrap(),
        create_update_runtime(1).unwrap(),
        create_general_purpose_runtime().unwrap(),
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

fn write_collection_file(collection_dir: &Path, relative_path: &str, contents: &[u8]) -> PathBuf {
    let path = collection_dir.join(relative_path);
    fs_err::create_dir_all(path.parent().unwrap()).unwrap();
    fs_err::write(&path, contents).unwrap();
    path
}

#[tokio::test]
async fn file_exists_rejects_path_traversal() {
    let (service, storage_dir, _collection_dir) = create_service_async().await;

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

    drop_service(service, storage_dir).await;
}

#[cfg(unix)]
#[tokio::test]
async fn file_exists_rejects_symlinked_collection_dir_escape() {
    use std::os::unix::fs::symlink;

    let (service, storage_dir, collection_dir) = create_service_async().await;
    let external_dir = tempfile::tempdir().unwrap();

    fs_err::remove_dir_all(&collection_dir).unwrap();
    fs_err::write(external_dir.path().join("escape.bin"), b"secret").unwrap();
    symlink(external_dir.path(), &collection_dir).unwrap();

    let err = service
        .file_exists(Request::new(FileExistsRequest {
            collection_name: TEST_COLLECTION_NAME.to_string(),
            path: "escape.bin".to_string(),
        }))
        .await
        .unwrap_err();

    assert_eq!(err.code(), Code::PermissionDenied);

    drop_service(service, storage_dir).await;
}

#[cfg(unix)]
#[tokio::test]
async fn file_exists_rejects_symlink_escape_to_other_collection() {
    use std::os::unix::fs::symlink;

    let (service, storage_dir, collection_dir) = create_service_async().await;
    let other_collection_dir = storage_dir
        .path()
        .join(COLLECTIONS_DIR)
        .join(OTHER_COLLECTION_NAME);
    fs_err::create_dir_all(&other_collection_dir).unwrap();
    fs_err::write(other_collection_dir.join("secret.bin"), b"secret").unwrap();
    symlink(&other_collection_dir, collection_dir.join("linked")).unwrap();

    let err = service
        .file_exists(Request::new(FileExistsRequest {
            collection_name: TEST_COLLECTION_NAME.to_string(),
            path: "linked/secret.bin".to_string(),
        }))
        .await
        .unwrap_err();

    assert_eq!(err.code(), Code::PermissionDenied);

    drop_service(service, storage_dir).await;
}

#[tokio::test]
async fn file_exists_reports_true_for_existing_and_false_for_missing_files() {
    let (service, storage_dir, collection_dir) = create_service_async().await;
    write_collection_file(&collection_dir, "exists/present.bin", b"abc");

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

    drop_service(service, storage_dir).await;
}

#[tokio::test]
async fn list_files_returns_paths_relative_to_collection_dir() {
    let (service, storage_dir, collection_dir) = create_service_async().await;
    write_collection_file(&collection_dir, "index/chunk_1.bin", b"123");
    write_collection_file(&collection_dir, "index/chunk_2.bin", b"456");
    write_collection_file(&collection_dir, "index/other.bin", b"789");

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

    drop_service(service, storage_dir).await;
}

#[tokio::test]
async fn file_length_returns_file_size() {
    let (service, storage_dir, collection_dir) = create_service_async().await;
    write_collection_file(&collection_dir, "length/data.bin", b"1234567");

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

    drop_service(service, storage_dir).await;
}

#[tokio::test]
async fn file_length_not_found_returns_error() {
    let (service, storage_dir, _collection_dir) = create_service_async().await;

    let err = service
        .file_length(Request::new(FileLengthRequest {
            collection_name: TEST_COLLECTION_NAME.to_string(),
            path: "nonexistent/file.bin".to_string(),
            open_options: None,
        }))
        .await
        .unwrap_err();

    assert_eq!(err.code(), Code::NotFound);

    drop_service(service, storage_dir).await;
}

#[tokio::test]
async fn read_bytes_returns_requested_range() {
    let (service, storage_dir, collection_dir) = create_service_async().await;
    write_collection_file(&collection_dir, "bytes/data.bin", b"abcdefghij");

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

    drop_service(service, storage_dir).await;
}

#[tokio::test]
async fn read_bytes_out_of_bounds_returns_error() {
    let (service, storage_dir, collection_dir) = create_service_async().await;
    write_collection_file(&collection_dir, "oob/data.bin", b"tiny");

    let err = service
        .read_bytes(Request::new(ReadBytesRequest {
            collection_name: TEST_COLLECTION_NAME.to_string(),
            path: "oob/data.bin".to_string(),
            offset: 0,
            length: 9999,
            open_options: None,
        }))
        .await
        .unwrap_err();

    assert_eq!(err.code(), Code::OutOfRange);

    drop_service(service, storage_dir).await;
}

#[tokio::test]
async fn read_bytes_stream_splits_large_reads_into_chunks() {
    let (service, storage_dir, collection_dir) = create_service_async().await;
    let total_len = STREAM_CHUNK_SIZE as usize + 17;
    let payload = (0..total_len)
        .map(|idx| (idx % 251) as u8)
        .collect::<Vec<_>>();
    write_collection_file(&collection_dir, "stream/data.bin", &payload);

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

    drop_service(service, storage_dir).await;
}

#[tokio::test]
async fn read_bytes_stream_returns_empty_stream_for_zero_length() {
    let (service, storage_dir, collection_dir) = create_service_async().await;
    write_collection_file(&collection_dir, "stream/zero.bin", b"some data");

    let mut stream = service
        .read_bytes_stream(Request::new(ReadBytesStreamRequest {
            collection_name: TEST_COLLECTION_NAME.to_string(),
            path: "stream/zero.bin".to_string(),
            offset: 0,
            length: 0,
            open_options: None,
        }))
        .await
        .unwrap()
        .into_inner();

    assert!(stream.next().await.is_none());

    drop_service(service, storage_dir).await;
}

#[tokio::test]
async fn read_bytes_stream_zero_length_checks_file_exists() {
    let (service, storage_dir, _collection_dir) = create_service_async().await;

    let Err(err) = service
        .read_bytes_stream(Request::new(ReadBytesStreamRequest {
            collection_name: TEST_COLLECTION_NAME.to_string(),
            path: "stream/missing.bin".to_string(),
            offset: 0,
            length: 0,
            open_options: None,
        }))
        .await
    else {
        panic!("expected missing file to be reported");
    };

    assert_eq!(err.code(), Code::NotFound);

    drop_service(service, storage_dir).await;
}

#[tokio::test]
async fn read_bytes_stream_out_of_bounds_returns_error() {
    let (service, storage_dir, collection_dir) = create_service_async().await;
    let payload = b"short";
    write_collection_file(&collection_dir, "stream/clamp.bin", payload);

    let Err(err) = service
        .read_bytes_stream(Request::new(ReadBytesStreamRequest {
            collection_name: TEST_COLLECTION_NAME.to_string(),
            path: "stream/clamp.bin".to_string(),
            offset: 0,
            length: 999999,
            open_options: None,
        }))
        .await
    else {
        panic!("expected out-of-bounds stream request to fail");
    };

    assert_eq!(err.code(), Code::OutOfRange);

    drop_service(service, storage_dir).await;
}

#[tokio::test]
async fn read_whole_returns_entire_file() {
    let (service, storage_dir, collection_dir) = create_service_async().await;
    let payload = b"whole file contents";
    write_collection_file(&collection_dir, "whole/data.bin", payload);

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

    drop_service(service, storage_dir).await;
}

#[tokio::test]
async fn read_batch_returns_each_requested_slice() {
    let (service, storage_dir, collection_dir) = create_service_async().await;
    write_collection_file(&collection_dir, "batch/data.bin", b"0123456789");

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

    drop_service(service, storage_dir).await;
}

#[tokio::test]
async fn read_multi_reads_ranges_in_request_order() {
    let (service, storage_dir, collection_dir) = create_service_async().await;
    write_collection_file(&collection_dir, "segments/a.bin", b"abcdefghij");
    write_collection_file(&collection_dir, "segments/b.bin", b"klmnopqrst");

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

    drop_service(service, storage_dir).await;
}

#[tokio::test]
async fn read_multi_rejects_empty_entry_path() {
    let (service, storage_dir, _collection_dir) = create_service_async().await;

    let err = service
        .read_multi(Request::new(ReadMultiRequest {
            collection_name: TEST_COLLECTION_NAME.to_string(),
            reads: vec![ReadMultiEntry {
                path: "".to_string(),
                offset: 0,
                length: 1,
            }],
            open_options: None,
        }))
        .await
        .unwrap_err();

    assert_eq!(err.code(), Code::InvalidArgument);

    drop_service(service, storage_dir).await;
}
