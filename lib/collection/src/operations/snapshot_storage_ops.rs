use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

use common::budget::ResourceBudget;
use fs_err as fs;
use fs_err::{File, tokio as tokio_fs};
use futures::StreamExt;
use object_store::{ObjectStoreExt, WriteMultipart};
use segment::common::BYTES_IN_MB;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::snapshot_ops::SnapshotDescription;
use super::types::{CollectionError, CollectionResult};

pub(crate) fn trim_dot_slash(path: &Path) -> CollectionResult<object_store::path::Path> {
    // Get file name by trimming the path.
    // if the path is ./path/to/file.txt, the key should be path/to/file.txt
    let key = path.to_str().ok_or_else(|| {
        CollectionError::object_storage_error(format!(
            "Failed to get key for snapshot: {}",
            path.display()
        ))
    })?;
    Ok(object_store::path::Path::from(key.trim_start_matches("./")))
}

fn get_filename(path: &str) -> CollectionResult<String> {
    let path = PathBuf::from(path);
    path.file_name()
        .ok_or_else(|| CollectionError::object_storage_error("Failed to get file name".to_string()))
        .and_then(|name| {
            name.to_str()
                .ok_or_else(|| {
                    CollectionError::object_storage_error("Failed to get file name".to_string())
                })
                .map(|name| name.to_string())
        })
}

pub async fn get_snapshot_description(
    client: &dyn object_store::ObjectStore,
    path: &Path,
) -> CollectionResult<SnapshotDescription> {
    let file_meta: object_store::ObjectMeta = client
        .head(&trim_dot_slash(path)?)
        .await
        .map_err(|e| CollectionError::service_error(format!("Failed to get head: {e}")))?;

    let name = get_filename(path.to_str().ok_or_else(|| {
        CollectionError::object_storage_error(format!(
            "Failed to get key for snapshot: {}",
            path.display()
        ))
    })?)?;
    let size = file_meta.size;
    let last_modified = file_meta.last_modified.naive_local();
    let checksum = None;

    Ok(SnapshotDescription {
        name,
        creation_time: Some(last_modified),
        size,
        checksum,
    })
}

/// 5TB as maximum object size.
/// Source: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html>
const MAX_UPLOAD_SIZE: usize = 5 * 1024 * 1024 * 1024 * 1024;

const MAX_PART_NUMBER: usize = 10000;

const MAX_BUFFERED_PARTS_SIZE: usize = 1024 * 1024 * 1024;

/// Part size for uploading the file at `local_source_path`, see [`chunk_size_for`].
pub async fn get_appropriate_chunk_size(local_source_path: &Path) -> CollectionResult<usize> {
    let file_meta = tokio_fs::metadata(local_source_path).await?;
    let file_size = file_meta.len() as usize;

    // check if the file size exceeds the maximum upload size
    if file_size > MAX_UPLOAD_SIZE {
        return Err(CollectionError::service_error(format!(
            "File size exceeds the maximum upload size: {MAX_UPLOAD_SIZE}"
        )));
    }

    Ok(chunk_size_for(file_size))
}

/// This function adjusts the chunk size based on service limits and the total size of the data to be uploaded.
/// Note:
///
/// * Amazon S3: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html>
///   partsize: min 5 MB, max 5 GB, up to 10,000 parts.
/// * Google Cloud Storage: <https://cloud.google.com/storage/quotas?hl=ja#objects>
///   partsize: min 5 MB, max 5 GB, up to 10,000 parts.
/// * Azure Storage: <https://learn.microsoft.com/en-us/rest/api/storageservices/put-blob?tabs=microsoft-entra-id#remarks>
///   TODO: It looks like Azure Storage has different limits for different service versions.
pub(crate) fn chunk_size_for(size: usize) -> usize {
    const DEFAULT_CHUNK_SIZE: usize = 50 * 1024 * 1024;
    // Estimates can overshoot; parts stay within the 5 GB part limit
    let size = size.min(MAX_UPLOAD_SIZE);

    // check if the size exceeds the maximum part number
    // if so, adjust the chunk size to fit the maximum part number
    if size > DEFAULT_CHUNK_SIZE * MAX_PART_NUMBER {
        return ((size - 1) / MAX_PART_NUMBER) + 1; // ceil(size / MAX_PART_NUMBER)
    }
    DEFAULT_CHUNK_SIZE
}

/// Uploads everything read from `reader` as a single object at `target_path`.
///
/// The object becomes visible only if `commit` fires and `reader` reaches EOF. If
/// `commit` is dropped, or reading or uploading fails, the multipart upload is
/// aborted, so a truncated stream never becomes a snapshot. Dropping `commit` is an
/// abort on request and returns `Ok`; any `Err` is a real read or upload failure.
pub(crate) async fn multipart_upload_stream(
    client: &dyn object_store::ObjectStore,
    target_path: &Path,
    mut reader: impl tokio::io::AsyncRead + Unpin,
    chunk_size: usize,
    commit: tokio::sync::oneshot::Receiver<()>,
) -> CollectionResult<()> {
    const READ_BUFFER_SIZE: usize = 1024 * 1024;

    let s3_path = trim_dot_slash(target_path)?;
    let upload = client
        .put_multipart(&s3_path)
        .await
        .map_err(|e| CollectionError::service_error(format!("Failed to put multipart: {e}")))?;
    let mut write = WriteMultipart::new_with_chunk_size(upload, chunk_size);

    // Cap max concurrency to avoid saturating the network on high core count
    let max_concurrency = std::cmp::min(ResourceBudget::default().available_cpu_budget(), 8);
    // Large parts are buffered in memory, so fewer of them are in flight
    let max_concurrency = max_concurrency.min((MAX_BUFFERED_PARTS_SIZE / chunk_size).max(1));
    let max_bytes = chunk_size
        .saturating_mul(MAX_PART_NUMBER)
        .min(MAX_UPLOAD_SIZE);

    // `Ok(false)` means the producer abandoned the stream.
    let streamed = async {
        let mut commit = commit;
        let mut committed = false;
        let mut total_bytes = 0;
        let mut buffer = vec![0u8; READ_BUFFER_SIZE];
        loop {
            let bytes_read = tokio::select! {
                biased;
                decision = &mut commit, if !committed => match decision {
                    Ok(()) => {
                        committed = true;
                        continue;
                    }
                    Err(_) => return Ok(false),
                },
                read = reader.read(&mut buffer) => match read {
                    Ok(bytes_read) => bytes_read,
                    Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(err) => {
                        return Err(CollectionError::service_error(format!(
                            "Failed to read snapshot stream: {err}"
                        )));
                    }
                },
            };
            if bytes_read == 0 {
                break;
            }
            total_bytes += bytes_read;
            if total_bytes > max_bytes {
                return Err(CollectionError::service_error(format!(
                    "Snapshot exceeds {max_bytes} bytes, the upload limit for {chunk_size} byte parts"
                )));
            }
            // Stalled parts can hold this wait for minutes, so an abandoned stream must end it too.
            loop {
                tokio::select! {
                    biased;
                    decision = &mut commit, if !committed => match decision {
                        Ok(()) => committed = true,
                        Err(_) => return Ok(false),
                    },
                    capacity = write.wait_for_capacity(max_concurrency) => {
                        capacity.map_err(|e| {
                            CollectionError::service_error(format!(
                                "Failed to wait for capacity: {e}"
                            ))
                        })?;
                        break;
                    }
                }
            }
            write.write(&buffer[..bytes_read]);
        }
        Ok(committed || commit.await.is_ok())
    }
    .await;
    // Unblocks a producer stuck on a full pipe before the slower cleanup below.
    drop(reader);

    if !matches!(streamed, Ok(true)) {
        if let Err(abort_err) = write.abort().await {
            log::warn!("Failed to abort multipart upload: {abort_err}");
        }
        return streamed.map(|_| ());
    }

    write
        .finish()
        .await
        .map_err(|e| CollectionError::service_error(format!("Failed to finish upload: {e}")))?;
    Ok(())
}

pub async fn multipart_upload(
    client: &dyn object_store::ObjectStore,
    source_path: &Path,
    target_path: &Path,
) -> CollectionResult<()> {
    let s3_path = trim_dot_slash(target_path)?;
    let upload = client
        .put_multipart(&s3_path)
        .await
        .map_err(|e| CollectionError::service_error(format!("Failed to put multipart: {e}")))?;

    let chunk_size: usize = get_appropriate_chunk_size(source_path).await?;
    let mut write = WriteMultipart::new_with_chunk_size(upload, chunk_size);
    let file = File::open(source_path)?;
    let mut reader = BufReader::new(file);
    let mut buffer = vec![0u8; chunk_size];

    // Initialize CpuBudget to manage concurrency
    let cpu_budget = ResourceBudget::default();
    // Cap max concurrency to avoid saturating the network on high core count
    let max_concurrency = std::cmp::min(cpu_budget.available_cpu_budget(), 8);

    // Note:
    //  1. write.write() is sync but a worker thread is spawned internally.
    //  2. write.finish() will wait for all the worker threads to finish.
    loop {
        let bytes_read = match reader.read(&mut buffer) {
            Ok(bytes_read) => bytes_read,
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(err) => {
                if let Err(abort_err) = write.abort().await {
                    log::warn!("Failed to abort multipart upload: {abort_err}");
                }
                return Err(CollectionError::service_error(format!(
                    "Failed to read snapshot file: {err}"
                )));
            }
        };
        if bytes_read == 0 {
            break;
        }
        let buffer = &buffer[..bytes_read];

        // Wait for capacity before writing the buffer
        write
            .wait_for_capacity(max_concurrency)
            .await
            .map_err(|e| {
                CollectionError::service_error(format!("Failed to wait for capacity: {e}"))
            })?;

        write.write(buffer);
    }
    write
        .finish() //  2. write.finish() will wait for all the worker threads to finish.
        .await
        .map_err(|e| CollectionError::service_error(format!("Failed to finish upload: {e}")))?;

    Ok(())
}

pub async fn list_snapshot_descriptions(
    client: &dyn object_store::ObjectStore,
    directory: &Path,
) -> CollectionResult<Vec<SnapshotDescription>> {
    let prefix = trim_dot_slash(directory)?;
    let mut list_stream = client.list(Some(&prefix));

    let mut snapshots = Vec::new();
    while let Some(meta) = list_stream
        .next()
        .await
        .transpose()
        .map_err(|e| CollectionError::service_error(format!("Failed to list snapshots: {e}")))?
    {
        snapshots.push(SnapshotDescription {
            name: get_filename(meta.location.as_ref())?,
            creation_time: Some(meta.last_modified.naive_local()),
            size: meta.size,
            checksum: None,
        });
    }
    Ok(snapshots)
}

pub async fn delete_snapshot(
    client: &dyn object_store::ObjectStore,
    path: &Path,
) -> CollectionResult<bool> {
    let s3_path = trim_dot_slash(path)?;

    #[expect(clippy::wildcard_enum_match_arm, reason = "error handling")]
    client.head(&s3_path).await.map_err(|e| match e {
        object_store::Error::NotFound { .. } => {
            CollectionError::not_found(format!("Snapshot {s3_path:?}"))
        }
        _ => CollectionError::service_error(format!("Failed to delete snapshot: {e}")),
    })?;

    client
        .delete(&s3_path)
        .await
        .map_err(|e| CollectionError::service_error(format!("Failed to delete snapshot: {e}")))?;
    Ok(true)
}

pub async fn download_snapshot(
    client: &dyn object_store::ObjectStore,
    path: &Path,
    target_path: &Path,
) -> CollectionResult<()> {
    let download_start_time = tokio::time::Instant::now();
    let s3_path = trim_dot_slash(path)?;
    #[expect(clippy::wildcard_enum_match_arm, reason = "error handling")]
    let download = client.get(&s3_path).await.map_err(|e| match e {
        object_store::Error::NotFound { .. } => {
            CollectionError::not_found(format!("Snapshot {s3_path:?}"))
        }
        _ => CollectionError::service_error(format!("Failed to get {s3_path}: {e}")),
    })?;

    let mut stream = download.into_stream();

    // Create the target directory if it does not exist
    if let Some(target_dir) = target_path.parent()
        && !target_dir.exists()
    {
        fs::create_dir_all(target_dir)?;
    }

    let mut file = tokio_fs::File::create(target_path)
        .await
        .map_err(|e| CollectionError::service_error(format!("Failed to create file: {e}")))?;

    let mut total_size = 0;
    while let Some(data) = stream.next().await {
        let data = data.map_err(|e| {
            CollectionError::service_error(format!("Failed to get data from stream: {e}"))
        })?;
        file.write_all(&data)
            .await
            .map_err(|e| CollectionError::service_error(format!("Failed to write to file: {e}")))?;
        total_size += data.len();
    }
    // ensure flush
    file.flush()
        .await
        .map_err(|e| CollectionError::service_error(format!("Failed to flush file: {e}")))?;

    let download_duration = download_start_time.elapsed();
    let total_size_mb = total_size as f64 / BYTES_IN_MB as f64;
    let download_speed_mbps = total_size_mb / download_duration.as_secs_f64();
    log::debug!(
        "Object storage snapshot download completed: path={}, size={:.2} MB, duration={:.2}s, speed={:.2} MB/s",
        target_path.display(),
        total_size_mb,
        download_duration.as_secs_f64(),
        download_speed_mbps
    );

    // check len to file len
    let file_meta = tokio_fs::metadata(target_path).await?;
    if file_meta.len() != total_size as u64 {
        return Err(CollectionError::service_error(format!(
            "Downloaded file size does not match the expected size: {} != {}",
            file_meta.len(),
            total_size
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use object_store::memory::InMemory;
    use object_store::throttle::{ThrottleConfig, ThrottledStore};

    use super::*;

    /// Reading a directory fails with `EISDIR`, which triggers a read error mid-upload.
    // Formatting an `fs-err` error blocks (its `debug` feature), which needs a multi-thread runtime.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn multipart_upload_fails_on_read_error() {
        let store = InMemory::new();
        let directory_source = tempfile::tempdir().unwrap();
        let target = Path::new("./snapshots/collection/test.snapshot");

        let err = multipart_upload(&store, directory_source.path(), target)
            .await
            .unwrap_err();

        assert!(
            matches!(&err, CollectionError::ServiceError { error, .. }
                if error.contains("Failed to read snapshot file")),
            "unexpected error: {err}",
        );
        let key = trim_dot_slash(target).unwrap();
        assert!(
            matches!(
                store.head(&key).await,
                Err(object_store::Error::NotFound { .. })
            ),
            "a failed upload must not store an object",
        );
    }

    const STREAM_CHUNK: usize = 5 * 1024 * 1024;
    async fn object_bytes(store: &InMemory, target: &Path) -> Option<Vec<u8>> {
        let key = trim_dot_slash(target).unwrap();
        match store.get(&key).await {
            Ok(result) => Some(result.bytes().await.unwrap().to_vec()),
            Err(object_store::Error::NotFound { .. }) => None,
            Err(err) => panic!("unexpected error: {err}"),
        }
    }

    /// Overestimated sizes must not produce parts above the 5 GB S3 part limit.
    #[test]
    fn chunk_size_for_respects_part_size_limit() {
        assert!(chunk_size_for(usize::MAX) <= 5 * 1024 * 1024 * 1024);
    }

    /// Committing before the producer finishes writing still stores the whole stream, in order.
    #[tokio::test]
    async fn multipart_upload_stream_reads_to_end_after_early_commit() {
        let store = InMemory::new();
        let target = Path::new("./snapshots/full.snapshot");
        let (reader, mut writer) = tokio::io::duplex(1024);
        let (commit, commit_rx) = tokio::sync::oneshot::channel();
        let data: Vec<u8> = (0..2 * STREAM_CHUNK + 123)
            .map(|i| (i % 251) as u8)
            .collect();
        let expected = data.clone();
        commit.send(()).unwrap();

        let producer = tokio::spawn(async move {
            writer.write_all(&data).await.unwrap();
        });
        multipart_upload_stream(&store, target, reader, STREAM_CHUNK, commit_rx)
            .await
            .unwrap();
        producer.await.unwrap();

        assert_eq!(object_bytes(&store, target).await, Some(expected));
    }

    /// Abandoning a stream whose parts never finish still stops the upload and the producer.
    #[tokio::test]
    async fn multipart_upload_stream_abort_releases_stalled_upload() {
        let config = ThrottleConfig {
            wait_put_per_call: Duration::from_secs(3600),
            ..Default::default()
        };
        let store = Arc::new(ThrottledStore::new(InMemory::new(), config));
        let target = Path::new("./snapshots/full.snapshot");
        let (reader, mut writer) = tokio::io::duplex(1024);
        let (commit, commit_rx) = tokio::sync::oneshot::channel::<()>();

        let upload = tokio::spawn({
            let store = store.clone();
            async move { multipart_upload_stream(store.as_ref(), target, reader, 1024, commit_rx).await }
        });
        // Parts never complete, so the uploader waits for capacity and the pipe fills up.
        let producer =
            tokio::spawn(async move { while writer.write_all(&[5u8; 1024]).await.is_ok() {} });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            !producer.is_finished(),
            "producer should be blocked on the full pipe"
        );
        drop(commit);

        let result = tokio::time::timeout(Duration::from_secs(5), upload)
            .await
            .expect("upload must stop while parts are stalled");
        result.unwrap().unwrap();
        tokio::time::timeout(Duration::from_secs(5), producer)
            .await
            .expect("producer must be released")
            .unwrap();
        let key = trim_dot_slash(target).unwrap();
        assert!(matches!(
            store.head(&key).await,
            Err(object_store::Error::NotFound { .. })
        ));
    }
}
