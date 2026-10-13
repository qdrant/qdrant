use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

use common::budget::ResourceBudget;
use fs_err as fs;
use fs_err::{File, tokio as tokio_fs};
use futures::StreamExt;
use object_store::{ObjectStoreExt, WriteMultipart};
use segment::common::BYTES_IN_MB;
use tokio::io::AsyncWriteExt;

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

/// This function adjusts the chunk size based on service limits and the total size of the data to be uploaded.
/// Note:
///
/// * Amazon S3: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html>
///   partsize: min 5 MB, max 5 GB, up to 10,000 parts.
/// * Google Cloud Storage: <https://cloud.google.com/storage/quotas?hl=ja#objects>
///   partsize: min 5 MB, max 5 GB, up to 10,000 parts.
/// * Azure Storage: <https://learn.microsoft.com/en-us/rest/api/storageservices/put-blob?tabs=microsoft-entra-id#remarks>
///   TODO: It looks like Azure Storage has different limits for different service versions.
pub async fn get_appropriate_chunk_size(local_source_path: &Path) -> CollectionResult<usize> {
    const DEFAULT_CHUNK_SIZE: usize = 50 * 1024 * 1024;
    const MAX_PART_NUMBER: usize = 10000;
    /// 5TB as maximum object size.
    /// Source: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html>
    const MAX_UPLOAD_SIZE: usize = 5 * 1024 * 1024 * 1024 * 1024;

    let file_meta = tokio_fs::metadata(local_source_path).await?;
    let file_size = file_meta.len() as usize;

    // check if the file size exceeds the maximum upload size
    if file_size > MAX_UPLOAD_SIZE {
        return Err(CollectionError::service_error(format!(
            "File size exceeds the maximum upload size: {MAX_UPLOAD_SIZE}"
        )));
    }

    // check if the file size exceeds the maximum part number
    // if so, adjust the chunk size to fit the maximum part number
    if file_size > DEFAULT_CHUNK_SIZE * MAX_PART_NUMBER {
        let chunk_size = ((file_size - 1) / MAX_PART_NUMBER) + 1; // ceil((file_size) / MAX_PART_NUMBER)
        return Ok(chunk_size);
    }
    Ok(DEFAULT_CHUNK_SIZE)
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
    use object_store::memory::InMemory;

    use super::*;

    #[tokio::test]
    async fn multipart_upload_stores_whole_file() {
        let store = InMemory::new();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source.snapshot");
        // One full 50 MiB part plus a short last part.
        let content: Vec<u8> = (0..50 * 1024 * 1024 + 12_345u32)
            .map(|i| (i % 251) as u8)
            .collect();
        fs::write(&source, &content).unwrap();
        let target = Path::new("./snapshots/collection/test.snapshot");

        multipart_upload(&store, &source, target).await.unwrap();

        let key = trim_dot_slash(target).unwrap();
        let stored = store.get(&key).await.unwrap().bytes().await.unwrap();
        // Not `assert_eq!`, which would print both 50 MiB buffers on failure.
        assert!(stored.as_ref() == content.as_slice(), "stored bytes differ");
    }

    /// Reading a directory fails with `EISDIR`, which is how these tests trigger a read error.
    #[cfg(unix)]
    mod read_error {
        use std::fmt;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        use async_trait::async_trait;
        use futures::stream::BoxStream;
        use object_store::{
            CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
            ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, UploadPart,
        };

        use super::*;

        /// Wraps `InMemory`, recording how multipart uploads end.
        #[derive(Debug, Default)]
        struct RecordingStore {
            inner: InMemory,
            fail_abort: bool,
            aborted: Arc<AtomicBool>,
            completed: Arc<AtomicBool>,
        }

        #[derive(Debug)]
        struct RecordingUpload {
            inner: Box<dyn MultipartUpload>,
            fail_abort: bool,
            aborted: Arc<AtomicBool>,
            completed: Arc<AtomicBool>,
        }

        impl fmt::Display for RecordingStore {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "RecordingStore({})", self.inner)
            }
        }

        #[async_trait]
        impl ObjectStore for RecordingStore {
            async fn put_opts(
                &self,
                location: &object_store::path::Path,
                payload: PutPayload,
                opts: PutOptions,
            ) -> object_store::Result<PutResult> {
                self.inner.put_opts(location, payload, opts).await
            }

            async fn put_multipart_opts(
                &self,
                location: &object_store::path::Path,
                opts: PutMultipartOptions,
            ) -> object_store::Result<Box<dyn MultipartUpload>> {
                let inner = self.inner.put_multipart_opts(location, opts).await?;
                Ok(Box::new(RecordingUpload {
                    inner,
                    fail_abort: self.fail_abort,
                    aborted: self.aborted.clone(),
                    completed: self.completed.clone(),
                }))
            }

            async fn get_opts(
                &self,
                location: &object_store::path::Path,
                options: GetOptions,
            ) -> object_store::Result<GetResult> {
                self.inner.get_opts(location, options).await
            }

            fn delete_stream(
                &self,
                locations: BoxStream<'static, object_store::Result<object_store::path::Path>>,
            ) -> BoxStream<'static, object_store::Result<object_store::path::Path>> {
                self.inner.delete_stream(locations)
            }

            fn list(
                &self,
                prefix: Option<&object_store::path::Path>,
            ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
                self.inner.list(prefix)
            }

            async fn list_with_delimiter(
                &self,
                prefix: Option<&object_store::path::Path>,
            ) -> object_store::Result<ListResult> {
                self.inner.list_with_delimiter(prefix).await
            }

            async fn copy_opts(
                &self,
                from: &object_store::path::Path,
                to: &object_store::path::Path,
                options: CopyOptions,
            ) -> object_store::Result<()> {
                self.inner.copy_opts(from, to, options).await
            }
        }

        #[async_trait]
        impl MultipartUpload for RecordingUpload {
            fn put_part(&mut self, data: PutPayload) -> UploadPart {
                self.inner.put_part(data)
            }

            async fn complete(&mut self) -> object_store::Result<PutResult> {
                self.completed.store(true, Ordering::SeqCst);
                self.inner.complete().await
            }

            async fn abort(&mut self) -> object_store::Result<()> {
                self.aborted.store(true, Ordering::SeqCst);
                if self.fail_abort {
                    return Err(object_store::Error::Generic {
                        store: "RecordingStore",
                        source: "abort failed".into(),
                    });
                }
                self.inner.abort().await
            }
        }

        /// Uploads a directory as the source file, which fails on the first read, and returns the error.
        async fn upload_failing_read(store: &RecordingStore) -> CollectionError {
            let directory_source = tempfile::tempdir().unwrap();
            let target = Path::new("./snapshots/collection/test.snapshot");
            multipart_upload(store, directory_source.path(), target)
                .await
                .unwrap_err()
        }

        // Formatting an `fs-err` error blocks (its `debug` feature), which needs a multi-thread runtime.
        #[tokio::test(flavor = "multi_thread")]
        async fn multipart_upload_aborts_on_read_error() {
            let store = RecordingStore::default();

            let err = upload_failing_read(&store).await;

            assert!(
                matches!(&err, CollectionError::ServiceError { error, .. }
                    if error.contains("Failed to read snapshot file")),
                "unexpected error: {err}",
            );
            assert!(
                store.aborted.load(Ordering::SeqCst),
                "a failed upload must be aborted",
            );
            assert!(
                !store.completed.load(Ordering::SeqCst),
                "a failed upload must not be completed",
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn multipart_upload_returns_read_error_when_abort_fails() {
            let store = RecordingStore {
                fail_abort: true,
                ..Default::default()
            };

            let err = upload_failing_read(&store).await;

            assert!(
                matches!(&err, CollectionError::ServiceError { error, .. }
                    if error.contains("Failed to read snapshot file")),
                "the read error must not be replaced by the abort error: {err}",
            );
            assert!(
                !store.completed.load(Ordering::SeqCst),
                "a failed upload must not be completed",
            );
        }
    }
}
