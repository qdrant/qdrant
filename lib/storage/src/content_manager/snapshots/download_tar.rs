use std::io::Read;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use cancel::CancellationToken;
use common::tar_unpack::tar_unpack_reader;
use futures::TryStreamExt;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, ReadBuf};
use tokio::time::{Instant, Sleep};
use tokio_util::io::StreamReader;
use url::Url;

use crate::StorageError;

/// Timeout for stream reads - if no data is received within this duration, the download fails.
const STREAM_READ_TIMEOUT: Duration = Duration::from_secs(60);

/// An async reader wrapper that times out if no data is received within a specified duration.
///
/// This implements an inactivity timeout - the timeout resets each time data is successfully read.
/// If the underlying reader returns `Pending` for too long without making progress, the read
/// will fail with `io::ErrorKind::TimedOut`.
struct TimeoutReader<R> {
    inner: Pin<Box<R>>,
    sleep: Pin<Box<Sleep>>,
    timeout: Duration,
}

impl<R> TimeoutReader<R> {
    fn new(inner: R, timeout: Duration) -> Self {
        Self {
            inner: Box::pin(inner),
            sleep: Box::pin(tokio::time::sleep(timeout)),
            timeout,
        }
    }
}

impl<R: AsyncRead> AsyncRead for TimeoutReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // Safe to get &mut self because TimeoutReader is Unpin
        // (all fields are Unpin: Pin<Box<_>> is Unpin, Duration is Unpin)
        let this = &mut *self;
        let filled_before = buf.filled().len();

        // First, try to read from the inner reader
        match this.inner.as_mut().poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                // If we read some data, reset the timeout
                if buf.filled().len() > filled_before {
                    this.sleep.as_mut().reset(Instant::now() + this.timeout);
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => {
                // Inner is not ready, check if timeout has expired
                // This also registers the sleep's waker so we wake up on timeout
                if this.sleep.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "stream read timeout: no data received within timeout period",
                    )));
                }
                Poll::Pending
            }
        }
    }
}

/// A sync Read wrapper that checks a cancellation token before each read.
struct CancellableReader<R> {
    inner: R,
    cancel: CancellationToken,
}

impl<R> CancellableReader<R> {
    fn new(inner: R, cancel: CancellationToken) -> Self {
        Self { inner, cancel }
    }
}

impl<R: Read> Read for CancellableReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.cancel.is_cancelled() {
            return Err(std::io::Error::other("download cancelled"));
        }
        self.inner.read(buf)
    }
}

/// A sync Read wrapper that computes SHA-256 hash of the data as it's read.
struct HashingReader<R> {
    inner: R,
    hasher: Option<Sha256>,
}

impl<R> HashingReader<R> {
    fn new(inner: R, compute_hash: bool) -> Self {
        Self {
            inner,
            hasher: compute_hash.then(Sha256::new),
        }
    }

    /// Consume the reader and return the computed hash as a hex string.
    /// Returns None if hashing was not enabled.
    fn finalize(self) -> Option<String> {
        self.hasher.map(|h| {
            let hash = h.finalize();
            format!("{hash:x}")
        })
    }
}

impl<R: Read> Read for HashingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        if let Some(ref mut hasher) = self.hasher {
            hasher.update(&buf[..n]);
        }
        Ok(n)
    }
}

/// Download and unpack a tar file in streaming fashion without saving to disk first.
///
/// This function streams the HTTP response directly into the tar extractor,
/// avoiding the need to store the entire tar file on disk before extraction.
///
/// # Cancel safety
///
/// This function is cancel safe. If cancelled, the cancellation token will be triggered
/// and the download will be interrupted at the next read operation.
///
/// # Arguments
///
/// * `client` - The reqwest HTTP client to use for the download
/// * `url` - The URL to download the tar file from
/// * `target_dir` - The directory to extract the tar contents into
/// * `compute_checksum` - If true, compute and return the SHA-256 hash of the downloaded data
///
/// # Returns
///
/// Returns `Ok(Some(hash))` if `compute_checksum` is true, `Ok(None)` otherwise.
/// Returns a `StorageError` if the download or extraction fails.
pub async fn download_and_unpack_tar(
    client: &reqwest::Client,
    url: &Url,
    target_dir: &Path,
    compute_checksum: bool,
) -> Result<Option<String>, StorageError> {
    log::debug!(
        "Streaming tar download from {url} to {}",
        target_dir.display()
    );

    let response = client.get(url.clone()).send().await?;

    if !response.status().is_success() {
        return Err(StorageError::bad_input(format!(
            "Failed to download tar from {url}: status - {}",
            response.status()
        )));
    }

    // Convert the response body stream into an AsyncRead with timeout
    let stream = response.bytes_stream().map_err(std::io::Error::other);
    let stream_reader = StreamReader::new(stream);
    // Wrap with timeout to detect stalled downloads
    let async_reader = TimeoutReader::new(stream_reader, STREAM_READ_TIMEOUT);

    let target_dir = target_dir.to_path_buf();
    let target_dir_for_log = target_dir.clone();

    // Use spawn_cancel_on_drop to ensure the blocking task is cancelled when the future is dropped
    let hash = cancel::blocking::spawn_cancel_on_drop(move |cancel| {
        // SyncIoBridge converts an AsyncRead into a sync Read
        // It must be used within a tokio runtime context (spawn_blocking provides this)
        let sync_reader = tokio_util::io::SyncIoBridge::new(async_reader);

        // Wrap the reader with cancellation support
        let cancellable_reader = CancellableReader::new(sync_reader, cancel);

        // Wrap the reader with optional hashing
        let hashing_reader = HashingReader::new(cancellable_reader, compute_checksum);

        let mut reader = tar_unpack_reader(hashing_reader, &target_dir).map_err(|e| {
            StorageError::service_error(format!("Failed to unpack tar archive: {e}"))
        })?;

        // Drain any remaining bytes to ensure the full stream is hashed.
        // Tar files have trailing padding that Archive doesn't read.
        if reader.hasher.is_some() {
            let mut buf = [0u8; 8192];
            while reader.read(&mut buf)? > 0 {}
        }

        // Get the hash from the inner reader
        let hash = reader.finalize();

        Ok::<Option<String>, StorageError>(hash)
    })
    .await
    .map_err(|e| StorageError::service_error(format!("Download task failed: {e}")))??;

    log::debug!(
        "Successfully unpacked tar from {url} to {}",
        target_dir_for_log.display()
    );

    Ok(hash)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    use futures::StreamExt;

    use super::*;

    #[tokio::test]
    async fn test_download_and_unpack_tar() {
        let mut server = mockito::Server::new_async().await;
        server
            .mock("GET", "/test-shard.snapshot")
            .with_body(include_bytes!("./test-shard.snapshot"))
            .create();
        let url = Url::parse(&format!("{}/test-shard.snapshot", server.url())).unwrap();

        let client = reqwest::Client::new();
        let temp_dir = tempfile::tempdir().unwrap();

        let hash = download_and_unpack_tar(&client, &url, temp_dir.path(), true)
            .await
            .unwrap();

        let hash = hash.expect("Hash should be computed");

        // Verify the expected hash
        assert_eq!(
            hash,
            "5d94eac5c1ede3994a28bc406120046c37370d5d45b489a0d2252531b4e3e1f2",
        );

        // Verify content was extracted
        let entries: Vec<_> = fs_err::read_dir(temp_dir.path())
            .unwrap()
            .map(|res| res.unwrap().file_name().to_string_lossy().to_string())
            .collect();

        assert!(entries.contains(&"wal".to_string()));
    }

    #[tokio::test]
    async fn test_async_cancellation() {
        let is_finished: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
        let is_finished_clone = is_finished.clone();

        let stream = futures::stream::iter(0..100).then(|_| async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok::<std::io::Cursor<Vec<u8>>, std::io::Error>(std::io::Cursor::new(vec![1u8]))
        });
        let stream_reader = StreamReader::new(stream);

        let async_reader = TimeoutReader::new(stream_reader, STREAM_READ_TIMEOUT);

        // Use spawn_cancel_on_drop to ensure the blocking task is canceled when the future is dropped
        let handler = async {
            cancel::blocking::spawn_cancel_on_drop(move |cancel| {
                // SyncIoBridge converts an AsyncRead into a sync Read
                // It must be used within a tokio runtime context (spawn_blocking provides this)
                let sync_reader = tokio_util::io::SyncIoBridge::new(async_reader);

                // Wrap the reader with cancellation support
                let mut cancellable_reader = CancellableReader::new(sync_reader, cancel);

                let mut buf = [0u8; 8192];

                loop {
                    let res = cancellable_reader.read(&mut buf);
                    if res.is_err() {
                        break;
                    }
                }

                is_finished.store(true, std::sync::atomic::Ordering::SeqCst);
            })
            .await
        };

        tokio::select! {
            _ = handler => {
                // Task finished on its own, which is unexpected in this test

            }
             _ = tokio::time::sleep(Duration::from_millis(50)) => {
                // Timeout waiting for task to finish, which means cancellation likely failed
                eprintln!("Cancelled");
            }
        }

        // Check that the task was stopped.
        tokio::time::timeout(Duration::from_millis(100), async {
            while !is_finished_clone.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("Task was not cancelled properly");
    }
}
