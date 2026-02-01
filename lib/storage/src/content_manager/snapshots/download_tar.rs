use std::path::Path;

use futures::TryStreamExt;
use tokio_util::io::StreamReader;
use url::Url;

use crate::StorageError;

/// Download and unpack a tar file in streaming fashion without saving to disk first.
///
/// This function streams the HTTP response directly into the tar extractor,
/// avoiding the need to store the entire tar file on disk before extraction.
///
/// # Arguments
///
/// * `client` - The reqwest HTTP client to use for the download
/// * `url` - The URL to download the tar file from
/// * `target_dir` - The directory to extract the tar contents into
///
/// # Returns
///
/// Returns `Ok(())` on success, or a `StorageError` if the download or extraction fails.
pub async fn download_and_unpack_tar(
    client: &reqwest::Client,
    url: &Url,
    target_dir: &Path,
) -> Result<(), StorageError> {
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

    // Convert the response body stream into an AsyncRead
    let stream = response
        .bytes_stream()
        .map_err(std::io::Error::other);
    let async_reader = StreamReader::new(stream);

    let target_dir = target_dir.to_path_buf();
    let target_dir_for_log = target_dir.clone();

    // Use spawn_blocking because tar::Archive is synchronous
    tokio::task::spawn_blocking(move || {
        // SyncIoBridge converts an AsyncRead into a sync Read
        // It must be used within a tokio runtime context (spawn_blocking provides this)
        let sync_reader = tokio_util::io::SyncIoBridge::new(async_reader);
        let mut archive = tar::Archive::new(sync_reader);

        archive.unpack(&target_dir).map_err(|e| {
            StorageError::service_error(format!("Failed to unpack tar archive: {e}"))
        })?;

        Ok::<(), StorageError>(())
    })
    .await??;

    log::debug!(
        "Successfully unpacked tar from {url} to {}",
        target_dir_for_log.display()
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_download_and_unpack_tar_invalid_url() {
        let client = reqwest::Client::new();
        let url = Url::parse("https://storage.googleapis.com/qdrant-benchmark-snapshots/test-shard.snapshot").unwrap();
        let temp_dir = tempfile::tempdir().unwrap();

        let result = download_and_unpack_tar(&client, &url, temp_dir.path()).await;

        // Print content of the directory for debugging
        let entries: Vec<_> = std::fs::read_dir(temp_dir.path())
            .unwrap()
            .map(|res| res.unwrap().file_name().display().to_string())
            .collect();

        assert!(entries.contains(&"wal".to_string()));
        assert!(result.is_ok());
    }
}
