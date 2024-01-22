use std::path::{Path, PathBuf};

use futures::StreamExt;
use reqwest;
use snapshot_manager::file::SnapshotFile;
use snapshot_manager::SnapshotManager;
use tempfile::TempPath;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use url::Url;
use uuid::Uuid;

use crate::StorageError;

fn random_name() -> String {
    format!("{}.snapshot", Uuid::new_v4())
}

fn snapshot_name(url: &Url) -> String {
    let path = Path::new(url.path());

    path.file_name()
        .and_then(|x| x.to_str())
        .map(|x| x.to_string())
        .unwrap_or_else(random_name)
}

/// Download a remote file from `url` to `path`
///
/// Returns a `TempPath` that will delete the downloaded file once it is dropped.
/// To persist the file, use `download_file(...).keep()`.
#[must_use = "returns a TempPath, if dropped the downloaded file is deleted"]
async fn download_file(
    client: &reqwest::Client,
    url: &Url,
    path: &Path,
) -> Result<TempPath, StorageError> {
    let temp_path = TempPath::from_path(path);
    let mut file = File::create(path).await?;

    let response = client.get(url.clone()).send().await?;

    if !response.status().is_success() {
        return Err(StorageError::bad_input(format!(
            "Failed to download snapshot from {}: status - {}",
            url,
            response.status()
        )));
    }

    let mut stream = response.bytes_stream();

    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result?;
        file.write_all(&chunk).await?;
    }

    file.flush().await?;

    Ok(temp_path)
}

/// Download a snapshot from the given URI.
///
/// May returen a `TempPath` if a file was downloaded from a remote source. If it is dropped the
/// downloaded file is deleted automatically. To keep the file `keep()` may be used.
#[must_use = "may return a TempPath, if dropped the downloaded file is deleted"]
pub async fn download_snapshot(
    client: &reqwest::Client,
    url: Url,
    snapshot_manager: SnapshotManager,
    snapshots_dir: &Path,
) -> Result<(PathBuf, Option<TempPath>), StorageError> {
    match url.scheme() {
        "file" => {
            let local_path = url.to_file_path().map_err(|_| {
                StorageError::bad_request(
                    "Invalid snapshot URI, file path must be absolute or on localhost",
                )
            })?;
            if snapshot_manager.is_path_on_s3(&local_path).await? {
                let snapshot = SnapshotFile::new_oop(local_path);
                snapshot_manager
                    .get_snapshot_path(&snapshot)
                    .await
                    .map_err(|x| x.into())
            } else {
                if !local_path.exists() {
                    return Err(StorageError::bad_request(format!(
                        "Snapshot file {local_path:?} does not exist"
                    )));
                }
                Ok((local_path, None))
            }
        }
        "http" | "https" => {
            let download_to = snapshots_dir.join(snapshot_name(&url));

            let temp_path = download_file(client, &url, &download_to).await?;
            Ok((download_to, Some(temp_path)))
        }
        _ => Err(StorageError::bad_request(format!(
            "URL {} with schema {} is not supported",
            url,
            url.scheme()
        ))),
    }
}
