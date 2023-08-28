use std::path::{Path, PathBuf};

use futures::StreamExt;
use reqwest;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use url::Url;
use uuid::Uuid;

use crate::StorageError;

pub fn downloaded_snapshots_dir(snapshots_dir: &str) -> PathBuf {
    Path::new(snapshots_dir).join("downloaded-snapshots")
}

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

async fn download_file(url: &Url, path: &Path) -> Result<(), StorageError> {
    let mut file = File::create(path).await?;

    let response = reqwest::get(url.clone()).await?;

    if !response.status().is_success() {
        return Err(StorageError::bad_input(&format!(
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

    Ok(())
}

/// Download a snapshot file an URI, return a file path to it
///
/// For remote resources such as an URL, this downloads the given file and puts it in
/// `snapshots_dir`. A path to the downloaded file is returned.
///
/// For `file://` URIs a direct path is returned.
///
/// # Security
///
/// A `file://` URI may point to arbitrary files on the file system, which could be a security
/// concern. Set `strict_file` to `true` to enforce a local file to be inside `snapshots_dir`.
pub async fn download_snapshot(
    url: Url,
    snapshots_dir: &Path,
    strict_file: bool,
) -> Result<PathBuf, StorageError> {
    match url.scheme() {
        "file" => {
            let local_path = url.to_file_path().map_err(|_| {
                StorageError::bad_request(
                    "Invalid snapshot URI, file path must be absolute or on localhost",
                )
            })?;
            if !local_path.exists() {
                return Err(StorageError::bad_request(&format!(
                    "Snapshot file {local_path:?} does not exist"
                )));
            }

            let local_path = local_path.canonicalize().unwrap_or(local_path);

            // Prevent using arbitrary files from our file system, enforce the file to be in the
            // snapshots directory
            if strict_file {
                let snapshots_dir = snapshots_dir
                    .canonicalize()
                    .unwrap_or_else(|_| snapshots_dir.to_path_buf());
                if !local_path.starts_with(snapshots_dir) {
                    return Err(StorageError::forbidden(&format!(
                        "Snapshot file {local_path:?} must be inside snapshots dir"
                    )));
                }
            }

            Ok(local_path)
        }
        "http" | "https" => {
            let download_to = snapshots_dir.join(snapshot_name(&url));

            download_file(&url, &download_to).await?;
            Ok(download_to)
        }
        _ => Err(StorageError::bad_request(&format!(
            "URL {} with schema {} is not supported",
            url,
            url.scheme()
        ))),
    }
}
