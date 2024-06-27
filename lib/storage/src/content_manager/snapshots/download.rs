use std::path::{Path, PathBuf};

use futures::StreamExt;
use reqwest;
use tempfile::TempPath;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use url::Url;
use uuid::Uuid;

use crate::StorageError;

/// Special file:// URI prefix for relative paths
const URI_FILE_RELATIVE_PREFIX: &str = "file://./";

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

/// Download a snapshot file an URI, return a file path to it
///
/// For remote resources such as an URL, this downloads the given file and puts it in
/// `snapshots_dir`. A path to the downloaded file is returned.
///
/// For `file://` URIs a direct path is returned.
///
/// May returen a `TempPath` if a file was downloaded from a remote source. If it is dropped the
/// downloaded file is deleted automatically. To keep the file `keep()` may be used.
///
/// # Security
///
/// A `file://` URI may point to arbitrary files on the file system, which could be a security
/// concern. Set `strict_file` to `true` to enforce a local file to be inside `snapshots_dir`.
#[must_use = "may return a TempPath, if dropped the downloaded file is deleted"]
pub async fn download_snapshot(
    client: &reqwest::Client,
    url: Url,
    snapshots_dir: &Path,
    only_snapshot_dir: bool,
) -> Result<(PathBuf, Option<TempPath>), StorageError> {
    match url.scheme() {
        "file" => {
            let local_path = resolve_uri_file_path(&url, snapshots_dir)?;
            if !local_path.exists() {
                // Report user provided URL here to prevent leaking the local path
                return Err(StorageError::bad_request(format!(
                    "Snapshot file {:?} does not exist",
                    url.to_string(),
                )));
            }

            let local_path = local_path.canonicalize().unwrap_or(local_path);

            // Prevent using arbitrary files from our file system, enforce the file to be in the
            // snapshots directory
            if only_snapshot_dir {
                let snapshots_dir = snapshots_dir
                    .canonicalize()
                    .unwrap_or_else(|_| snapshots_dir.to_path_buf());
                if !local_path.starts_with(snapshots_dir) {
                    return Err(StorageError::forbidden(format!(
                        "Snapshot file {local_path:?} must be inside snapshots directory",
                    )));
                }
            }

            Ok((local_path, None))
        }
        "http" | "https" => {
            let download_to = snapshots_dir.join(snapshot_name(&url));

            log::debug!(
                "Downloading snapshot from {url} to {}",
                snapshots_dir.display(),
            );

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

/// Resolve a file:// URI to a local path
///
/// This supports both absolute and relative paths. If the path is relative, it is resolved within
/// the given `workdir`.
///
/// # Security
///
/// This may point to arbitrary files. The resolved file may not exist.
pub fn resolve_uri_file_path(url: &Url, workdir: &Path) -> Result<PathBuf, StorageError> {
    // Must be a file URI
    if url.scheme() != "file" {
        return Err(StorageError::service_error(
            "provided URI is not a file:// URI",
        ));
    }

    // Parse relative path with specific prefix, normally not supported
    if let Some(relative_path) = url.to_string().strip_prefix(URI_FILE_RELATIVE_PREFIX) {
        let workdir = workdir
            .canonicalize()
            .unwrap_or_else(|_| workdir.to_path_buf());
        return Ok(workdir.join(relative_path));
    }

    // Parse absolute path
    url.to_file_path()
        .map_err(|_| StorageError::bad_request("Malformed file URI"))
}
