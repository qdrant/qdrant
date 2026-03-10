use std::ffi::OsString;
use std::path::{Path, PathBuf};

use collection::common::sha_256::hash_file;
use common::tempfile_ext::MaybeTempPath;
use reqwest;
use shard::snapshots::snapshot_data::SnapshotData;
use tap::Tap;
use tempfile::TempDir;
use url::Url;

use crate::StorageError;
use crate::content_manager::snapshots::download_result::DownloadResult;
use crate::content_manager::snapshots::download_tar::download_and_unpack_tar;

fn snapshot_prefix(url: &Url) -> OsString {
    Path::new(url.path())
        .file_name()
        .map(|x| OsString::from(x).tap_mut(|x| x.push("-")))
        .unwrap_or_default()
}

/// Download and unpack a snapshot from `url` into a temporary directory.
///
/// Returns a `TempDir` that will delete the downloaded file once it is dropped.
/// To persist the file, use [`keep()`](TempDir::keep).
#[must_use = "returns a TempDir, if dropped the downloaded file is deleted"]
async fn _download_snapshot(
    client: &reqwest::Client,
    url: &Url,
    dir_path: &Path,
    compute_checksum: bool,
) -> Result<(TempDir, Option<String>), StorageError> {
    let download_start_time = tokio::time::Instant::now();

    let snapshot_name = snapshot_prefix(url);

    let tempdir = tempfile::Builder::new()
        .prefix(&snapshot_name)
        .suffix(".download")
        .tempdir_in(dir_path)?;

    let hash = download_and_unpack_tar(client, url, tempdir.path(), compute_checksum).await?;

    let download_duration = download_start_time.elapsed();
    log::debug!(
        "Snapshot download completed: path={tempdir:?}, duration={:.2}s",
        download_duration.as_secs_f64(),
    );

    Ok((tempdir, hash))
}

/// Validate that the given path is within the allowed snapshots directory.
///
/// Canonicalizes both paths to resolve symlinks and `..` components,
/// then checks that `path` is a descendant of `allowed_dir`.
fn validate_snapshot_path(path: &Path, allowed_dir: &Path) -> Result<PathBuf, StorageError> {
    let canonical_path = fs_err::canonicalize(path).map_err(|err| {
        StorageError::bad_request(format!("Failed to resolve snapshot path {path:?}: {err}"))
    })?;

    let canonical_allowed = fs_err::canonicalize(allowed_dir).map_err(|err| {
        StorageError::service_error(format!(
            "Failed to resolve snapshots directory {allowed_dir:?}: {err}"
        ))
    })?;

    if !canonical_path.starts_with(&canonical_allowed) {
        return Err(StorageError::forbidden(format!(
            "Snapshot file path must be inside the snapshots directory {canonical_allowed:?}",
        )));
    }

    Ok(canonical_path)
}

/// Download a snapshot from the given URI.
///
/// Returns a `DownloadResult` containing the snapshot data and optional checksum.
///
/// For `file://` URLs, `snapshots_path` is used to validate that the referenced file
/// is within the snapshots directory. For `http`/`https` URLs, `download_dir` is used
/// as the temporary download location.
pub async fn download_snapshot(
    client: &reqwest::Client,
    url: Url,
    download_dir: &Path,
    snapshots_path: &Path,
    compute_checksum: bool,
) -> Result<DownloadResult, StorageError> {
    match url.scheme() {
        "file" => {
            let local_path = url.to_file_path().map_err(|_| {
                StorageError::bad_request(
                    "Invalid snapshot URI, file path must be absolute or on localhost",
                )
            })?;
            if !local_path.exists() {
                return Err(StorageError::bad_request(format!(
                    "Snapshot file {local_path:?} does not exist"
                )));
            }

            // Restrict file:// URLs to the snapshots directory
            let local_path = validate_snapshot_path(&local_path, snapshots_path)?;

            let hash = if compute_checksum {
                Some(hash_file(&local_path).await?)
            } else {
                None
            };

            Ok(DownloadResult {
                snapshot: SnapshotData::Packed(MaybeTempPath::Persistent(local_path)),
                hash,
            })
        }
        "http" | "https" => {
            let (snapshot_dir, hash) =
                _download_snapshot(client, &url, download_dir, compute_checksum).await?;
            Ok(DownloadResult {
                snapshot: SnapshotData::Unpacked(snapshot_dir),
                hash,
            })
        }
        _ => Err(StorageError::bad_request(format!(
            "URL {url} with scheme {} is not supported",
            url.scheme(),
        ))),
    }
}
