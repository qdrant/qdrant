use std::ffi::OsString;
use std::path::Path;

use common::tempfile_ext::MaybeTempPath;
use fs_err::tokio as tokio_fs;
use futures::StreamExt;
use segment::common::BYTES_IN_MB;
use tap::Tap;
use tempfile::TempPath;
use tokio::io::AsyncWriteExt;
use url::Url;
use {fs_err as fs, reqwest};

use crate::StorageError;

fn snapshot_prefix(url: &Url) -> OsString {
    Path::new(url.path())
        .file_name()
        .map(|x| OsString::from(x).tap_mut(|x| x.push("-")))
        .unwrap_or_default()
}

/// Download a remote file from `url` to `path`
///
/// Returns a `TempPath` that will delete the downloaded file once it is dropped.
/// To persist the file, use `download_file(...).keep()`.
#[must_use = "returns a TempPath, if dropped the downloaded file is deleted"]
async fn download_file(
    client: &reqwest::Client,
    url: &Url,
    dir_path: &Path,
) -> Result<TempPath, StorageError> {
    let download_start_time = tokio::time::Instant::now();
    let (file, temp_path) = tempfile::Builder::new()
        .prefix(&snapshot_prefix(url))
        .suffix(".download")
        .tempfile_in(dir_path)?
        .into_parts();
    let file = fs::File::from_parts::<&Path>(file, temp_path.as_ref());

    log::debug!("Downloading snapshot from {url} to {temp_path:?}");

    let mut file = tokio_fs::File::from_std(file);

    let response = client.get(url.clone()).send().await?;

    if !response.status().is_success() {
        return Err(StorageError::bad_input(format!(
            "Failed to download snapshot from {}: status - {}",
            url,
            response.status()
        )));
    }

    let mut stream = response.bytes_stream();
    let mut total_bytes_downloaded = 0u64;

    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result?;
        total_bytes_downloaded += chunk.len() as u64;
        file.write_all(&chunk).await?;
    }

    file.flush().await?;

    let download_duration = download_start_time.elapsed();
    let total_size_mb = total_bytes_downloaded as f64 / BYTES_IN_MB as f64;
    let download_speed_mbps = total_size_mb / download_duration.as_secs_f64();
    log::debug!(
        "Snapshot download completed: path={}, size={:.2} MB, duration={:.2}s, speed={:.2} MB/s",
        temp_path.display(),
        total_size_mb,
        download_duration.as_secs_f64(),
        download_speed_mbps
    );

    Ok(temp_path)
}

/// Download a snapshot from the given URI.
///
/// May returen a `TempPath` if a file was downloaded from a remote source. If it is dropped the
/// downloaded file is deleted automatically. To keep the file `keep()` may be used.
pub async fn download_snapshot(
    client: &reqwest::Client,
    url: Url,
    snapshots_dir: &Path,
) -> Result<MaybeTempPath, StorageError> {
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
            Ok(MaybeTempPath::Persistent(local_path))
        }
        "http" | "https" => Ok(MaybeTempPath::Temporary(
            download_file(client, &url, snapshots_dir).await?,
        )),
        _ => Err(StorageError::bad_request(format!(
            "URL {} with schema {} is not supported",
            url,
            url.scheme()
        ))),
    }
}
