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

pub async fn download_snapshot(url: Url, snapshots_dir: &Path) -> Result<PathBuf, StorageError> {
    match url.scheme() {
        "file" => {
            let local_file_path = Path::new(url.path());
            if !local_file_path.exists() {
                return Err(StorageError::bad_request(&format!(
                    "Snapshot file {:?} does not exist",
                    local_file_path
                )));
            }
            Ok(local_file_path.to_path_buf())
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
