use std::path::{Path, PathBuf};

use futures::StreamExt;
use reqwest;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use url::Url;
use uuid::Uuid;

use crate::StorageError;

fn downloaded_snapshots_dir(snapshots_dir: &str) -> PathBuf {
    Path::new(snapshots_dir).join("downloaded-snapshots")
}

fn random_name() -> String {
    format!("{}.snapshot", Uuid::new_v4().to_string())
}

fn snapshot_name(url: &Url) -> String {
    let path = Path::new(url.path());

    path.file_name()
        .and_then(|x| x.to_str())
        .map(|x| x.to_string())
        .unwrap_or_else(random_name)
}

async fn download_files(url: &Url, path: &Path) -> Result<(), StorageError> {
    let mut file = File::create(path).await?;

    let response = reqwest::get(url.clone()).await?;
    let mut stream = response.bytes_stream();

    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result?;
        file.write_all(&chunk).await?;
    }

    file.flush().await?;

    Ok(())
}

pub async fn download_snapshot(url: Url, snapshots_dir: &str) -> Result<PathBuf, StorageError> {
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
            let download_to = Path::new(snapshots_dir).join(snapshot_name(&url));

            download_files(&url, &download_to).await?;
            Ok(download_to)
        }
        _ => {
            return Err(StorageError::bad_request(&format!(
                "URL {} with schema {} is not supported",
                url,
                url.scheme()
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_url_parsing() {
        let remote_url = Url::parse("http://localhost:6333/collections/test_collection/snapshots/test_collection-2022-08-04-10-49-10.snapshot").unwrap();
        let local_url =
            Url::parse("file:///qdrant/snapshots/test_collection-2022-08-04-10-49-10.snapshot")
                .unwrap();

        eprintln!("local_url_abs.path() = {:#?}", remote_url.path());
    }
}
