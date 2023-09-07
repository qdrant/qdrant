use std::path::{Path, PathBuf};
use std::{io, result};

use futures::TryStreamExt as _;
use tokio::io::AsyncWriteExt as _;
use url::Url;

#[derive(Debug)]
pub struct DownloadedFile {
    pub file: async_tempfile::NamedTempFile,
    pub url: Url,
    pub file_name: Option<String>,
}

pub async fn download_file(url: Url, dir: impl Into<PathBuf>) -> Result<DownloadedFile> {
    let response = reqwest::get(url).await?;

    response.error_for_status_ref()?;

    let mut file = async_tempfile::NamedTempFile::new_in(dir).await?;

    // Save *final* (i.e., after possible redirects) URL
    let url = response.url().clone();

    // Parse file name from the *final* URL
    // TODO: Parse file name from `Content-Disposition` header!?
    let file_name = Path::new(url.path())
        .file_name()
        .and_then(|str| str.to_str())
        .map(|str| str.to_string());

    {
        let mut bytes_stream = response.bytes_stream();
        let mut writer = tokio::io::BufWriter::new(file.as_file_mut());

        while let Some(bytes) = bytes_stream.try_next().await? {
            writer.write_all(&bytes).await?;
        }

        writer.shutdown().await?;
    }

    let downloaded_file = DownloadedFile {
        file,
        url,
        file_name,
    };

    Ok(downloaded_file)
}

pub type Result<T, E = Error> = result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub enum Error {
    Io(#[from] io::Error),
    Reqwest(#[from] reqwest::Error),
    AsyncTempfile(#[from] async_tempfile::Error),
}
