use std::path::{Path, PathBuf};
use std::{fmt, io, result};

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
    let response = reqwest::get(url).await.map_err(|err| Error::new(err, ""))?;

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

        while let Some(bytes) = bytes_stream
            .try_next()
            .await
            .map_err(|err| Error::new(err, ""))?
        {
            writer
                .write_all(&bytes)
                .await
                .map_err(|err| Error::new(err, ""))?;
        }

        writer.shutdown().await.map_err(|err| Error::new(err, ""))?;
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
pub struct Error {
    source: Source,
    context: String,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let context = self.context.trim();
        let context_sep = if !context.is_empty() { ": " } else { "" };

        let source = &self.source;

        write!(f, "{context}{context_sep}{source}")
    }
}

impl Error {
    pub fn new(source: impl Into<Source>, context: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            context: context.into(),
        }
    }
}

impl<E: Into<Source>> From<E> for Error {
    fn from(error: E) -> Self {
        Self::new(error, String::new())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub enum Source {
    Io(#[from] io::Error),
    Reqwest(#[from] reqwest::Error),
    AsyncTempfile(#[from] async_tempfile::Error),
}
