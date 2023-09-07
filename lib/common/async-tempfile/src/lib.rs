use std::path::{Path, PathBuf};
use std::{fmt, io, result};

#[derive(Debug)]
pub struct NamedTempFile {
    inner: Inner<tempfile::NamedTempFile<tokio::fs::File>>,
}

impl NamedTempFile {
    pub async fn new_in(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();

        let tempfile = tokio::task::spawn_blocking(move || {
            tempfile::NamedTempFile::new_in(&dir).map_err(move |err| Error::new(err).path(dir))
        })
        .await
        .map_err(Error::from)
        .and_then(|res| res)
        .map_err(|err| err.context("failed to create", "file"))?
        .into();

        Ok(tempfile)
    }

    pub fn as_file(&self) -> &tokio::fs::File {
        self.inner.as_ref().as_file()
    }

    pub fn as_file_mut(&mut self) -> &mut tokio::fs::File {
        self.inner.as_mut().as_file_mut()
    }

    pub fn path(&self) -> &Path {
        self.inner.path()
    }

    pub async fn keep(self) -> Result<(tokio::fs::File, PathBuf), PersistError> {
        let file_and_path = self
            .inner
            .asyncify(tempfile::NamedTempFile::keep)
            .await
            .map_err(PersistError::from)
            .and_then(|res| res.map_err(PersistError::from))
            .map_err(move |err| err.resource("file"))?;

        Ok(file_and_path)
    }

    pub async fn try_close(self) -> Result<()> {
        self.inner.try_close().await
    }

    pub async fn close(self) {
        self.inner.close().await
    }

    pub async fn into_sync(self) -> tempfile::NamedTempFile {
        let tempfile = self.inner.into_inner();
        let (file, path) = tempfile.into_parts();
        tempfile::NamedTempFile::from_parts(file.into_std().await, path)
    }
}

impl From<Inner<tempfile::NamedTempFile<tokio::fs::File>>> for NamedTempFile {
    fn from(inner: Inner<tempfile::NamedTempFile<tokio::fs::File>>) -> Self {
        Self { inner }
    }
}

impl From<tempfile::NamedTempFile<tokio::fs::File>> for NamedTempFile {
    fn from(tempfile: tempfile::NamedTempFile<tokio::fs::File>) -> Self {
        Inner::new(tempfile).into()
    }
}

impl From<tempfile::NamedTempFile> for NamedTempFile {
    fn from(tempfile: tempfile::NamedTempFile) -> Self {
        let (file, path) = tempfile.into_parts();
        tempfile::NamedTempFile::from_parts(tokio::fs::File::from_std(file), path).into()
    }
}

#[derive(Debug)]
pub struct TempDir {
    inner: Inner<tempfile::TempDir>,
}

impl TempDir {
    pub async fn new_in(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();

        let tempdir = tokio::task::spawn_blocking(move || {
            tempfile::TempDir::new_in(&dir).map_err(move |err| Error::new(err).path(dir))
        })
        .await
        .map_err(Error::from)
        .and_then(|res| res)
        .map_err(|err| err.context("failed to create", "directory"))?
        .into();

        Ok(tempdir)
    }

    pub fn path(&self) -> &Path {
        self.inner.path()
    }

    pub fn into_path(mut self) -> PathBuf {
        self.inner.take().into_path()
    }

    pub async fn try_close(self) -> Result<()> {
        self.inner.try_close().await
    }

    pub async fn close(self) {
        self.inner.close().await
    }

    pub fn into_sync(self) -> tempfile::TempDir {
        self.inner.into_inner()
    }
}

impl From<Inner<tempfile::TempDir>> for TempDir {
    fn from(inner: Inner<tempfile::TempDir>) -> Self {
        Self { inner }
    }
}

impl From<tempfile::TempDir> for TempDir {
    fn from(tempdir: tempfile::TempDir) -> Self {
        Inner::new(tempdir).into()
    }
}

#[derive(Clone, Debug, Default)]
pub struct Builder {
    prefix: Option<String>,
    suffix: Option<String>,
}

impl Builder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn prefix(&mut self, prefix: impl Into<String>) -> &mut Self {
        self.prefix = Some(prefix.into());
        self
    }

    pub fn suffix(&mut self, suffix: impl Into<String>) -> &mut Self {
        self.suffix = Some(suffix.into());
        self
    }

    pub async fn tempfile_in(&self, dir: impl Into<PathBuf>) -> Result<NamedTempFile> {
        let dir = dir.into();

        let tempfile = self
            .asyncify(move |builder| {
                builder
                    .tempfile_in(&dir)
                    .map_err(move |err| Error::new(err).path(dir))
            })
            .await
            .map_err(Error::from)
            .and_then(|res| res)
            .map_err(|err| err.context("failed to create", "file"))?
            .into();

        Ok(tempfile)
    }

    pub async fn tempdir_in(&self, dir: impl Into<PathBuf>) -> Result<TempDir> {
        let dir = dir.into();

        let tempdir = self
            .asyncify(move |builder| {
                builder
                    .tempdir_in(&dir)
                    .map_err(move |err| Error::new(err).path(dir))
            })
            .await
            .map_err(Error::from)
            .and_then(|res| res)
            .map_err(|err| err.context("failed to create", "directory"))?
            .into();

        Ok(tempdir)
    }

    async fn asyncify<T, F>(&self, task: F) -> Result<T>
    where
        F: FnOnce(tempfile::Builder<'_, '_>) -> T + Send + 'static,
        T: Send + Sync + 'static,
    {
        let builder = self.clone();
        let result = tokio::task::spawn_blocking(move || task(builder.to_builder())).await?;
        Ok(result)
    }

    fn to_builder(&self) -> tempfile::Builder<'_, '_> {
        let mut builder = tempfile::Builder::new();

        if let Some(prefix) = &self.prefix {
            builder.prefix(prefix);
        }

        if let Some(suffix) = &self.suffix {
            builder.suffix(suffix);
        }

        builder
    }
}

pub type Result<T, E = Error> = result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub struct Error {
    source: io::Error,
    message: &'static str,
    resource: &'static str,
    path: Option<PathBuf>,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        format_error(f, self, self.path.as_deref())
    }
}

impl Error {
    fn new(source: impl Into<io::Error>) -> Self {
        Self {
            source: source.into(),
            message: "",
            resource: "",
            path: None,
        }
    }

    fn context(self, message: &'static str, resource: &'static str) -> Self {
        self.message(message).resource(resource)
    }

    fn message(mut self, message: &'static str) -> Self {
        self.message = message;
        self
    }

    fn resource(mut self, resource: &'static str) -> Self {
        self.resource = resource;
        self
    }

    fn path(mut self, path: impl Into<PathBuf>) -> Self {
        self.path = Some(path.into());
        self
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Self::new(err)
    }
}

impl From<tokio::task::JoinError> for Error {
    fn from(err: tokio::task::JoinError) -> Self {
        Self::new(err)
    }
}

#[derive(Debug, thiserror::Error)]
pub struct PersistError {
    #[source]
    pub error: Error,
    pub file: Option<NamedTempFile>,
}

impl fmt::Display for PersistError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        format_error(
            f,
            &self.error,
            self.file
                .as_ref()
                .map(NamedTempFile::path)
                .or(self.error.path.as_deref()),
        )
    }
}

impl PersistError {
    fn new(error: impl Into<Error>, file: Option<NamedTempFile>) -> Self {
        Self {
            error: error.into().message("failed to persist"),
            file,
        }
    }

    fn resource(mut self, resource: &'static str) -> Self {
        self.error.resource = resource;
        self
    }
}

impl<T> From<tempfile::PersistError<T>> for PersistError
where
    tempfile::NamedTempFile<T>: Into<NamedTempFile>,
{
    fn from(error: tempfile::PersistError<T>) -> Self {
        let tempfile::PersistError { error, file } = error;
        Self::new(error, Some(file.into()))
    }
}

impl<E> From<E> for PersistError
where
    E: Into<Error>,
{
    fn from(error: E) -> Self {
        Self::new(error, None)
    }
}

#[derive(Debug)]
struct Inner<Res: Resource> {
    resource: Option<Res>,
}

impl<Res: Resource> Inner<Res> {
    pub fn new(resource: Res) -> Self {
        Self {
            resource: Some(resource),
        }
    }

    pub fn as_ref(&self) -> &Res {
        Self::expect_resource(self.resource.as_ref())
    }

    pub fn as_mut(&mut self) -> &mut Res {
        Self::expect_resource(self.resource.as_mut())
    }

    pub fn take(&mut self) -> Res {
        Self::expect_resource(self.resource.take())
    }

    fn expect_resource<T>(resource: Option<T>) -> T {
        resource.expect("resource is not closed")
    }

    pub fn path(&self) -> &Path {
        self.as_ref().path()
    }

    pub async fn asyncify<T, F>(mut self, task: F) -> Result<T>
    where
        Res: Send + Sync + 'static,
        F: FnOnce(Res) -> T + Send + 'static,
        T: Send + Sync + 'static,
    {
        let resource = self.take();
        let result = tokio::task::spawn_blocking(move || task(resource)).await?;
        Ok(result)
    }

    pub async fn try_close(self) -> Result<()> {
        let path = self.path().to_path_buf();

        self.asyncify(Resource::try_close)
            .await
            .map_err(Error::from)
            .and_then(|res| res.map_err(Error::from))
            .map_err(move |err| err.context("failed to close", Res::TYPE).path(path))?;

        Ok(())
    }

    pub async fn close(self) {
        if let Err(err) = self.try_close().await {
            log::error!("{err}");
        }
    }

    pub fn into_inner(mut self) -> Res {
        self.take()
    }
}

impl<Res: Resource> Drop for Inner<Res> {
    fn drop(&mut self) {
        if let Some(resource) = self.resource.take() {
            let handle = tokio::task::spawn_blocking(move || {
                let path = resource.path().to_path_buf();

                if let Err(err) = resource.try_close() {
                    log::error!(
                        "Failed to close temporary {} {}: {err}",
                        Res::TYPE,
                        path.display(),
                    );
                }
            });

            drop(handle);
        }
    }
}

trait Resource: Sized + Send + Sync + 'static {
    const TYPE: &'static str;

    fn path(&self) -> &Path;
    fn try_close(self) -> io::Result<()>;
}

impl<T: Send + Sync + 'static> Resource for tempfile::NamedTempFile<T> {
    const TYPE: &'static str = "file";

    fn path(&self) -> &Path {
        self.path()
    }

    fn try_close(self) -> io::Result<()> {
        self.close()
    }
}

impl Resource for tempfile::TempDir {
    const TYPE: &'static str = "directory";

    fn path(&self) -> &Path {
        self.path()
    }

    fn try_close(self) -> io::Result<()> {
        self.close()
    }
}

fn format_error(f: &mut fmt::Formatter<'_>, error: &Error, path: Option<&Path>) -> fmt::Result {
    let source = &error.source;
    let message = error.message;
    let resource = error.resource;

    let path = path.map(Path::display);

    let path_sep = if path.is_some() { " " } else { "" };
    let path: &dyn fmt::Display = path.as_ref().map_or(&"", |path| path);

    write!(
        f,
        "{message} temporary {resource}{path_sep}{path}: {source}"
    )
}
