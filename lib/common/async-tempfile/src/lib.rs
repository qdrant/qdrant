use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::{io, result};

#[derive(Debug)]
pub struct NamedTempFile {
    inner: Inner<tempfile::NamedTempFile<tokio::fs::File>>,
}

impl NamedTempFile {
    pub async fn new_in(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();

        let tempfile = tokio::task::spawn_blocking(move || tempfile::NamedTempFile::new_in(dir))
            .await??
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

    pub async fn keep(self) -> Result<(tokio::fs::File, PathBuf)> {
        let result = self.inner.asyncify(tempfile::NamedTempFile::keep).await??;
        Ok(result)
    }

    pub async fn close(self) -> Result<()> {
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

        let tempdir = tokio::task::spawn_blocking(move || tempfile::TempDir::new_in(dir))
            .await??
            .into();

        Ok(tempdir)
    }

    pub fn path(&self) -> &Path {
        self.inner.path()
    }

    pub fn into_path(mut self) -> PathBuf {
        self.inner.take().into_path()
    }

    pub async fn close(self) -> Result<()> {
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
    prefix: Option<OsString>,
    suffix: Option<OsString>,
}

impl Builder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn prefix(&mut self, prefix: impl Into<OsString>) -> &mut Self {
        self.prefix = Some(prefix.into());
        self
    }

    pub fn suffix(&mut self, suffix: impl Into<OsString>) -> &mut Self {
        self.suffix = Some(suffix.into());
        self
    }

    pub async fn tempfile_in(&self, dir: impl Into<PathBuf>) -> Result<NamedTempFile> {
        let dir = dir.into();

        let tempfile = self
            .asyncify(move |builder| builder.tempfile_in(dir))
            .await??
            .into();

        Ok(tempfile)
    }

    pub async fn tempdir_in(&self, dir: impl Into<PathBuf>) -> Result<TempDir> {
        let dir = dir.into();

        let tempdir = self
            .asyncify(move |builder| builder.tempdir_in(dir))
            .await??
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
#[error("{0}")]
pub enum Error {
    Io(#[from] io::Error),
    Join(#[from] tokio::task::JoinError),
    Persist(#[from] PersistError),
}

impl From<tempfile::PersistError<tokio::fs::File>> for Error {
    fn from(err: tempfile::PersistError<tokio::fs::File>) -> Self {
        PersistError::from(err).into()
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{source}")]
pub struct PersistError {
    pub source: io::Error,
    pub file: NamedTempFile,
}

impl From<tempfile::PersistError<tokio::fs::File>> for PersistError {
    fn from(err: tempfile::PersistError<tokio::fs::File>) -> Self {
        Self {
            source: err.error,
            file: err.file.into(),
        }
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

    pub async fn close(self) -> Result<()> {
        self.asyncify(Resource::close).await??;
        Ok(())
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

                if let Err(err) = resource.close() {
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
    fn close(self) -> io::Result<()>;
}

impl<T: Send + Sync + 'static> Resource for tempfile::NamedTempFile<T> {
    const TYPE: &'static str = "file";

    fn path(&self) -> &Path {
        self.path()
    }

    fn close(self) -> io::Result<()> {
        self.close()
    }
}

impl Resource for tempfile::TempDir {
    const TYPE: &'static str = "directory";

    fn path(&self) -> &Path {
        self.path()
    }

    fn close(self) -> io::Result<()> {
        self.close()
    }
}
