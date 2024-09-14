//! Extensions for the `tar` crate.

use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::Mutex;
use tokio::task::JoinError;

/// A wrapper around [`tar::Builder`] that:
/// 1. Usable in async contexts.
/// 2. Provides the [`BuilderExt::descend`] method.
#[derive(Clone)]
pub struct BuilderExt {
    tar: Arc<Mutex<tar::Builder<File>>>,
    path: PathBuf,
}

impl BuilderExt {
    pub fn new(tar_file: File) -> Self {
        let mut tar = tar::Builder::new(tar_file);
        tar.sparse(false);
        Self {
            tar: Arc::new(Mutex::new(tar)),
            path: PathBuf::new(),
        }
    }

    /// Create a new [`BuilderExt`] that writes to a subdirectory of the current
    /// path. I.e. the following two lines are equivalent:
    /// ```rust,ignore
    /// builder.append_data(data, Path::new("foo/bar/baz")).await?;
    /// builder.descend(Path::new("foo/bar"))?.append_data(data, Path::new("baz")).await?;
    /// ```
    pub fn descend(&self, subdir: &Path) -> std::io::Result<Self> {
        Ok(BuilderExt {
            tar: Arc::clone(&self.tar),
            path: join_relative(&self.path, subdir)?,
        })
    }

    /// Write an entry to the tar archive. Takes a closure that takes an
    /// `impl Write` and writes the entry contents into it.
    ///
    /// # Panics
    ///
    /// This function panics if called within an asynchronous execution context.
    /// Use [`BuilderExt::write_fn`] instead.
    pub fn blocking_write_fn<E>(
        &self,
        dst: &Path,
        f: impl FnOnce(tar::EntryWriter) -> Result<(), E>,
    ) -> Result<(), E>
    where
        E: std::error::Error + From<std::io::Error>,
    {
        let dst = join_relative(&self.path, dst)?;
        let mut header = tar::Header::new_gnu();
        header.set_mode(0o644);
        let mut tar = self.tar.blocking_lock();
        let writer = tar.append_writer(&mut header, dst)?;
        f(writer)
    }

    /// Write an entry to the tar archive. Takes a closure that takes an
    /// `impl Write` and writes the entry contents into it.
    pub async fn write_fn<E>(
        &self,
        dst: &Path,
        f: impl FnOnce(tar::EntryWriter) -> Result<(), E>,
    ) -> Result<(), E>
    where
        E: std::error::Error + From<std::io::Error>,
    {
        let dst = join_relative(&self.path, dst)?;
        let mut header = tar::Header::new_gnu();
        header.set_mode(0o644);
        let mut tar = self.tar.lock().await;
        let writer = tar.append_writer(&mut header, dst)?;
        f(writer)
    }

    /// Append a file to the tar archive.
    ///
    /// # Panics
    ///
    /// This function panics if called within an asynchronous execution context.
    /// Use [`BuilderExt::append_file`] instead.
    pub fn blocking_append_file(&self, src: &Path, dst: &Path) -> std::io::Result<()> {
        let dst = join_relative(&self.path, dst)?;
        self.tar.blocking_lock().append_path_with_name(src, dst)
    }

    /// Append a file to the tar archive.
    pub async fn append_file(&self, src: &Path, dst: &Path) -> std::io::Result<()> {
        let src = src.to_path_buf();
        let dst = join_relative(&self.path, dst)?;
        self.run_async(move |tar| tar.append_path_with_name(src, dst))
            .await
    }

    /// Append a directory to the tar archive.
    ///
    /// # Panics
    ///
    /// This function panics if called within an asynchronous execution context.
    pub fn blocking_append_dir_all(&self, src: &Path, dst: &Path) -> std::io::Result<()> {
        let dst = join_relative(&self.path, dst)?;
        self.tar.blocking_lock().append_dir_all(dst, src)
    }

    /// Append a new entry to the tar archive with the given file contents.
    ///
    /// # Panics
    ///
    /// This function panics if called within an asynchronous execution context.
    pub async fn append_data(&self, src: Vec<u8>, dst: &Path) -> std::io::Result<()> {
        let dst = join_relative(&self.path, dst)?;
        let mut header = tar::Header::new_gnu();
        header.set_mode(0o644);
        header.set_size(src.len() as u64);
        self.run_async(move |tar| tar.append_data(&mut header, dst, src.as_slice()))
            .await
    }

    /// Finish writing the tar archive. For async counterpart, see
    /// [`BuilderExt::finish`].
    pub fn blocking_finish(self) -> std::io::Result<()> {
        Arc::try_unwrap(self.tar)
            .map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "finish called with multiple references to the tar builder",
                )
            })?
            .into_inner()
            .into_inner()?
            .flush()
    }

    /// Finish writing the tar archive.
    pub async fn finish(self) -> std::io::Result<()> {
        tokio::task::spawn_blocking(move || self.blocking_finish()).await?
    }

    async fn run_async<T, E>(
        &self,
        f: impl FnOnce(&mut tar::Builder<File>) -> Result<T, E> + Send + 'static,
    ) -> Result<T, E>
    where
        T: Send + 'static,
        E: Send + 'static + From<JoinError>,
    {
        let tar = Arc::clone(&self.tar);
        tokio::task::spawn_blocking(move || f(&mut tar.blocking_lock())).await?
    }
}

fn join_relative(base: &Path, rel_path: &Path) -> std::io::Result<PathBuf> {
    if rel_path.is_absolute() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("path must be relative, but got {rel_path:?}"),
        ));
    }

    Ok(base.join(rel_path))
}
