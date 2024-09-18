//! Extensions for the `tar` crate.

use std::io::{Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tap::Tap;
use tokio::sync::Mutex;
use tokio::task::JoinError;

/// A wrapper around [`tar::Builder`] that:
/// 1. Usable in async contexts.
/// 2. Provides the [`BuilderExt::descend`] method.
#[derive(Clone)]
pub struct BuilderExt {
    tar: Arc<Mutex<BuilderBox>>,
    path: PathBuf,
}

/// A wrapper around [`tar::Builder<WriteBox>`] that disables [`WriteBox`] when
/// it is dropped.
///
/// Disabling the [`WriteBox`] is a workaround for the inconvenient
/// [`tar::Builder`] behavior: dropping a [`tar::Builder`] might cause a final
/// write of archive footer.
/// This behavior is problematic for [`Write`] implementations that could panic
/// when used in an async context, such as [`SyncIoBridge`].
///
/// [`SyncIoBridge`]: https://docs.rs/tokio-util/0.7.12/tokio_util/io/struct.SyncIoBridge.html#method.new
struct BuilderBox {
    tar: Option<tar::Builder<WriteBox>>,
    enabled: Arc<AtomicBool>,
}

/// A wrapper around [`Write`] that could be disabled by [`BuilderBox`].
struct WriteBox {
    inner: Box<dyn Send + WriteSeek>,
    enabled: Arc<AtomicBool>,
}

/// TODO: This trait will be replaced with just [`Write`] once we get rid of
/// nested tar archives.
trait WriteSeek: Write + Seek {}
impl<T: Write + Seek> WriteSeek for T {}

impl BuilderBox {
    fn tar(&mut self) -> &mut tar::Builder<WriteBox> {
        self.tar.as_mut().unwrap()
    }
}

impl Drop for BuilderBox {
    fn drop(&mut self) {
        self.enabled.store(false, Ordering::Release);
    }
}

impl Write for WriteBox {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if !self.enabled.load(Ordering::Acquire) {
            // This error shouldn't be observable. It might appear only in
            // `tar::Builder::drop`, and will be ignored there.
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Using WriteBox after it is disabled",
            ));
        }
        self.inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // This method is never called by `tar::Builder`.
        self.inner.flush()
    }
}

impl Seek for WriteBox {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(pos)
    }
}

impl BuilderExt {
    #[inline]
    pub fn new(output: impl Send + Write + Seek + 'static) -> Self {
        Self::from_box(Box::new(output))
    }

    fn from_box(output: Box<dyn Send + WriteSeek>) -> Self {
        let enabled = Arc::new(AtomicBool::new(true));
        Self {
            tar: Arc::new(Mutex::new(BuilderBox {
                tar: Some(
                    tar::Builder::new(WriteBox {
                        inner: output,
                        enabled: Arc::clone(&enabled),
                    })
                    .tap_mut(|tar| tar.sparse(false)),
                ),
                enabled,
            })),
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
        let writer = tar.tar().append_writer(&mut header, dst)?;
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
        let writer = tar.tar().append_writer(&mut header, dst)?;
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
        self.tar
            .blocking_lock()
            .tar()
            .append_path_with_name(src, dst)
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
        self.tar.blocking_lock().tar().append_dir_all(dst, src)
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
        let mut bb: BuilderBox = Arc::try_unwrap(self.tar)
            .map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "finish called with multiple references to the tar builder",
                )
            })?
            .into_inner();

        // Extract the builder out of bb.
        let tar: tar::Builder<WriteBox> = bb.tar.take().unwrap();

        // Finish and flush before BuilderBox is dropped.
        let mut wb: WriteBox = tar.into_inner()?; // calls finish()
        wb.inner.flush()?;

        Ok(())
    }

    /// Finish writing the tar archive.
    pub async fn finish(self) -> std::io::Result<()> {
        tokio::task::spawn_blocking(move || self.blocking_finish()).await?
    }

    async fn run_async<T, E>(
        &self,
        f: impl FnOnce(&mut tar::Builder<WriteBox>) -> Result<T, E> + Send + 'static,
    ) -> Result<T, E>
    where
        T: Send + 'static,
        E: Send + 'static + From<JoinError>,
    {
        let tar = Arc::clone(&self.tar);
        tokio::task::spawn_blocking(move || f(tar.blocking_lock().tar())).await?
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

#[cfg(test)]
mod tests {
    use super::*;

    struct DummyBridgeWriter(bool, Arc<Mutex<Vec<u8>>>);

    impl Write for DummyBridgeWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if self.0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Forced error in write",
                ));
            }
            self.1.blocking_lock().extend_from_slice(buf); // panics in async
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            if self.0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Forced error in write",
                ));
            }
            let _ = self.1.blocking_lock(); // panics in async
            Ok(())
        }
    }

    impl Seek for DummyBridgeWriter {
        fn seek(&mut self, _: std::io::SeekFrom) -> std::io::Result<u64> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn test_finish_ok() {
        let data = Arc::new(Mutex::new(Vec::new()));
        let tar = BuilderExt::new(DummyBridgeWriter(false, Arc::clone(&data)));
        assert!(tar.finish().await.is_ok());
        assert_eq!(data.lock().await.len(), 1024);
    }

    #[tokio::test]
    async fn test_finish_fail() {
        let data = Arc::new(Mutex::new(Vec::new()));
        let tar = BuilderExt::new(DummyBridgeWriter(true, Arc::clone(&data)));
        assert!(tar.finish().await.is_err());
        assert_eq!(data.lock().await.len(), 0);
    }

    #[tokio::test]
    async fn test_drop_fail() {
        let data = Arc::new(Mutex::new(Vec::new()));
        let tar = BuilderExt::new(DummyBridgeWriter(false, Arc::clone(&data)));
        drop(tar);
        assert_eq!(data.lock().await.len(), 0);
    }

    #[tokio::test]
    async fn test_drop_ok() {
        let data = Arc::new(Mutex::new(Vec::new()));
        let tar = BuilderExt::new(DummyBridgeWriter(false, Arc::clone(&data)));
        drop(tar);
        assert_eq!(data.lock().await.len(), 0);
    }
}
