use std::fs::File;
use std::io::{self, IoSliceMut, Read, Seek};
use std::ops::Deref;
use std::path::Path;

use delegate::delegate;
#[cfg(posix_fadvise_supported)]
use nix::fcntl::{PosixFadviseAdvice, posix_fadvise};

#[cfg(posix_fadvise_supported)]
fn fadvise(f: &impl std::os::unix::io::AsRawFd, advise: PosixFadviseAdvice) -> io::Result<()> {
    Ok(posix_fadvise(f.as_raw_fd(), 0, 0, advise)?)
}

/// For given file path, clear disk cache with `posix_fadvise`
///
/// If `posix_fadvise` is not supported, this function does nothing.
pub fn clear_disk_cache(file_path: &Path) -> io::Result<()> {
    #[cfg(posix_fadvise_supported)]
    fadvise(
        &File::open(file_path)?,
        PosixFadviseAdvice::POSIX_FADV_DONTNEED,
    )?;
    _ = file_path;
    Ok(())
}

/// A wrapper around [`File`] intended for one-time sequential read.
///
/// On supported platforms, the file contents is evicted from the OS file cache
/// after the file is closed.
pub struct OneshotFile {
    /// Is `None` only when `drop_cache` is called, to avoid double call on drop.
    file: Option<File>,
}

impl OneshotFile {
    /// Similar to [`File::open`].
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = File::open(path)?;
        #[cfg(posix_fadvise_supported)]
        {
            fadvise(&file, PosixFadviseAdvice::POSIX_FADV_SEQUENTIAL)?;
            fadvise(&file, PosixFadviseAdvice::POSIX_FADV_NOREUSE)?;
        }
        Ok(Self { file: Some(file) })
    }

    /// Consume this [`OneshotFile`] and clear the cache.
    ///
    /// If not called, the cache still will be implicitly cleared on drop.
    /// The only difference is that this method might return an error.
    pub fn drop_cache(mut self) -> io::Result<()> {
        let file = self.file.take().unwrap();
        #[cfg(posix_fadvise_supported)]
        fadvise(&file, PosixFadviseAdvice::POSIX_FADV_DONTNEED)?;
        let _ = file;
        Ok(())
    }
}

impl Deref for OneshotFile {
    type Target = File;

    fn deref(&self) -> &Self::Target {
        self.file.as_ref().unwrap()
    }
}

impl Read for OneshotFile {
    delegate! {
        to self.file.as_ref().unwrap() {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize>;
            fn read_vectored(&mut self, bufs: &mut [IoSliceMut<'_>]) -> io::Result<usize>;
            fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize>;
            fn read_to_string(&mut self, buf: &mut String) -> io::Result<usize>;
        }
    }
}

impl Seek for OneshotFile {
    delegate! {
        to self.file.as_ref().unwrap() {
            fn seek(&mut self, pos: std::io::SeekFrom) -> io::Result<u64>;
        }
    }
}

impl Drop for OneshotFile {
    fn drop(&mut self) {
        if let Some(file) = self.file.take() {
            #[cfg(posix_fadvise_supported)]
            let _ = fadvise(&file, PosixFadviseAdvice::POSIX_FADV_DONTNEED);
            let _ = file;
        }
    }
}
