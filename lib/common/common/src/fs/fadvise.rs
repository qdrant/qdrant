use std::io::{self, IoSliceMut, Read, Seek};
use std::ops::Deref;
use std::path::Path;

use fs_err::File;
#[cfg(posix_fadvise_supported)]
use nix::fcntl::{PosixFadviseAdvice, posix_fadvise};

#[cfg(posix_fadvise_supported)]
fn fadvise(f: &impl std::os::unix::io::AsFd, advise: PosixFadviseAdvice) -> io::Result<()> {
    fadvise_with_len(f, advise, 0)
}

/// Call `posix_fadvise` with an explicit `len`.
///
/// `len = 0` means "to end of file" for most advice types, but the meaning is
/// filesystem-specific for `POSIX_FADV_NOREUSE` on F2FS, where a zero-length
/// request removes a previously-registered NOREUSE range and returns `ENOENT`
/// when no range exists. Callers using `POSIX_FADV_NOREUSE` must pass the
/// file's actual length instead.
#[cfg(posix_fadvise_supported)]
fn fadvise_with_len(
    f: &impl std::os::unix::io::AsFd,
    advise: PosixFadviseAdvice,
    len: nix::libc::off_t,
) -> io::Result<()> {
    Ok(posix_fadvise(f, 0, len, advise)?)
}

/// For given file path, clear disk cache with `posix_fadvise`
///
/// Does nothing if:
/// - the file does not exist
/// - `posix_fadvise` is not supported on this platform
pub fn clear_disk_cache(file_path: &Path) -> io::Result<()> {
    #[cfg(posix_fadvise_supported)]
    match File::open(file_path.to_path_buf()) {
        Ok(file) => fadvise(&file, PosixFadviseAdvice::POSIX_FADV_DONTNEED),
        // If file is not found, no need to clear cache
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }

    #[cfg(not(posix_fadvise_supported))]
    {
        let _ = file_path;
        Ok(())
    }
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
        let file = File::open(path.as_ref())?;
        #[cfg(posix_fadvise_supported)]
        {
            fadvise(&file, PosixFadviseAdvice::POSIX_FADV_SEQUENTIAL)?;
            // `POSIX_FADV_NOREUSE` is an advisory hint and must never abort
            // loading on otherwise-valid data. `len = 0` has filesystem-
            // specific semantics: on F2FS (used by Android) the kernel treats
            // a zero-length request as "remove a previously-registered NOREUSE
            // range" and returns `ENOENT` when no range exists. Pass the
            // file's actual length so the advice covers the full file,
            // skip the call for empty files (no range to advise on), and
            // swallow any error so an unknown FS quirk cannot block
            // shard loading.
            let metadata = file.metadata()?;
            if let Ok(len) = i64::try_from(metadata.len())
                && len > 0
            {
                let _ = fadvise_with_len(&file, PosixFadviseAdvice::POSIX_FADV_NOREUSE, len);
            }
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
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.file.as_ref().unwrap().read(buf)
    }

    fn read_vectored(&mut self, bufs: &mut [IoSliceMut<'_>]) -> io::Result<usize> {
        self.file.as_ref().unwrap().read_vectored(bufs)
    }

    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        self.file.as_ref().unwrap().read_to_end(buf)
    }

    fn read_to_string(&mut self, buf: &mut String) -> io::Result<usize> {
        self.file.as_ref().unwrap().read_to_string(buf)
    }
}

impl Seek for OneshotFile {
    fn seek(&mut self, pos: std::io::SeekFrom) -> io::Result<u64> {
        self.file.as_ref().unwrap().seek(pos)
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

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::Builder;

    use super::*;

    /// Smoke test: `OneshotFile::open` succeeds on a non-empty file. The
    /// F2FS regression (where the `POSIX_FADV_NOREUSE` call returned `ENOENT`)
    /// only reproduces on F2FS-backed filesystems, but the fix uses the
    /// file's actual length on every platform, so we can at least confirm
    /// the open path still works on a regular Linux fs.
    #[test]
    fn oneshot_file_open_succeeds_on_non_empty_file() {
        let dir = Builder::new().prefix("fadvise").tempdir().unwrap();
        let path = dir.path().join("data.bin");
        {
            let mut f = File::create(&path).unwrap();
            f.write_all(b"hello world").unwrap();
            f.sync_all().unwrap();
        }
        let mut file = OneshotFile::open(&path).expect("open should succeed");
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).unwrap();
        assert_eq!(buf, b"hello world");
    }

    /// An empty file is handled by skipping the `POSIX_FADV_NOREUSE` call
    /// entirely (no range to advise on). On F2FS, calling it with `len = 0`
    /// would return `ENOENT`; skipping avoids that and any other FS-specific
    /// zero-length semantics.
    #[test]
    fn oneshot_file_open_succeeds_on_empty_file() {
        let dir = Builder::new().prefix("fadvise").tempdir().unwrap();
        let path = dir.path().join("empty.bin");
        File::create(&path).unwrap();
        let mut file = OneshotFile::open(&path).expect("open should succeed on empty file");
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).unwrap();
        assert!(buf.is_empty());
    }

    /// A larger file (multi-page) exercises the `len > 0` branch that triggers
    /// the actual `posix_fadvise(fd, 0, file_len, NOREUSE)` call.
    #[test]
    fn oneshot_file_open_succeeds_on_multipage_file() {
        let dir = Builder::new().prefix("fadvise").tempdir().unwrap();
        let path = dir.path().join("big.bin");
        let payload = vec![0u8; 256 * 1024];
        std::fs::write(&path, &payload).unwrap();
        let mut file = OneshotFile::open(&path).expect("open should succeed on multipage file");
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).unwrap();
        assert_eq!(buf.len(), payload.len());
    }

    /// `OneshotFile::open` must not consume the file twice: opening the same
    /// path twice produces two independent handles that each read the full
    /// payload.
    #[test]
    fn oneshot_file_open_is_repeatable() {
        let dir = Builder::new().prefix("fadvise").tempdir().unwrap();
        let path = dir.path().join("repeat.bin");
        std::fs::write(&path, b"abcdefghij").unwrap();
        for _ in 0..3 {
            let mut file = OneshotFile::open(&path).expect("open should succeed");
            let mut buf = Vec::new();
            file.read_to_end(&mut buf).unwrap();
            assert_eq!(buf, b"abcdefghij");
        }
    }
}
