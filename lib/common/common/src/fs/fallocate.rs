use std::cmp::{max, min};
use std::io;
use std::ops::Range;
use std::path::Path;
use std::sync::Once;

use fs_err::File;
use roaring::RoaringBitmap;

use crate::mmap::advice::page_size;

/// Max preallocation lookahead for sequential writers.
const MAX_LOOKAHEAD_BYTES: usize = 1024 * 1024;

/// Preallocate disk blocks for the written pages of a sparse file.
///
/// Usage: before writing via mmap, call [FallocatedPages::ensure] first, so a
/// full disk is `ENOSPC` rather than `SIGBUS`.
#[derive(Debug, Default)]
pub struct FallocatedPages {
    /// Reserved bytes at the start of the file.
    ///
    /// Sentinel value [usize::MAX] means not supported; not [Option] to save a
    /// branch in hot path and keep struct size to 32 bytes.
    prefix_bytes: usize,
    /// Reserved pages past [Self::prefix_bytes].
    pages: RoaringBitmap,
}

impl FallocatedPages {
    /// Make sure `range` of the file is backed by allocated blocks.
    ///
    /// If there is no free disk space, it will most likely return `ENOSPC` aka
    /// [io::ErrorKind::StorageFull].
    #[inline]
    pub fn ensure(&mut self, path: &Path, file_size: usize, range: Range<usize>) -> io::Result<()> {
        if range.end <= self.prefix_bytes {
            return Ok(());
        }
        let page_size = page_size().unwrap_or(4096);
        let page_range = page_range(range.clone(), page_size);
        if self.pages.contains_range(page_range) {
            return Ok(());
        }
        self.reserve(path, file_size, range, page_size)
    }

    #[cold]
    fn reserve(
        &mut self,
        path: &Path,
        file_size: usize,
        range: Range<usize>,
        page_size: usize,
    ) -> io::Result<()> {
        // For sequential writers, allocate up to MAX_LOOKAHEAD_BYTES.
        let is_sequential = range.start <= self.prefix_bytes;
        let lookahead_bytes = if is_sequential {
            min(self.prefix_bytes / 4, MAX_LOOKAHEAD_BYTES) // +25%
        } else {
            0
        };

        let start_bytes = max(range.start, self.prefix_bytes) / page_size * page_size;
        let end_bytes = (range.end + lookahead_bytes).div_ceil(page_size) * page_size;
        let end_bytes = min(end_bytes, file_size);
        let file = File::options().write(true).open(path)?;
        match fallocate(&file, start_bytes as u64..end_bytes as u64) {
            Ok(()) => (),
            Err(err) if err.kind() == io::ErrorKind::Unsupported => {
                self.prefix_bytes = usize::MAX;
                warn_unsupported(path, &err);
                return Ok(());
            }
            Err(err) => return Err(err),
        }

        if is_sequential {
            self.prefix_bytes = end_bytes;
        } else {
            let page_range = page_range(start_bytes..end_bytes, page_size);
            self.pages.insert_range(page_range);
        }
        Ok(())
    }
}

/// Byte range -> page idx range.
fn page_range(range_bytes: Range<usize>, page_size: usize) -> Range<u32> {
    let index = |pages| u32::try_from(pages).expect("file larger than 16 TiB");
    index(range_bytes.start / page_size)..index(range_bytes.end.div_ceil(page_size))
}

#[cold]
fn warn_unsupported(path: &Path, err: &io::Error) {
    static WARNED: Once = Once::new();
    WARNED.call_once(|| {
        log::warn!(
            "Filesystem of {} does not support reserving file blocks; \
             running out of disk space while writing may crash with SIGBUS: {err}",
            path.display(),
        )
    });
}

fn fallocate(file: &File, Range { start, end }: Range<u64>) -> io::Result<()> {
    cfg_select! {
        target_os = "linux" => {
            // Not `posix_fallocate`: glibc writes zeroes on unsupported
            // filesystems.
            let flags = nix::fcntl::FallocateFlags::empty();
            nix::fcntl::fallocate(file, flags, start as i64, (end - start) as i64)?;
            Ok(())
        }
        target_os = "android" => {
            // Bionic's `posix_fallocate` is a thin `fallocate` wrapper, no fallback.
            nix::fcntl::posix_fallocate(file, start as i64, (end - start) as i64)?;
            Ok(())
        }
        _ => {
            // Windows: `set_len` already allocates non-sparse NTFS files.
            // Apple: `fs4::FileExt::allocate` truncates the file.
            _ = (file, start, end);
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use fs_err as fs;

    use super::*;

    #[cfg(target_os = "linux")]
    #[test]
    fn test_ensure_reserves_blocks() {
        use std::os::unix::fs::MetadataExt as _;

        const LEN: usize = 16 * 1024 * 1024;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sparse.dat");
        let file = File::create(&path).unwrap();
        file.set_len(LEN as u64).unwrap();
        let allocated = || fs::metadata(&path).unwrap().blocks() as usize * 512;
        assert_eq!(allocated(), 0);

        let page_size = page_size().unwrap_or(4096);
        let mut fallocated = FallocatedPages::default();

        // No look-ahead on the first write.
        fallocated.ensure(&path, LEN, 0..1024 * page_size).unwrap();
        assert_eq!(fallocated.prefix_bytes, 1024 * page_size);
        assert_eq!(allocated(), 1024 * page_size);

        // A random write reserves only its own pages.
        fallocated.ensure(&path, LEN, LEN / 2..LEN / 2 + 1).unwrap();
        assert_eq!(fallocated.prefix_bytes, 1024 * page_size);
        assert_eq!(fallocated.pages.len(), 1);
        assert_eq!(allocated(), 1025 * page_size);

        // A sequential write earns a look-ahead of a quarter of the prefix.
        fallocated
            .ensure(&path, LEN, 1024 * page_size..1024 * page_size + 1)
            .unwrap();
        assert_eq!(fallocated.prefix_bytes, (1025 + 256) * page_size);
        assert_eq!(allocated(), (1025 + 256 + 1) * page_size);

        // Look-ahead is clamped to the file length, never extending it.
        fallocated.ensure(&path, LEN, 0..LEN - page_size).unwrap();
        assert_eq!(fallocated.prefix_bytes, LEN);
        assert_eq!(allocated(), LEN);
        assert_eq!(fs::metadata(&path).unwrap().len() as usize, LEN);
    }

    #[test]
    fn test_ensure_keeps_contents() {
        use std::io::{Seek as _, SeekFrom, Write as _};

        // Not page-aligned: the look-ahead must not extend the file.
        const LEN: usize = 16 * 1024 * 1024 + 100;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sparse.dat");
        let mut file = File::create(&path).unwrap();
        file.set_len(LEN as u64).unwrap();
        file.seek(SeekFrom::End(-1)).unwrap();
        file.write_all(b"!").unwrap();

        let mut fallocated = FallocatedPages::default();
        for range in [0..1, 0..LEN / 2, LEN - 1..LEN, 0..LEN] {
            fallocated.ensure(&path, LEN, range).unwrap();
            let data = fs::read(&path).unwrap();
            assert_eq!(data.len(), LEN);
            assert_eq!(data.last(), Some(&b'!'));
        }
    }
}
