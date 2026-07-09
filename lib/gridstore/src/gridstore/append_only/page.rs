use std::borrow::Cow;
use std::path::{Path, PathBuf};

use common::generic_consts::AccessPattern;
use common::mmap::{Advice, AdviceSetting};
use common::universal_io::{
    CachedReadFs, IsNotFound, OpenOptions, Populate, ReadRange, UniversalRead, UniversalReadFs,
    UniversalWrite, UniversalWriteFileOps,
};

use crate::Result;
use crate::error::GridstoreError;
use crate::gridstore::Flusher;
use crate::tracker::{BlockOffset, ValuePointer};

/// File name of the append-only page file
///
/// Deliberately different from the dynamic page file names (`page_{id}.dat`), so that one mode
/// never attempts to load the incompatible file format of the other. Keeps a page number for
/// forward compatibility, even though the append-only mode always uses a single page for now.
const PAGE_FILE_NAME: &str = "append_only_page_0.dat";

/// Append-only page of value data for the append-only storage mode.
///
/// A single file holding the raw (compressed) value bytes. Every value starts at a block aligned
/// offset. The file starts empty and only ever grows by appending; existing bytes are never
/// rewritten. The file length always matches the end of the last appended value, there is no
/// preallocation and no trailing padding.
///
/// The file is read and written through the universal IO backend `S`.
#[derive(Debug)]
pub(super) struct AppendOnlyPage<S> {
    /// Path to the page file
    path: PathBuf,
    /// Open handle to the page file
    file: S,
    /// Length of the page file in bytes, tracked in memory
    len: u64,
}

impl<S: UniversalRead> AppendOnlyPage<S> {
    pub(super) fn page_file_name(dir: &Path) -> PathBuf {
        dir.join(PAGE_FILE_NAME)
    }

    /// Universal IO open options for the page file.
    fn open_options(writeable: bool) -> OpenOptions {
        OpenOptions {
            writeable,
            need_sequential: true,
            // The append-only mode never populates, see [`super::AppendOnlyGridstore::populate`]
            populate: Populate::No,
            advice: AdviceSetting::Advice(Advice::Random),
        }
    }

    /// Schedule a prefetch of the page file, so a subsequent open is served from the prefetch
    /// pool.
    pub(super) fn preopen<Fs: CachedReadFs<File = S>>(fs: &Fs, dir: &Path) -> Result<()> {
        let path = Self::page_file_name(dir);
        fs.schedule_prefetch(&path, Some(Self::open_options(false)), None)?;
        Ok(())
    }

    /// Open an existing page in the given directory.
    ///
    /// If the file does not exist, return an error.
    pub(super) fn open<Fs: UniversalReadFs<File = S>>(
        fs: &Fs,
        dir: &Path,
        writeable: bool,
    ) -> Result<Self> {
        let path = Self::page_file_name(dir);
        let file = fs
            .open(&path, Self::open_options(writeable), Default::default())
            .map_err(|err| {
                if err.is_not_found() {
                    // If config exists and this file doesn't,
                    // it should be treated as inconsistent storage rather than a missing one
                    GridstoreError::service_error(format!(
                        "Append-only page file does not exist: {}",
                        path.display(),
                    ))
                } else {
                    GridstoreError::from(err)
                }
            })?;
        let len = file.len::<u8>()?;
        Ok(Self { path, file, len })
    }

    pub(super) fn files(&self) -> Vec<PathBuf> {
        vec![self.path.clone()]
    }

    /// Length of the page file in bytes, which is the end of the last appended value.
    pub(super) fn len(&self) -> u64 {
        self.len
    }

    /// Read the raw value bytes at the given pointer.
    pub(super) fn read_value<P: AccessPattern>(
        &self,
        pointer: ValuePointer,
        block_size_bytes: u64,
    ) -> Result<Cow<'_, [u8]>> {
        // The append-only mode stores all values in a single page
        if pointer.page_id != 0 {
            return Err(GridstoreError::PageNotFound {
                page_id: pointer.page_id,
            });
        }

        let range = ReadRange {
            byte_offset: u64::from(pointer.block_offset) * block_size_bytes,
            length: u64::from(pointer.length),
        };
        Ok(self.file.read::<P, u8>(range)?)
    }

    /// Reopen the page file handle and reload its length, making newly appended value data
    /// visible to reads and the reported storage size.
    ///
    /// The reopen is a no-op for backends that read the file directly, those see newly appended
    /// data without it.
    pub(super) fn live_reload(&mut self) -> Result<()> {
        self.file.reopen()?;
        self.len = self.file.len::<u8>()?;
        Ok(())
    }
}

impl<S: UniversalWrite> AppendOnlyPage<S> {
    /// Create a new empty page in the given directory, truncating it if it already exists.
    ///
    /// The directory must exist already.
    pub(super) fn new(fs: &S::Fs, dir: &Path) -> Result<Self> {
        let path = Self::page_file_name(dir);
        fs.create(&path, 0)?;
        let file = fs.open(&path, Self::open_options(true), Default::default())?;
        Ok(Self { path, file, len: 0 })
    }

    /// Append a value at the next block aligned offset, returning the block offset it landed at.
    ///
    /// The write starts exactly at the current end of the file and includes the zero padding up
    /// to the next block boundary, so that it is a pure append.
    pub(super) fn append_value(
        &mut self,
        fs: &S::Fs,
        value: &[u8],
        block_size_bytes: u64,
    ) -> Result<BlockOffset> {
        let start = self.len.next_multiple_of(block_size_bytes);

        // Validate addressability before writing anything, a rejected append must not grow the
        // file
        let block_offset = BlockOffset::try_from(start / block_size_bytes).map_err(|_| {
            GridstoreError::service_error(format!(
                "append-only page file {} exceeds the maximum addressable size",
                self.path.display(),
            ))
        })?;

        if let Err(err) = self.grow_and_write(fs, value, start) {
            // Best effort: drop partially appended bytes, so that the file stays consistent
            // with the tracked length and a retried append never rewrites existing bytes
            let _ = fs.create(&self.path, self.len as usize);
            return Err(err);
        }
        self.len = start + value.len() as u64;

        Ok(block_offset)
    }

    /// Grow the page file so the value fits at `start`, then write the value and the zero
    /// padding before it.
    ///
    /// Universal IO writes cannot grow a file, so the file is extended to its new length first
    /// and the handle is reopened to make the appended range accessible. Backends resize an
    /// existing file in place, preserving its content.
    //
    // TODO(serverless): the append-only mode must grow the file and write the appended bytes in a single
    // syscall, growing the file before writing to it is not allowed. Universal IO has no such
    // append operation yet, replace the separate grow and write steps with it once it exists.
    fn grow_and_write(&mut self, fs: &S::Fs, value: &[u8], start: u64) -> Result<()> {
        let end = start + value.len() as u64;
        fs.create(&self.path, end as usize)?;
        self.file.reopen()?;

        let pad = (start - self.len) as usize;
        if pad == 0 {
            self.file.write(self.len, value)?;
        } else {
            // Prefix the write with the padding, so that it lands at the end of the file
            let mut buf = vec![0; pad + value.len()];
            buf[pad..].copy_from_slice(value);
            self.file.write(self.len, &buf)?;
        }

        Ok(())
    }

    /// Create a closure that syncs all written value data in the page file to disk.
    pub(super) fn flusher(&self) -> Flusher {
        let flusher = self.file.flusher();
        Box::new(move || flusher().map_err(GridstoreError::from))
    }
}

#[cfg(test)]
mod tests {
    use common::universal_io::{MmapFile, MmapFs};
    use fs_err as fs;
    use tempfile::TempDir;

    use super::*;
    use crate::config::DEFAULT_BLOCK_SIZE_BYTES;

    /// An append beyond the maximum addressable block offset is rejected before writing
    /// anything, so a retried put does not grow the page file unboundedly.
    #[test]
    fn test_page_append_rejects_beyond_addressable_range() {
        let dir = TempDir::new().unwrap();
        let mut page = AppendOnlyPage::<MmapFile>::new(&MmapFs, dir.path()).unwrap();

        // Pretend the page already holds the maximum addressable amount of data
        let block_size_bytes = DEFAULT_BLOCK_SIZE_BYTES as u64;
        page.len = (u64::from(u32::MAX) + 1) * block_size_bytes;

        let err = page
            .append_value(&MmapFs, &[1, 2, 3], block_size_bytes)
            .unwrap_err();
        assert!(matches!(err, GridstoreError::ServiceError { .. }));

        // Nothing was written, and the tracked length is unchanged
        assert_eq!(
            fs::metadata(dir.path().join("append_only_page_0.dat"))
                .unwrap()
                .len(),
            0,
        );
        assert_eq!(page.len, (u64::from(u32::MAX) + 1) * block_size_bytes);
    }
}
