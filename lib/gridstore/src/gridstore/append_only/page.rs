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
/// rewritten. The file length always matches the end of the last flushed value, there is no
/// preallocation and no trailing padding.
///
/// Newly appended values are buffered in memory, and written to the file as a single append when
/// flushing. Reads transparently serve buffered values from memory.
///
/// The file is read and written through the universal IO backend `S`.
#[derive(Debug)]
pub(super) struct AppendOnlyPage<S> {
    /// Path to the page file
    path: PathBuf,
    /// Open handle to the page file
    file: S,
    /// Length of the value data persisted in the page file, in bytes
    persisted_len: u64,
    /// Value data that hasn't been written to the file yet
    ///
    /// Byte `i` corresponds to file offset `persisted_len + i`. The zero padding between block
    /// aligned values is materialized in the buffer, so this is byte for byte the data of the
    /// next append.
    pending: Vec<u8>,
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
        let persisted_len = file.len::<u8>()?;
        Ok(Self {
            path,
            file,
            persisted_len,
            pending: Vec::new(),
        })
    }

    pub(super) fn files(&self) -> Vec<PathBuf> {
        vec![self.path.clone()]
    }

    /// Length of the value data in bytes, which is the end of the last appended value.
    ///
    /// This includes buffered values that haven't been flushed to the file yet.
    pub(super) fn len(&self) -> u64 {
        self.persisted_len + self.pending.len() as u64
    }

    /// Read the raw value bytes at the given pointer.
    ///
    /// Values that haven't been flushed yet are served from the in-memory buffer.
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

        let start = u64::from(pointer.block_offset) * block_size_bytes;
        let end = start + u64::from(pointer.length);

        // Persisted values are read from the file
        if end <= self.persisted_len {
            let range = ReadRange {
                byte_offset: start,
                length: u64::from(pointer.length),
            };
            return Ok(self.file.read::<P, u8>(range)?);
        }

        // Buffered values are served from memory. A value never straddles the persisted
        // boundary: values are buffered whole, and flushes only write up to a watermark that
        // was captured between puts.
        debug_assert!(
            start >= self.persisted_len,
            "value must not straddle the persisted boundary",
        );
        let bytes = start
            .checked_sub(self.persisted_len)
            .map(|index| index as usize)
            .and_then(|index| self.pending.get(index..index + pointer.length as usize))
            .ok_or_else(|| {
                GridstoreError::service_error(format!(
                    "value pointer at byte {start} with length {} is out of range",
                    pointer.length,
                ))
            })?;
        Ok(Cow::Borrowed(bytes))
    }

    /// Reopen the page file handle and reload its length, making newly appended value data
    /// visible to reads and the reported storage size.
    ///
    /// The reopen is a no-op for backends that read the file directly, those see newly appended
    /// data without it.
    pub(super) fn live_reload(&mut self) -> Result<()> {
        debug_assert!(
            self.pending.is_empty(),
            "live reload must only be used on read-only instances",
        );
        self.file.reopen()?;
        self.persisted_len = self.file.len::<u8>()?;
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
        Ok(Self {
            path,
            file,
            persisted_len: 0,
            pending: Vec::new(),
        })
    }

    /// Append a value at the next block aligned offset, returning the block offset it landed at.
    ///
    /// The value is buffered in memory until the next flush, together with the zero padding up
    /// to its block boundary, so that the flush appends all buffered values with a single write
    /// that lands exactly at the end of the file.
    pub(super) fn append_value(
        &mut self,
        value: &[u8],
        block_size_bytes: u64,
    ) -> Result<BlockOffset> {
        let start = self.len().next_multiple_of(block_size_bytes);

        // Validate addressability before buffering anything, a rejected append must not grow
        // the page
        let block_offset = BlockOffset::try_from(start / block_size_bytes).map_err(|_| {
            GridstoreError::service_error(format!(
                "append-only page file {} exceeds the maximum addressable size",
                self.path.display(),
            ))
        })?;

        // Materialize the zero padding up to the block boundary in the buffer
        let pad = (start - self.len()) as usize;
        self.pending.resize(self.pending.len() + pad, 0);
        self.pending.extend_from_slice(value);

        Ok(block_offset)
    }

    /// Append buffered values for file offsets up to, but excluding, `target_len` to the file.
    ///
    /// All buffered value data is written with a single write at the end of the file. The
    /// `target_len` is captured through [`len`](Self::len) when a flusher is created, so that
    /// values appended while a flush is in progress stay buffered.
    ///
    /// A stale flush, with a `target_len` at or below what a more recent flush already
    /// persisted, is a no-op: bytes that were appended before must never be written again.
    ///
    /// Universal IO writes cannot grow a file, so the file is extended to its new length first
    /// and the handle is reopened to make the appended range accessible. Backends resize an
    /// existing file in place, preserving its content.
    ///
    /// This does not sync the file to disk, use [`flusher`](Self::flusher) afterwards.
    //
    // TODO(serverless): the append-only mode must grow the file and write the appended bytes in a single
    // syscall, growing the file before writing to it is not allowed. Universal IO has no such
    // append operation yet, replace the separate grow and write steps with it once it exists.
    pub(super) fn write_pending(&mut self, fs: &S::Fs, target_len: u64) -> Result<()> {
        if target_len <= self.persisted_len {
            return Ok(());
        }

        let count = (target_len - self.persisted_len) as usize;
        debug_assert!(
            count <= self.pending.len(),
            "flush target exceeds buffered value data",
        );
        let count = count.min(self.pending.len());

        if let Err(err) = self.grow_and_write(fs, count) {
            // Best effort: drop partially appended bytes, so that the file stays consistent
            // with the persisted length and a retried flush never rewrites existing bytes
            let _ = fs.create(&self.path, self.persisted_len as usize);
            return Err(err);
        }

        self.pending.drain(..count);
        self.persisted_len += count as u64;

        Ok(())
    }

    /// Grow the page file by `count` buffered bytes, then write those bytes at its old end.
    fn grow_and_write(&mut self, fs: &S::Fs, count: usize) -> Result<()> {
        let end = self.persisted_len + count as u64;
        fs.create(&self.path, end as usize)?;
        self.file.reopen()?;

        self.file
            .write(self.persisted_len, &self.pending[..count])?;

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

    /// An append beyond the maximum addressable block offset is rejected before buffering
    /// anything, so a retried put does not grow the page unboundedly.
    #[test]
    fn test_page_append_rejects_beyond_addressable_range() {
        let dir = TempDir::new().unwrap();
        let mut page = AppendOnlyPage::<MmapFile>::new(&MmapFs, dir.path()).unwrap();

        // Pretend the page already holds the maximum addressable amount of data
        let block_size_bytes = DEFAULT_BLOCK_SIZE_BYTES as u64;
        page.persisted_len = (u64::from(u32::MAX) + 1) * block_size_bytes;

        let err = page.append_value(&[1, 2, 3], block_size_bytes).unwrap_err();
        assert!(matches!(err, GridstoreError::ServiceError { .. }));

        // Nothing was written or buffered, and the tracked length is unchanged
        assert_eq!(
            fs::metadata(dir.path().join("append_only_page_0.dat"))
                .unwrap()
                .len(),
            0,
        );
        assert!(page.pending.is_empty());
        assert_eq!(page.len(), (u64::from(u32::MAX) + 1) * block_size_bytes);
    }
}
