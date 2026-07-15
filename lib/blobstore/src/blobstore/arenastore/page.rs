use std::borrow::Cow;
use std::path::{Path, PathBuf};

use ahash::HashSet;
use common::generic_consts::AccessPattern;
use common::mmap::{Advice, AdviceSetting};
use common::universal_io::{
    CachedReadFs, IsNotFound, OpenOptions, Populate, ReadRange, UniversalAppend, UniversalIoError,
    UniversalRead, UniversalReadFileOps, UniversalReadFs, UniversalWriteFileOps,
};

use crate::Result;
use crate::blobstore::Flusher;
use crate::error::GridstoreError;
use crate::tracker::{BlockOffset, PageId, ValuePointer};

/// File name prefix of the append-only page files, followed by `{id}.dat`
///
/// Deliberately different from the mutable mode page file names (`page_{id}.dat`), so that one mode
/// never attempts to load the incompatible file format of the other.
const PAGE_FILE_NAME_PREFIX: &str = "arena_page_";

fn page_file_name(dir: &Path, page_id: PageId) -> PathBuf {
    dir.join(format!("{PAGE_FILE_NAME_PREFIX}{page_id}.dat"))
}

/// The append-only page files of the append-only storage mode.
///
/// Values are packed back to back in the pages, without blocks or alignment: each value starts
/// right after the previous one. Once appending a value would grow the current page beyond the
/// configured page size, a new page is started, bounding the size of and the number of appends
/// to each file (object stores limit appends per object). A value larger than the page size gets
/// a page of its own; values never span pages.
///
/// Page files start empty and only ever grow by appending; existing bytes are never rewritten.
#[derive(Debug)]
pub(super) struct AppendOnlyPages<S> {
    /// Directory holding the page files
    dir: PathBuf,
    /// All pages, the page id is the index. Never empty, values are appended to the last page.
    pages: Vec<AppendOnlyPage<S>>,
}

impl<S: UniversalRead> AppendOnlyPages<S> {
    /// Schedule prefetches of all page files, so a subsequent open is served from the prefetch
    /// pool.
    pub(super) fn preopen<Fs: CachedReadFs<File = S>>(fs: &Fs, dir: &Path) -> Result<()> {
        let page_files: HashSet<_> = fs
            .list_files(&dir.join(PAGE_FILE_NAME_PREFIX))?
            .into_iter()
            .map(|listed| listed.path)
            .collect();

        for page_id in 0.. {
            let path = page_file_name(dir, page_id);
            if !page_files.contains(&path) {
                break;
            }
            fs.schedule_prefetch(&path, Some(AppendOnlyPage::<S>::open_options(false)), None)?;
        }
        Ok(())
    }

    /// Open the existing pages in the given directory.
    ///
    /// Scans for consecutively numbered page files, starting at page 0. If no page file exists,
    /// return an error.
    pub(super) fn open<Fs: UniversalReadFs<File = S>>(
        fs: &Fs,
        dir: &Path,
        writeable: bool,
    ) -> Result<Self> {
        let page_files: HashSet<_> = fs
            .list_files(&dir.join(PAGE_FILE_NAME_PREFIX))?
            .into_iter()
            .map(|listed| listed.path)
            .collect();

        let mut pages = Vec::new();
        for page_id in 0.. {
            let path = page_file_name(dir, page_id);
            if !page_files.contains(&path) {
                break;
            }
            pages.push(AppendOnlyPage::open(fs, path, writeable)?);
        }

        if pages.is_empty() {
            // If config exists and no page file does, it should be treated as inconsistent
            // storage rather than a missing one
            return Err(GridstoreError::service_error(format!(
                "Append-only page file does not exist: {}",
                page_file_name(dir, 0).display(),
            )));
        }

        Ok(Self {
            dir: dir.to_path_buf(),
            pages,
        })
    }

    pub(super) fn files(&self) -> Vec<PathBuf> {
        self.pages.iter().map(|page| page.path.clone()).collect()
    }

    /// Total length of the value data in all pages, in bytes.
    ///
    /// This includes buffered values that haven't been flushed to the files yet.
    pub(super) fn len(&self) -> u64 {
        self.pages.iter().map(AppendOnlyPage::len).sum()
    }

    /// Length of the value data in the given page, or `None` if the page does not exist.
    ///
    /// This includes buffered values that haven't been flushed to the file yet.
    pub(super) fn page_len(&self, page_id: PageId) -> Option<u64> {
        self.pages.get(page_id as usize).map(AppendOnlyPage::len)
    }

    /// Read the raw value bytes at the given pointer.
    ///
    /// Values that haven't been flushed yet are served from the in-memory buffers.
    pub(super) fn read_value<P: AccessPattern>(
        &self,
        pointer: ValuePointer,
    ) -> Result<Cow<'_, [u8]>> {
        let page =
            self.pages
                .get(pointer.page_id as usize)
                .ok_or(GridstoreError::PageNotFound {
                    page_id: pointer.page_id,
                })?;
        page.read_value::<P>(pointer)
    }

    /// Reload the pages from "disk", making newly appended value data visible to reads and the
    /// reported storage size.
    ///
    /// Reloads the last held page, the only one that can have grown, and adopts page files
    /// created since. Earlier pages never change once a newer page exists.
    pub(super) fn live_reload(&mut self, fs: &S::Fs) -> Result<()> {
        if let Some(last) = self.pages.last_mut() {
            last.live_reload()?;
        }

        for page_id in self.pages.len() as PageId.. {
            let path = page_file_name(&self.dir, page_id);
            if !fs.exists(&path)? {
                break;
            }
            self.pages.push(AppendOnlyPage::open(fs, path, false)?);
        }

        Ok(())
    }
}

impl<S: UniversalAppend> AppendOnlyPages<S> {
    /// Create a new empty page 0 in the given directory, truncating it if it already exists.
    ///
    /// The directory must exist already.
    pub(super) fn new(fs: &S::Fs, dir: &Path) -> Result<Self> {
        let page = AppendOnlyPage::new(fs, page_file_name(dir, 0))?;
        Ok(Self {
            dir: dir.to_path_buf(),
            pages: vec![page],
        })
    }

    /// Append a value, returning the page it landed in and its byte offset within that page.
    ///
    /// The value is appended right after the previous one, without alignment. When it does not
    /// fit within `page_capacity_bytes` of the current page, a new page file is created and the
    /// value starts that page instead. A value larger than the capacity gets a page of its own;
    /// values never span pages.
    ///
    /// The value is buffered in memory until the next flush; only a page rollover touches disk
    /// by creating the new, empty page file.
    pub(super) fn append_value(
        &mut self,
        fs: &S::Fs,
        value: &[u8],
        page_capacity_bytes: u64,
    ) -> Result<(PageId, BlockOffset)> {
        let last = self
            .pages
            .last()
            .expect("there is always at least one page");
        if last.len() > 0 && last.len() + value.len() as u64 > page_capacity_bytes {
            let page_id = self.pages.len() as PageId;
            let page = AppendOnlyPage::new(fs, page_file_name(&self.dir, page_id))?;
            self.pages.push(page);
        }

        let page_id = (self.pages.len() - 1) as PageId;
        let offset = self
            .pages
            .last_mut()
            .expect("there is always at least one page")
            .append_value(value)?;

        Ok((page_id, offset))
    }

    /// Per-page lengths to capture as flush targets, see [`write_pending`](Self::write_pending).
    pub(super) fn page_lens(&self) -> Vec<u64> {
        self.pages.iter().map(AppendOnlyPage::len).collect()
    }

    /// Append buffered values up to the captured per-page `targets` to the page files.
    ///
    /// Pages created after the targets were captured are skipped, their values stay buffered for
    /// the next flush. Only the pages that were current when their target was captured can have
    /// gained data since; for the older, sealed pages a stale target is a no-op.
    ///
    /// This does not sync the files to disk, use [`flusher`](Self::flusher) afterwards.
    pub(super) fn write_pending(&mut self, targets: &[u64]) -> Result<()> {
        for (page, target_len) in self.pages.iter_mut().zip(targets) {
            page.write_pending(*target_len)?;
        }
        Ok(())
    }

    /// Create a closure that syncs all written value data in the page files to disk.
    pub(super) fn flusher(&self) -> Flusher {
        let flushers: Vec<_> = self.pages.iter().map(AppendOnlyPage::flusher).collect();
        Box::new(move || {
            for flusher in flushers {
                flusher()?;
            }
            Ok(())
        })
    }
}

/// A single append-only page file holding raw (compressed) value bytes.
///
/// The file length always matches the end of the last flushed value, there is no preallocation
/// and no trailing padding.
///
/// Newly appended values are buffered in memory, and written to the file as a single append when
/// flushing. Reads transparently serve buffered values from memory.
///
/// The file is read and written through the universal IO backend `S`.
#[derive(Debug)]
struct AppendOnlyPage<S> {
    /// Path to the page file
    path: PathBuf,
    /// Open handle to the page file
    file: S,
    /// Length of the value data persisted in the page file, in bytes
    persisted_len: u64,
    /// Value data that hasn't been written to the file yet
    ///
    /// Byte `i` corresponds to file offset `persisted_len + i`, so this is byte for byte the
    /// data of the next append.
    pending: Vec<u8>,
}

impl<S: UniversalRead> AppendOnlyPage<S> {
    /// Universal IO open options for a page file.
    fn open_options(writeable: bool) -> OpenOptions {
        OpenOptions {
            writeable,
            need_sequential: true,
            // The append-only mode never populates, see [`super::Arenastore::populate`]
            populate: Populate::No,
            advice: AdviceSetting::Advice(Advice::Random),
        }
    }

    /// Open an existing page file at the given path.
    ///
    /// If the file does not exist, return an error.
    fn open<Fs: UniversalReadFs<File = S>>(
        fs: &Fs,
        path: PathBuf,
        writeable: bool,
    ) -> Result<Self> {
        let file = fs
            .open(&path, Self::open_options(writeable), Default::default())
            .map_err(|err| {
                if err.is_not_found() {
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

    /// Length of the value data in bytes, which is the end of the last appended value.
    ///
    /// This includes buffered values that haven't been flushed to the file yet.
    fn len(&self) -> u64 {
        self.persisted_len + self.pending.len() as u64
    }

    /// Read the raw value bytes at the given pointer.
    ///
    /// The pointer offset is the byte offset within this page, values are packed without blocks
    /// or alignment. Values that haven't been flushed yet are served from the in-memory buffer.
    fn read_value<P: AccessPattern>(&self, pointer: ValuePointer) -> Result<Cow<'_, [u8]>> {
        let start = u64::from(pointer.block_offset);
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
    fn live_reload(&mut self) -> Result<()> {
        debug_assert!(
            self.pending.is_empty(),
            "live reload must only be used on read-only instances",
        );
        self.file.reopen()?;
        self.persisted_len = self.file.len::<u8>()?;
        Ok(())
    }
}

impl<S: UniversalAppend> AppendOnlyPage<S> {
    /// Create a new empty page file at the given path, truncating it if it already exists.
    ///
    /// The directory must exist already.
    fn new(fs: &S::Fs, path: PathBuf) -> Result<Self> {
        fs.create(&path, 0)?;
        let file = fs.open(&path, Self::open_options(true), Default::default())?;
        Ok(Self {
            path,
            file,
            persisted_len: 0,
            pending: Vec::new(),
        })
    }

    /// Append a value right after the previous one, returning the byte offset it landed at.
    ///
    /// The value is buffered in memory until the next flush, so that the flush appends all
    /// buffered values with a single write that lands exactly at the end of the file.
    fn append_value(&mut self, value: &[u8]) -> Result<BlockOffset> {
        // Validate addressability before buffering anything, a rejected append must not grow
        // the page
        let offset = BlockOffset::try_from(self.len()).map_err(|_| {
            GridstoreError::service_error(format!(
                "append-only page file {} exceeds the maximum addressable size",
                self.path.display(),
            ))
        })?;

        self.pending.extend_from_slice(value);

        Ok(offset)
    }

    /// Append buffered values for file offsets up to, but excluding, `target_len` to the file.
    ///
    /// All buffered value data lands with a single atomic append at the end of the file: one
    /// grow+write syscall on local backends, one RPC on object stores. The `target_len` is
    /// captured through [`len`](Self::len) when a flusher is created, so that values appended
    /// while a flush is in progress stay buffered.
    ///
    /// A stale flush, with a `target_len` at or below what a more recent flush already
    /// persisted, is a no-op: bytes that were appended before must never be written again.
    ///
    /// The append offset doubles as a compare-and-swap token: if a previous flush appended
    /// these bytes but its acknowledgement was lost, the retry conflicts instead of appending
    /// twice, and the bytes are adopted as persisted when the file ends exactly where this
    /// append would have ended.
    ///
    /// This does not sync the file to disk, use [`flusher`](Self::flusher) afterwards.
    fn write_pending(&mut self, target_len: u64) -> Result<()> {
        if target_len <= self.persisted_len {
            return Ok(());
        }

        let count = (target_len - self.persisted_len) as usize;
        debug_assert!(
            count <= self.pending.len(),
            "flush target exceeds buffered value data",
        );
        let count = count.min(self.pending.len());
        let end = self.persisted_len + count as u64;

        match self.file.append(self.persisted_len, &self.pending[..count]) {
            Ok(()) => {}
            // A retried append after a lost acknowledgement conflicts instead of appending
            // twice. If the file ends exactly where this append would have ended, the bytes
            // landed before; adopt them as persisted. Any other length means the file was
            // modified outside this writer.
            Err(UniversalIoError::AppendOffsetConflict { .. }) => {
                self.file.reopen()?;
                let len = self.file.len::<u8>()?;
                if len != end {
                    return Err(GridstoreError::service_error(format!(
                        "append-only page file {} was modified outside this writer: it ends \
                         at byte {len}, expected {} before or {end} after the append",
                        self.path.display(),
                        self.persisted_len,
                    )));
                }
            }
            Err(err) => return Err(err.into()),
        }

        self.pending.drain(..count);
        self.persisted_len = end;

        Ok(())
    }

    /// Create a closure that syncs all written value data in the page file to disk.
    fn flusher(&self) -> Flusher {
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

    /// An append beyond the maximum addressable byte offset is rejected before buffering
    /// anything, so a retried put does not grow the page unboundedly.
    #[test]
    fn test_page_append_rejects_beyond_addressable_range() {
        let dir = TempDir::new().unwrap();
        let path = page_file_name(dir.path(), 0);
        let mut page = AppendOnlyPage::<MmapFile>::new(&MmapFs, path).unwrap();

        // Pretend the page already holds the maximum addressable amount of data
        page.persisted_len = u64::from(u32::MAX) + 1;

        let err = page.append_value(&[1, 2, 3]).unwrap_err();
        assert!(matches!(err, GridstoreError::ServiceError { .. }));

        // Nothing was written or buffered, and the tracked length is unchanged
        assert_eq!(
            fs::metadata(dir.path().join("arena_page_0.dat"))
                .unwrap()
                .len(),
            0,
        );
        assert!(page.pending.is_empty());
        assert_eq!(page.len(), u64::from(u32::MAX) + 1);
    }

    /// A conflicting append whose bytes already landed — a retried flush after a lost
    /// acknowledgement — is adopted as persisted instead of appending twice.
    #[test]
    fn test_write_pending_adopts_lost_append_on_conflict() {
        let dir = TempDir::new().unwrap();
        let path = page_file_name(dir.path(), 0);
        let mut page = AppendOnlyPage::<MmapFile>::new(&MmapFs, path.clone()).unwrap();

        page.append_value(&[1, 2, 3, 4]).unwrap();

        // Simulate a flush that landed but was never acknowledged: the bytes are in the
        // (previously empty) file, but the page still counts them as pending
        fs::write(&path, [1, 2, 3, 4]).unwrap();

        page.write_pending(4).unwrap();
        assert_eq!(page.persisted_len, 4);
        assert!(page.pending.is_empty());
        assert_eq!(
            fs::read(&path).unwrap(),
            [1, 2, 3, 4],
            "the bytes must not be appended twice",
        );
    }

    /// A conflicting append against a file that does not end where the append would have
    /// ended fails: the file was modified outside this writer.
    #[test]
    fn test_write_pending_rejects_foreign_growth() {
        let dir = TempDir::new().unwrap();
        let path = page_file_name(dir.path(), 0);
        let mut page = AppendOnlyPage::<MmapFile>::new(&MmapFs, path.clone()).unwrap();

        page.append_value(&[1, 2, 3, 4]).unwrap();

        // Something else grew the (previously empty) file to an unexpected length
        fs::write(&path, [9; 7]).unwrap();

        let err = page.write_pending(4).unwrap_err();
        assert!(matches!(err, GridstoreError::ServiceError { .. }));

        // Nothing was adopted, the pending bytes are kept for a later flush
        assert_eq!(page.persisted_len, 0);
        assert_eq!(page.pending, [1, 2, 3, 4]);
    }
}
