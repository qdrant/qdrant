use std::path::{Path, PathBuf};

use fs_err::File;

use crate::error::GridstoreError;
use crate::gridstore::Flusher;
use crate::tracker::{BlockOffset, ValuePointer};
use crate::{Result, direct_io};

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
/// The file is read and written directly, it is never memory mapped.
#[derive(Debug)]
pub(super) struct AppendOnlyPage {
    /// Path to the page file
    path: PathBuf,
    /// Open handle to the page file
    file: File,
    /// Length of the page file in bytes, tracked in memory
    len: u64,
}

impl AppendOnlyPage {
    pub(super) fn page_file_name(dir: &Path) -> PathBuf {
        dir.join(PAGE_FILE_NAME)
    }

    /// Create a new empty page in the given directory.
    ///
    /// The directory must exist already.
    pub(super) fn new(dir: &Path) -> Result<Self> {
        let path = Self::page_file_name(dir);
        let file = direct_io::create_new(&path)?;
        Ok(Self { path, file, len: 0 })
    }

    /// Open an existing page in the given directory.
    ///
    /// If the file does not exist, return an error.
    pub(super) fn open(dir: &Path, writeable: bool) -> Result<Self> {
        let path = Self::page_file_name(dir);
        let file = direct_io::open_existing(&path, writeable, "Append-only page")?;
        let len = file.metadata()?.len();
        Ok(Self { path, file, len })
    }

    pub(super) fn files(&self) -> Vec<PathBuf> {
        vec![self.path.clone()]
    }

    /// Length of the page file in bytes, which is the end of the last appended value.
    pub(super) fn len(&self) -> u64 {
        self.len
    }

    /// Append a value at the next block aligned offset, returning the block offset it landed at.
    ///
    /// The write starts exactly at the current end of the file and includes the zero padding up
    /// to the next block boundary, so that it is a pure append.
    pub(super) fn append_value(
        &mut self,
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

        let pad = (start - self.len) as usize;
        let result = if pad == 0 {
            direct_io::write_all_at(&self.file, value, self.len)
        } else {
            // Prefix the write with the padding, so that it lands at the end of the file
            let mut buf = vec![0; pad + value.len()];
            buf[pad..].copy_from_slice(value);
            direct_io::write_all_at(&self.file, &buf, self.len)
        };
        if let Err(err) = result {
            // Best effort: drop partially appended bytes, so that the file stays consistent
            // with the tracked length and a retried append never rewrites existing bytes
            let _ = self.file.set_len(self.len);
            return Err(err.into());
        }
        self.len = start + value.len() as u64;

        Ok(block_offset)
    }

    /// Read the raw value bytes at the given pointer.
    pub(super) fn read_value(
        &self,
        pointer: ValuePointer,
        block_size_bytes: u64,
    ) -> Result<Vec<u8>> {
        // The append-only mode stores all values in a single page
        if pointer.page_id != 0 {
            return Err(GridstoreError::PageNotFound {
                page_id: pointer.page_id,
            });
        }

        let start = u64::from(pointer.block_offset) * block_size_bytes;
        let mut buf = vec![0; pointer.length as usize];
        direct_io::read_exact_at(&self.file, &mut buf, start)?;
        Ok(buf)
    }

    /// Reload the length from the file, making newly appended value data visible in the reported
    /// storage size.
    ///
    /// Reads themselves always go directly to the file and need no reload.
    pub(super) fn refresh_len(&mut self) -> Result<()> {
        self.len = self.file.metadata()?.len();
        Ok(())
    }

    /// Create a closure that syncs all written value data in the page file to disk.
    pub(super) fn flusher(&self) -> Result<Flusher> {
        let file = self.file.try_clone()?;
        Ok(Box::new(move || {
            file.sync_data()?;
            Ok(())
        }))
    }
}

#[cfg(test)]
mod tests {
    use fs_err as fs;
    use tempfile::TempDir;

    use super::*;
    use crate::config::DEFAULT_BLOCK_SIZE_BYTES;

    /// An append beyond the maximum addressable block offset is rejected before writing
    /// anything, so a retried put does not grow the page file unboundedly.
    #[test]
    fn test_page_append_rejects_beyond_addressable_range() {
        let dir = TempDir::new().unwrap();
        let mut page = AppendOnlyPage::new(dir.path()).unwrap();

        // Pretend the page already holds the maximum addressable amount of data
        let block_size_bytes = DEFAULT_BLOCK_SIZE_BYTES as u64;
        page.len = (u64::from(u32::MAX) + 1) * block_size_bytes;

        let err = page.append_value(&[1, 2, 3], block_size_bytes).unwrap_err();
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
