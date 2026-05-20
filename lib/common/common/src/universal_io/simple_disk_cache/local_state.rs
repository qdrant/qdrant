use std::ops::Range;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use fs_err as fs;
use memmap2::MmapRaw;
use parking_lot::Mutex;
use roaring::RoaringBitmap;

use crate::mmap::Madviseable;
use crate::universal_io::simple_disk_cache::BLOCK_SIZE;
use crate::universal_io::{OpenOptions, Result};

#[derive(Debug)]
pub(super) struct LocalState {
    pub mmap: MmapRaw,
    /// Bitmask to know which blocks have been fetched so far.
    pub fetched: Mutex<RoaringBitmap>,
    /// Fast-path flag: when true, the mmap is fully populated and the
    /// `fetched` bitmap can be skipped on the read hot path.
    pub fully_populated: AtomicBool,
}

impl LocalState {
    pub(super) fn new(
        local_path: impl AsRef<Path>,
        len: u64,
        options: OpenOptions,
    ) -> Result<Self> {
        if let Some(parent) = local_path.as_ref().parent() {
            fs::create_dir_all(parent)?;
        }

        let OpenOptions {
            writeable: _,       // always needs to be writeable
            need_sequential: _, // TODO: add sequential mmap
            populate: _,        // this is handled externally to LocalState
            advice,
            extra: _, // unsupported
        } = options;

        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(local_path.as_ref())?;

        file.set_len(len)?;
        let mmap = MmapRaw::map_raw(&file)?;

        mmap.madvise(advice.resolve())?;

        Ok(LocalState {
            mmap,
            fetched: Mutex::new(RoaringBitmap::new()),
            fully_populated: AtomicBool::new(false),
        })
    }

    /// Whether `blocks_range` is already cached locally.
    ///
    /// Cheap when the file is fully populated (one relaxed atomic load);
    /// otherwise locks `fetched` and checks the bitmap.
    pub(super) fn contains(&self, blocks_range: Range<u32>) -> bool {
        if self.fully_populated.load(Ordering::Relaxed) {
            return true;
        }
        self.fetched.lock().contains_range(blocks_range)
    }

    /// Mark the local mirror as fully populated. Subsequent reads can skip
    /// the `fetched` bitmap check.
    pub(super) fn mark_fully_populated(&self) {
        self.fully_populated.store(true, Ordering::Relaxed);
    }

    /// # Safety
    /// `byte_range` must have been populated first, caller must ensure `self.fetched` references the
    /// blocks for the byte range.
    pub(super) unsafe fn read_mmap_bytes(&self, byte_range: Range<u64>) -> &[u8] {
        let bytes = unsafe { std::slice::from_raw_parts(self.mmap.as_ptr(), self.mmap.len()) };
        &bytes[byte_range.start as usize..byte_range.end as usize]
    }

    /// # Safety
    /// `DiskCache` is only used in immutable files. Since `blocks_range` can include already-fetched
    /// data, it is possible that some sections get overwritten; however, it should be the same data,
    /// so it is fine.
    ///
    /// Assumes the bytes slice covers the entirety of `blocks_range`.
    pub(super) unsafe fn write_mmap_bytes(&self, bytes: &[u8], blocks_range: Range<u32>) {
        if self.fully_populated.load(Ordering::Relaxed) {
            return;
        }

        let mut fetched = self.fetched.lock();
        if fetched.contains_range(blocks_range.clone()) {
            return;
        }

        let byte_offset = blocks_range.start as usize * BLOCK_SIZE;

        let max_len = self.mmap.len().saturating_sub(byte_offset);
        assert!(bytes.len() == max_len.min(blocks_range.len() * BLOCK_SIZE));

        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                self.mmap.as_mut_ptr().add(byte_offset),
                bytes.len(),
            );
        }
        fetched.insert_range(blocks_range);
    }
}
