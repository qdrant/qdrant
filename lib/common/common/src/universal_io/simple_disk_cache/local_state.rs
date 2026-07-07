use std::cell::UnsafeCell;
use std::io::ErrorKind;
use std::ops::Range;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use fs_err as fs;
use parking_lot::Mutex;
use roaring::RoaringBitmap;

use crate::generic_consts::AccessPattern;
use crate::universal_io::simple_disk_cache::BLOCK_SIZE;
use crate::universal_io::{
    MmapFile, MmapFs, OpenOptions, Populate, Result, UniversalIoError, UniversalRead,
    UniversalReadFs, UniversalWrite, mmap as mmap_file,
};

#[derive(Debug)]
pub(crate) struct LocalState {
    /// UnsafeCell so that we can write to it under non-mut reference.
    /// Such as when the pipeline reads from remote first.
    pub mmap: UnsafeCell<MmapFile>,
    /// Bitmask to know which blocks have been fetched so far.
    pub fetched: Mutex<RoaringBitmap>,
    /// Fast-path flag: when true, the mmap is fully populated and the
    /// `fetched` bitmap can be skipped on the read hot path.
    pub fully_populated: AtomicBool,
}

unsafe impl Sync for LocalState {}

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
            writeable: _, // always needs to be writeable
            need_sequential,
            populate: _, // this is handled externally to LocalState
            advice,
        } = options;

        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(local_path.as_ref())?;

        file.set_len(len)?;
        let mmap = MmapFs.open(
            local_path.as_ref(),
            OpenOptions {
                writeable: true,
                need_sequential,
                populate: Populate::No,
                advice,
            },
            (),
        )?;

        Ok(LocalState {
            mmap: UnsafeCell::new(mmap),
            fetched: Mutex::new(RoaringBitmap::new()),
            fully_populated: AtomicBool::new(false),
        })
    }

    pub(super) fn resize(&mut self, local_path: impl AsRef<Path>, new_len: u64) -> Result<()> {
        let mmap = self.mmap.get_mut();
        let current_len = mmap.len::<u8>()?;
        if current_len == new_len {
            return Ok(());
        }
        if current_len > new_len {
            return Err(UniversalIoError::Io(std::io::Error::new(
                ErrorKind::Unsupported,
                "Shrinking the file is not supported",
            )));
        }
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(local_path.as_ref())?;

        file.set_len(new_len)?;

        mmap.reopen()?;

        *self.fully_populated.get_mut() = false;

        // The previous tail block may have been only partially populated (its
        // fetch was clamped to the old EOF). `set_len` zero-filled the bytes
        // between the old EOF and the next block boundary, so the block is no
        // longer accurate. Drop it from `fetched` to force a re-fetch on the
        // next read.
        if current_len % BLOCK_SIZE as u64 != 0 {
            let partial_tail_block = (current_len / BLOCK_SIZE as u64) as u32;
            self.fetched.lock().remove(partial_tail_block);
        }

        Ok(())
    }

    pub(super) fn mmap(&self) -> &MmapFile {
        // SAFETY: we have `&self` reference
        unsafe { self.mmap.get().as_ref_unchecked() }
    }

    /// Write-through for bytes just appended to the remote: grow the mirror
    /// and fill `slices` (contiguously, in order) at `offset`, which must
    /// equal the current mirror length.
    ///
    /// Every block whose in-file extent is fully covered by the appended
    /// range is marked fetched — including the partial block at the new
    /// end-of-file, since fetches clamp to it. The pre-append tail block is
    /// re-marked only if it was fetched before ([`Self::resize`] drops it
    /// because `set_len` zero-fills its gap; the write below overwrites that
    /// gap with the real bytes, making the block accurate again iff its
    /// pre-`offset` prefix was).
    pub(super) fn append_local(
        &mut self,
        local_path: impl AsRef<Path>,
        offset: u64,
        slices: &[&[u8]],
    ) -> Result<()> {
        let block_size = BLOCK_SIZE as u64;
        let total: u64 = slices.iter().map(|slice| slice.len() as u64).sum();
        let new_len = offset + total;

        let pre_append_tail_block =
            (!offset.is_multiple_of(block_size)).then(|| (offset / block_size) as u32);
        let was_tail_fetched = pre_append_tail_block
            .is_some_and(|tail_block| self.fetched.get_mut().contains(tail_block));

        self.resize(&local_path, new_len)?;

        let mmap = self.mmap.get_mut();
        let mut position = offset;
        for slice in slices {
            mmap.write(position, slice)?;
            position += slice.len() as u64;
        }

        let fetched = self.fetched.get_mut();
        let first_covered_block = offset.div_ceil(block_size) as u32;
        let end_block = new_len.div_ceil(block_size) as u32;
        fetched.insert_range(first_covered_block..end_block);
        if was_tail_fetched {
            fetched.insert(pre_append_tail_block.expect("checked above"));
        }

        // `resize` cleared `fully_populated`; recompute it.
        let total_blocks = new_len.div_ceil(block_size);
        if fetched.len() == total_blocks {
            *self.fully_populated.get_mut() = true;
        }

        Ok(())
    }

    /// Whether `blocks_range` is already cached locally.
    ///
    /// Cheap when the file is fully populated (one relaxed atomic load);
    /// otherwise locks `fetched` and checks the bitmap.
    pub(super) fn contains(&self, blocks_range: Range<u32>) -> bool {
        if self.fully_populated.load(Ordering::Acquire) {
            return true;
        }
        self.fetched.lock().contains_range(blocks_range)
    }

    /// # Safety
    /// `byte_range` must have been populated first, caller must ensure `self.fetched` references the
    /// blocks for the byte range.
    pub(super) unsafe fn read_mmap_bytes<P: AccessPattern>(
        &self,
        range: Range<u64>,
    ) -> Result<&[u8]> {
        let mmap_bytes = self.mmap().as_bytes::<P>();
        mmap_file::read_bytes(mmap_bytes, range)
    }

    /// # Safety
    /// `DiskCache` is only used for append-only remotes with an immutable
    /// prefix, so every fetch of `blocks_range` yields the same bytes the
    /// mirror already holds for it (appends write-through via
    /// [`Self::append_local`] before any fetch can cover them). Since
    /// `blocks_range` can include already-fetched data, it is possible that
    /// some sections get overwritten; however, it is the same data, so it is
    /// fine.
    ///
    /// Assumes the bytes slice covers the entirety of `blocks_range`.
    pub(super) unsafe fn write_mmap_bytes(&self, bytes: &[u8], blocks_range: Range<u32>) {
        // SAFETY:
        // 1. The remote's existing bytes are immutable, so worst case, same
        //    data is overwritten.
        // 2. The `fetched` bitmap should track which blocks are already present.
        let mmap = unsafe { self.mmap.get().as_mut_unchecked() };
        if self.fully_populated.load(Ordering::Acquire) {
            return;
        }

        let mut fetched = self.fetched.lock();
        if fetched.contains_range(blocks_range.clone()) {
            return;
        }

        let byte_offset = (blocks_range.start as usize * BLOCK_SIZE) as u64;

        let max_len = mmap
            .len::<u8>()
            .expect("MmapFile::len is infallible")
            .saturating_sub(byte_offset);
        assert_eq!(
            bytes.len() as u64,
            max_len.min((blocks_range.len() * BLOCK_SIZE) as u64)
        );

        mmap.write(byte_offset, bytes)
            .expect("MmapFile::write is infallible");

        fetched.insert_range(blocks_range);

        // If every block is now populated, turn `fully_populated` on.
        let total_blocks = mmap
            .len::<u8>()
            .expect("MmapFile::len is infallible")
            .div_ceil(BLOCK_SIZE as u64);
        if fetched.len() == total_blocks {
            self.fully_populated.store(true, Ordering::Release);
        }
    }
}
