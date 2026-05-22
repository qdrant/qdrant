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
    MmapFile, OpenOptions, OpenOptionsExtra, Populate, ReadRange, Result, UniversalIoError,
    UniversalRead, UniversalWrite, mmap as mmap_file,
};

#[derive(Debug)]
pub(super) struct LocalState {
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
            extra: _, // unsupported
        } = options;

        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(local_path.as_ref())?;

        file.set_len(len)?;
        let mmap = MmapFile::open(
            local_path.as_ref(),
            OpenOptions {
                writeable: true,
                need_sequential,
                populate: Populate::No,
                advice,
                extra: OpenOptionsExtra::default(),
            },
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

        self.fully_populated.store(false, Ordering::Release);

        Ok(())
    }

    pub(super) fn mmap(&self) -> &MmapFile {
        // SAFETY: we have `&self` reference
        unsafe { self.mmap.get().as_ref_unchecked() }
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

    /// Mark the local mirror as fully populated. Subsequent reads can skip
    /// the `fetched` bitmap check.
    pub(super) fn mark_fully_populated(&self, last_block: u32) {
        self.fully_populated.store(true, Ordering::Release);
        self.fetched.lock().insert_range(0..last_block);
    }

    /// # Safety
    /// `byte_range` must have been populated first, caller must ensure `self.fetched` references the
    /// blocks for the byte range.
    pub(super) unsafe fn read_mmap_bytes<P: AccessPattern, T: bytemuck::Pod>(
        &self,
        range: ReadRange,
    ) -> Result<&[T]> {
        let mmap_bytes = self.mmap().as_bytes::<P>();
        mmap_file::read::<T>(mmap_bytes, range)
    }

    /// # Safety
    /// `DiskCache` is only used in immutable files. Since `blocks_range` can include already-fetched
    /// data, it is possible that some sections get overwritten; however, it should be the same data,
    /// so it is fine.
    ///
    /// Assumes the bytes slice covers the entirety of `blocks_range`.
    pub(super) unsafe fn write_mmap_bytes(&self, bytes: &[u8], blocks_range: Range<u32>) {
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
    }
}
