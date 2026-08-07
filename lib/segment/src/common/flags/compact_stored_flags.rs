use std::path::{Path, PathBuf};
use std::sync::Arc;

use common::is_alive_lock::IsAliveLock;
use common::mmap::AdviceSetting;
use common::stored_bitmask::MutableStoredBitmask;
use common::types::PointOffsetType;
use common::universal_io::{
    OkNotFound, OpenOptions, Populate, UniversalRead, UniversalWrite, UniversalWriteFileOps,
};
use parking_lot::Mutex;
use roaring::RoaringBitmap;

use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};

/// Name of the single file holding the mask, inside the storage directory.
pub(super) const COMPACT_FLAGS_FILE: &str = "compact_flags.dat";

/// Flags over a single compact stored-bitmask file, rewritten whole on flush.
///
/// The serverless-compatible counterpart of `DynamicStoredFlags` +
/// `BufferedDynamicFlags`: the mask is fully resident in RAM, mutations
/// collect there, and a flush rewrites the file in one atomic whole-file
/// write — no in-place mmap mutation, so it also works on object stores.
/// A flush with no effective changes skips the write entirely.
#[derive(Debug)]
pub struct CompactStoredFlags<S: UniversalRead> {
    /// The mask, resident in RAM.
    mask: Arc<Mutex<MutableStoredBitmask>>,

    /// Filesystem handle used to rewrite the file on flush.
    fs: Arc<S::Fs>,

    /// Path of the mask file inside the storage directory.
    path: PathBuf,

    /// Lock to prevent concurrent flush and drop
    is_alive_flush_lock: IsAliveLock,
}

impl<S> CompactStoredFlags<S>
where
    S: UniversalWrite + Send + 'static,
    S::Fs: Send + Sync + 'static,
{
    /// Open the flags in `directory`, materializing the whole mask into RAM.
    ///
    /// Creates the directory and persists an empty mask when the file is
    /// missing, so [`Self::files`] always exist on disk.
    pub fn open(fs: S::Fs, directory: &Path, populate: Populate) -> OperationResult<Self> {
        fs.create_dir(directory)?;
        let path = directory.join(COMPACT_FLAGS_FILE);

        let options = OpenOptions {
            writeable: false,
            need_sequential: true,
            populate,
            advice: AdviceSetting::Global,
        };
        let mask = match MutableStoredBitmask::open(&fs, &path, options, Default::default())
            .ok_not_found()?
        {
            Some(mask) => mask,
            None => {
                let mut mask = MutableStoredBitmask::new(0);
                mask.save(&fs, &path)?;
                mask
            }
        };

        Ok(Self {
            mask: Arc::new(Mutex::new(mask)),
            fs: Arc::new(fs),
            path,
            is_alive_flush_lock: IsAliveLock::new(),
        })
    }

    /// Number of logical flags in the mask.
    pub fn len(&self) -> usize {
        self.mask.lock().bit_len() as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Value of the flag at `index`; `false` at and beyond [`Self::len`].
    pub fn get(&self, index: PointOffsetType) -> bool {
        self.mask.lock().get(index)
    }

    /// Number of set flags.
    pub fn count_flags(&self) -> usize {
        self.mask.lock().count_ones() as usize
    }

    /// Snapshot of the set flags as a roaring bitmap.
    pub fn to_bitmap(&self) -> RoaringBitmap {
        self.mask.lock().ones().clone()
    }

    /// Set the flag at `index`, returning its previous value. Grows the mask
    /// when `index` is at or beyond its length.
    pub fn set(&self, index: PointOffsetType, value: bool) -> bool {
        let mut mask = self.mask.lock();
        if u64::from(index) >= mask.bit_len() {
            mask.set_len(u64::from(index) + 1);
        }
        mask.set(index, value)
    }

    /// Grow the mask to `new_len` flags; the new flags are unset.
    ///
    /// # Panics
    ///
    /// If `new_len` would shrink the mask.
    pub fn set_len(&self, new_len: usize) {
        self.mask.lock().set_len(new_len as u64);
    }

    /// Flusher that persists the mask as it is at the moment of flushing.
    ///
    /// The write is skipped when nothing effectively changed since the mask
    /// was opened or last flushed. Flushing after the instance was dropped
    /// is cancelled.
    pub fn flusher(&self) -> Flusher {
        // The mask persists itself and knows when it is clean: nothing is
        // snapshotted here. Clean right now means this flush cycle has
        // nothing to write; later mutations are for the next cycle.
        if !self.mask.lock().is_dirty() {
            return Box::new(|| Ok(()));
        }

        // Weak reference to detect when the storage has been deleted
        let mask_weak = Arc::downgrade(&self.mask);
        let fs = Arc::clone(&self.fs);
        let path = self.path.clone();
        let is_alive_flush_lock = self.is_alive_flush_lock.handle();

        Box::new(move || {
            let (Some(is_alive_flush_guard), Some(mask_arc)) =
                (is_alive_flush_lock.lock_if_alive(), mask_weak.upgrade())
            else {
                log::trace!("CompactStoredFlags was dropped, cancelling flush");
                return Err(OperationError::cancelled(
                    "Aborted flushing on a dropped CompactStoredFlags instance",
                ));
            };

            // TODO(serverless): we should not lock here during save
            // TODO(serverless): currently acceptable because readers and writers are completely separate
            mask_arc.lock().save(&*fs, &path)?;

            drop(is_alive_flush_guard);
            Ok(())
        })
    }

    /// The single file backing the flags; guaranteed to exist on disk.
    pub fn files(&self) -> Vec<PathBuf> {
        vec![self.path.clone()]
    }

    /// No-op: the mask is fully resident from open.
    pub fn populate(&self) -> OperationResult<()> {
        Ok(())
    }

    /// No-op: the resident mask is the authoritative state, not a cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        Ok(())
    }
}

#[allow(clippy::default_constructed_unit_structs)]
#[duplicate::duplicate_item(
    tests_mod       S               Fs              cfg_predicate;
    [tests_mmap]    [MmapFile]      [MmapFs]        [cfg(all())];
    [tests_uring]   [IoUringFile]   [IoUringFs]     [cfg(target_os = "linux")];
)]
#[cfg_predicate]
#[cfg(test)]
mod tests_mod {
    use common::universal_io::Populate;
    #[cfg_predicate]
    use common::universal_io::{Fs, S};
    use tempfile::TempDir;

    use crate::common::flags::compact_stored_flags::CompactStoredFlags;
    use crate::common::operation_error::OperationError;

    fn open(dir: &std::path::Path) -> CompactStoredFlags<S> {
        CompactStoredFlags::open(Fs::default(), dir, Populate::Blocking).unwrap()
    }

    #[test]
    fn open_creates_file_and_lists_it() {
        let dir = TempDir::new().unwrap();
        let flags = open(dir.path());
        assert_eq!(flags.len(), 0);
        assert_eq!(flags.count_flags(), 0);

        let files = flags.files();
        assert_eq!(files.len(), 1);
        assert!(files[0].exists()); // eagerly persisted empty mask
        assert!(files[0].starts_with(dir.path()));
    }

    #[test]
    fn set_flush_reopen_roundtrip() {
        let dir = TempDir::new().unwrap();
        {
            let flags = open(dir.path());
            assert!(!flags.set(3, true));
            assert!(!flags.set(100, true)); // grows to 101
            assert!(flags.set(3, true)); // previous value
            assert!(!flags.set(50, false));
            assert_eq!(flags.len(), 101);
            assert_eq!(flags.count_flags(), 2);
            flags.flusher()().unwrap();
        }
        {
            let flags = open(dir.path());
            assert_eq!(flags.len(), 101);
            assert_eq!(flags.count_flags(), 2);
            assert!(flags.get(3));
            assert!(flags.get(100));
            assert!(!flags.get(50));
        }
    }

    #[test]
    fn clean_flusher_skips_write() {
        let dir = TempDir::new().unwrap();
        let flags = open(dir.path());
        flags.set(1, true);
        flags.flusher()().unwrap();

        // A clean flush must not touch storage: with the file deleted behind
        // its back, only an actual write could bring it back.
        fs_err::remove_file(&flags.files()[0]).unwrap();
        flags.flusher()().unwrap();
        assert!(!flags.files()[0].exists());

        // The next effective change rewrites the whole mask.
        flags.set(2, true);
        flags.flusher()().unwrap();
        let reopened = open(dir.path());
        assert!(reopened.get(1));
        assert!(reopened.get(2));
    }

    #[test]
    fn flusher_flushes_call_time_state() {
        let dir = TempDir::new().unwrap();
        let flags = open(dir.path());
        flags.set(1, true);
        let flusher = flags.flusher();
        flags.set(2, true); // after flusher creation, before the flush
        flusher().unwrap();

        let reopened = open(dir.path());
        assert!(reopened.get(1));
        assert!(reopened.get(2));
    }

    #[test]
    fn flush_after_drop_is_cancelled() {
        let dir = TempDir::new().unwrap();
        let flags = open(dir.path());
        flags.set(1, true);
        let flusher = flags.flusher();
        drop(flags);
        assert!(matches!(flusher(), Err(OperationError::Cancelled { .. })));
    }

    #[test]
    fn set_len_grows_and_persists() {
        let dir = TempDir::new().unwrap();
        {
            let flags = open(dir.path());
            flags.set_len(10);
            assert_eq!(flags.len(), 10);
            assert_eq!(flags.count_flags(), 0);
            flags.flusher()().unwrap();
        }
        let flags = open(dir.path());
        assert_eq!(flags.len(), 10);
        assert_eq!(flags.count_flags(), 0);
    }
}
