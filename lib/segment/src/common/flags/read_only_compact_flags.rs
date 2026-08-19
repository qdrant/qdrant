use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use common::mmap::AdviceSetting;
use common::stored_bitmask::{self, StoredBitmask};
use common::universal_io::{
    CachedReadFs, OkNotFound, OkUnchanged, OpenOptions, Populate, ReadRange, UniversalRead,
    UniversalReadFs,
};
use roaring::RoaringBitmap;

use super::compact_stored_flags::COMPACT_FLAGS_FILE;
use super::roaring_flags::RoaringFlagsRead;
use crate::common::operation_error::OperationResult;

/// Read-only counterpart of [`CompactStoredFlags`][1], and thereby of the
/// compact [mode](super::FlagsMode) of the writable wrappers.
///
/// Materializes the persisted flags into an in-memory roaring bitmap on first
/// use — *not* on open, which reads only the fixed-size file header. The
/// backing [`StoredBitmask`] handle is kept for that lazy read and is replaced
/// wholesale by [`Self::live_reload`]. There is no write path: opening never
/// creates a missing file, unlike the writable [`CompactStoredFlags::open`][2].
///
/// [1]: super::compact_stored_flags::CompactStoredFlags
/// [2]: super::compact_stored_flags::CompactStoredFlags::open
pub struct ReadOnlyCompactFlags<S: UniversalRead> {
    /// In-memory bitmap of true flags, materialized from the backing file on
    /// first access and resynced by [`Self::live_reload`].
    ///
    /// Lazy so that opening the flags reads only the file header: a segment
    /// open would otherwise decode every flags file whole, which defeats
    /// prefetching only the bytes a query actually needs.
    ///
    /// [`OnceLock`] rather than a plain cell because the flags are queried
    /// through `&self` from many threads. On a race both threads may build a
    /// bitmap; the first to finish wins and the loser's copy is dropped.
    bitmap: OnceLock<RoaringBitmap>,
    /// Backing bitmask, used by the lazy [`Self::bitmap`] read. Replaced with
    /// a freshly opened handle on every [`Self::live_reload`].
    storage: StoredBitmask<S>,
    /// Total length of the flags, including trailing falses. Read from the
    /// bitmask header.
    len: usize,
    directory: PathBuf,
}

fn open_options(populate: Populate) -> OpenOptions {
    OpenOptions {
        writeable: false,
        need_sequential: true,
        populate,
        advice: AdviceSetting::Global,
    }
}

impl<S: UniversalRead> ReadOnlyCompactFlags<S> {
    /// Schedule background prefetch of the single file this storage reads.
    ///
    /// Returns whether the flags exist.
    pub fn preopen(
        fs: &impl CachedReadFs<File = S>,
        directory: &Path,
        mut populate: Populate,
    ) -> OperationResult<bool> {
        populate = match populate {
            Populate::Auto | Populate::No => {
                Populate::Partial(ReadRange::new(0, stored_bitmask::HEADER_SIZE as u64))
            }
            Populate::Blocking | Populate::PreferBackground => Populate::PreferBackground,
            partial @ Populate::Partial(_) => partial,
        };

        Ok(fs
            .schedule_prefetch(
                &directory.join(COMPACT_FLAGS_FILE),
                Some(open_options(populate)),
                None,
            )
            .ok_not_found()?
            .is_some())
    }

    /// Open persisted flags read-only, retaining the bitmask handle for the
    /// lazy [`RoaringFlagsRead::get_bitmap`].
    ///
    /// Returns [`Ok(None)`] when the flags file doesn't exist, matching the
    /// read path's never-create contract.
    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        directory: &Path,
    ) -> OperationResult<Option<Self>> {
        // A missing file means the flags aren't present on disk.
        let Some(storage) = StoredBitmask::<S>::open(
            fs,
            directory.join(COMPACT_FLAGS_FILE),
            open_options(Populate::No),
            Default::default(),
        )
        .ok_not_found()?
        else {
            return Ok(None);
        };

        Ok(Some(Self {
            bitmap: OnceLock::new(),
            len: storage.bit_len() as usize,
            storage,
            directory: directory.to_path_buf(),
        }))
    }

    /// The in-memory bitmap of set positions, decoding the backing file to
    /// build it on the first call and returning the cached one afterwards.
    ///
    /// This is the whole-file read that [`Self::open`] avoids. It is deferred
    /// to the first query rather than paid per segment open — many segments
    /// hold flag indexes that no query ever touches.
    fn bitmap(&self) -> OperationResult<&RoaringBitmap> {
        // `OnceLock::get_or_try_init` is still unstable, so build outside the
        // lock and let `get_or_init` arbitrate. A racing thread's bitmap is
        // simply dropped: both are built from the same bytes.
        if let Some(bitmap) = self.bitmap.get() {
            return Ok(bitmap);
        }

        let bitmap = self.storage.read_ones()?;
        Ok(self.bitmap.get_or_init(|| bitmap))
    }

    pub fn live_preload<Fs: CachedReadFs<File = S>>(&self, cached_fs: &Fs) -> OperationResult<()> {
        let directory = self.directory.as_path();
        let populate = if self.bitmap.get().is_some() {
            Populate::PreferBackground
        } else {
            Populate::Partial(ReadRange::new(0, stored_bitmask::HEADER_SIZE as u64))
        };
        cached_fs.reschedule_prefetch(
            &directory.join(COMPACT_FLAGS_FILE),
            Some(open_options(populate)),
            None,
        )?;
        Ok(())
    }

    /// Refresh to the current on-disk state.
    ///
    /// The compact file is never mutated in place — every flush replaces it
    /// whole — but the held handle keeps serving the bytes it was opened on,
    /// on caching backends forever. So a *fresh* handle is opened (a fresh
    /// open always mirrors the current remote bytes), the materialized
    /// bitmap, if any, is resynced from it, and it replaces the old handle.
    ///
    /// While the bitmap is still unmaterialized there is nothing to resync:
    /// the eventual first read decodes the fresh storage installed here.
    pub fn live_reload(&mut self, fs: &impl UniversalReadFs<File = S>) -> OperationResult<()> {
        // Once the flags exist their file always does, so absence here is a
        // genuine not-found (segment removed mid-reload), not a lazy file.
        let Some(storage) = StoredBitmask::<S>::open(
            fs,
            self.directory.join(COMPACT_FLAGS_FILE),
            open_options(Populate::No),
            Default::default(),
        )
        .ok_unchanged()?
        else {
            return Ok(());
        };

        if let Some(bitmap) = self.bitmap.get_mut() {
            *bitmap = storage.read_ones()?;
        }

        // The logical length grows as points are appended; refresh it so
        // length-driven readers stay correct.
        self.len = storage.bit_len() as usize;
        self.storage = storage;

        Ok(())
    }
}

impl<S: UniversalRead> RoaringFlagsRead for ReadOnlyCompactFlags<S> {
    fn len(&self) -> usize {
        self.len
    }

    fn get_bitmap(&self) -> OperationResult<&RoaringBitmap> {
        self.bitmap()
    }

    fn bitmap_if_materialized(&self) -> Option<&RoaringBitmap> {
        self.bitmap.get()
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![self.directory.join(COMPACT_FLAGS_FILE)]
    }
}

#[cfg(test)]
mod tests {
    #[cfg(not(windows))]
    use std::sync::Arc;

    #[cfg(not(windows))]
    use common::universal_io::{
        DiskCache, DiskCacheConfig, DiskCacheFs, DiskCacheFsContext, UniversalReadFileOps,
    };
    use common::universal_io::{MmapFile, MmapFs};
    use tempfile::Builder;

    use super::*;
    use crate::common::flags::compact_stored_flags::CompactStoredFlags;

    #[test]
    fn open_reads_persisted_flags_lazily_and_masks_missing() {
        let tmp = Builder::new().prefix("compact_read").tempdir().unwrap();
        let dir = tmp.path().join("flags");

        // Never-create contract: nothing on disk, nothing opened.
        assert!(
            ReadOnlyCompactFlags::<MmapFile>::open(&MmapFs, &dir)
                .unwrap()
                .is_none()
        );

        let writer = CompactStoredFlags::<MmapFile>::open(MmapFs, &dir, Populate::No).unwrap();
        writer.set(3, true);
        writer.set(9, false); // grows the flags to 10
        writer.flusher()().unwrap();

        let flags = ReadOnlyCompactFlags::<MmapFile>::open(&MmapFs, &dir)
            .unwrap()
            .unwrap();
        assert_eq!(flags.len(), 10);
        assert!(flags.bitmap_if_materialized().is_none()); // open decodes nothing

        assert!(flags.get(3).unwrap());
        assert!(!flags.get(9).unwrap());
        assert_eq!(flags.count_trues().unwrap(), 1);
        assert!(flags.bitmap_if_materialized().is_some());
        assert_eq!(flags.files(), vec![dir.join(COMPACT_FLAGS_FILE)]);
    }

    /// Every flush replaces the compact file whole, which the handle held by
    /// the reader on a caching backend never picks up. `live_reload` opens a
    /// fresh handle instead — this drives it over `DiskCacheFs`, where the
    /// stale-cache failure actually reproduces.
    ///
    /// Not run on Windows: the flush renames over a file the reader's disk
    /// cache keeps mapped, which Windows forbids. That limitation is specific
    /// to this setup — a local mmap file standing in as the "remote" — and
    /// the reload logic stays covered on the other targets.
    #[cfg(not(windows))]
    #[test]
    fn live_reload_over_disk_cache_sees_replaced_file() {
        let tmp = Builder::new().prefix("compact_reload").tempdir().unwrap();
        let remote_root = tmp.path().join("remote");
        let local_root = tmp.path().join("local");
        let dir = remote_root.join("flags");
        fs_err::create_dir_all(&dir).unwrap();
        fs_err::create_dir_all(&local_root).unwrap();

        // The writer works on the "remote" directly; the reader mirrors it
        // into `local_root` through the disk cache.
        let writer = CompactStoredFlags::<MmapFile>::open(MmapFs, &dir, Populate::No).unwrap();
        writer.set(5, true);
        writer.set_len(100);
        writer.flusher()().unwrap();

        let cache_fs = DiskCacheFs::<MmapFile>::from_context(DiskCacheFsContext {
            config: Arc::new(DiskCacheConfig::new(remote_root, local_root).unwrap()),
            remote: Default::default(),
        })
        .unwrap();
        let mut flags = ReadOnlyCompactFlags::<DiskCache<MmapFile>>::open(&cache_fs, &dir)
            .unwrap()
            .unwrap();

        // Materialize the bitmap: reads (and locally caches) the whole file —
        // the pre-write state this test must escape.
        assert!(flags.get_bitmap().unwrap().contains(5));
        assert_eq!(flags.len(), 100);

        // Replace the file behind the reader's back: new bits, a cleared bit,
        // and growth.
        writer.set(6, true);
        writer.set(50, true);
        writer.set(5, false);
        writer.set_len(120);
        writer.flusher()().unwrap();

        flags.live_reload(&cache_fs).unwrap();

        let bitmap = flags.get_bitmap().unwrap();
        assert!(bitmap.contains(6));
        assert!(bitmap.contains(50));
        assert!(
            !bitmap.contains(5),
            "cleared flag must not survive a reload"
        );
        assert_eq!(flags.len(), 120);
    }

    /// A bitmap that was never materialized needs no resync: the reload just
    /// swaps in the fresh handle, and the eventual first read decodes it.
    ///
    /// Not run on Windows: same rename-over-mapped-file limitation as
    /// `live_reload_over_disk_cache_sees_replaced_file`.
    #[cfg(not(windows))]
    #[test]
    fn live_reload_before_materialization_reads_fresh_state() {
        let tmp = Builder::new().prefix("compact_reload").tempdir().unwrap();
        let remote_root = tmp.path().join("remote");
        let local_root = tmp.path().join("local");
        let dir = remote_root.join("flags");
        fs_err::create_dir_all(&dir).unwrap();
        fs_err::create_dir_all(&local_root).unwrap();

        let writer = CompactStoredFlags::<MmapFile>::open(MmapFs, &dir, Populate::No).unwrap();
        writer.set(5, true);
        writer.flusher()().unwrap();

        let cache_fs = DiskCacheFs::<MmapFile>::from_context(DiskCacheFsContext {
            config: Arc::new(DiskCacheConfig::new(remote_root, local_root).unwrap()),
            remote: Default::default(),
        })
        .unwrap();
        let mut flags = ReadOnlyCompactFlags::<DiskCache<MmapFile>>::open(&cache_fs, &dir)
            .unwrap()
            .unwrap();

        // No `get_bitmap` here: the bitmap stays unmaterialized.
        writer.set(6, true);
        writer.flusher()().unwrap();

        flags.live_reload(&cache_fs).unwrap();

        let bitmap = flags.get_bitmap().unwrap();
        assert!(bitmap.contains(5));
        assert!(bitmap.contains(6));
    }
}
