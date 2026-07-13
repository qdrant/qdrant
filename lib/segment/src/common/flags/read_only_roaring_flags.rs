use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use common::mmap::AdviceSetting;
use common::stored_bitslice::StoredBitSlice;
use common::types::PointOffsetType;
use common::universal_io::{
    CachedReadFs, OkNotFound, OpenOptions, Populate, TypedStorage, UniversalRead, UniversalReadFs,
};
use roaring::RoaringBitmap;

use super::dynamic_stored_flags::{DynamicFlagsStatus, FLAGS_FILE, status_file};
use super::roaring_flags::RoaringFlagsRead;
use crate::common::operation_error::OperationResult;

/// Read-only counterpart of [`RoaringFlags`][1].
///
/// Materializes the persisted flags into an in-memory roaring bitmap on first
/// use — *not* on open, unlike the writable variant. The backing
/// [`StoredBitSlice`] handle is kept for that lazy scan and is replaced
/// wholesale by [`Self::live_reload`]. There is no write path: no buffer, no
/// [`BufferedDynamicFlags`][2], no [`DynamicStoredFlags`][3]. The retained `S`
/// is the one [`Self::open`] was called with, mirroring the other read-only
/// field indexes, which likewise hold their backing storage across reloads.
///
/// [1]: super::roaring_flags::RoaringFlags
/// [2]: super::buffered_dynamic_flags::BufferedDynamicFlags
/// [3]: super::dynamic_stored_flags::DynamicStoredFlags
pub struct ReadOnlyRoaringFlags<S: UniversalRead> {
    /// In-memory bitmap of true flags, materialized from the backing file on
    /// first access and resynced by [`Self::live_reload`].
    ///
    /// Lazy so that opening the flags reads only the (tiny) status file: a
    /// segment open would otherwise scan every flags file end to end, which
    /// defeats prefetching only the bytes a query actually needs.
    ///
    /// [`OnceLock`] rather than a plain cell because the index is queried
    /// through `&self` from many threads. On a race both threads may build a
    /// bitmap; the first to finish wins and the loser's copy is dropped.
    bitmap: OnceLock<RoaringBitmap>,
    /// Backing bitslice, used by the lazy [`Self::bitmap`] scan. Replaced with
    /// a freshly opened handle on every [`Self::live_reload`].
    storage: StoredBitSlice<S>,
    /// Total length of the flags, including trailing falses. Read from the status file.
    len: usize,
    directory: PathBuf,
}

fn open_options(populate: Populate) -> OpenOptions {
    OpenOptions {
        writeable: false,
        need_sequential: false,
        populate,
        advice: AdviceSetting::Global,
    }
}

/// Read the logical flag length from the status struct, opened read-only.
///
/// An absent status file propagates as not-found: whether that is legitimate
/// depends on the caller — [`ReadOnlyRoaringFlags::open`] masks it (the index
/// may simply not exist), a reload must not (the file existed before).
fn read_status_len<S: UniversalRead>(
    fs: &impl UniversalReadFs<File = S>,
    directory: &Path,
) -> OperationResult<usize> {
    let file = fs.open(
        status_file(directory),
        open_options(Populate::No),
        Default::default(),
    )?;
    let status = TypedStorage::<S, DynamicFlagsStatus>::new(file);
    Ok(status.read_whole()?[0].len())
}

impl<S: UniversalRead> ReadOnlyRoaringFlags<S> {
    /// Schedule background prefetch of the two files this storage reads.
    ///
    /// Returns whether the flag directory exists.
    pub fn preopen(
        fs: &impl CachedReadFs<File = S>,
        directory: &Path,
        populate: Populate,
    ) -> OperationResult<bool> {
        // Status file.
        if fs
            .schedule_prefetch(
                &status_file(directory),
                Some(open_options(Populate::PreferBackground)),
                None,
            )
            .ok_not_found()?
            .is_none()
        {
            return Ok(false);
        }

        // Bitslice
        fs.schedule_prefetch(
            &directory.join(FLAGS_FILE),
            Some(open_options(populate)),
            None,
        )?;

        Ok(true)
    }

    /// Open persisted flags read-only, retaining the bitslice handle for
    /// [`Self::bitmap`].
    ///
    /// Returns [`Ok(None)`] when the flag directory doesn't exist (the status
    /// file is absent), matching the read path's never-create contract.
    ///
    /// [1]: super::roaring_flags::RoaringFlags::new
    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        directory: &Path,
    ) -> OperationResult<Option<Self>> {
        // A missing status file means the index isn't present on disk.
        let Some(len) = read_status_len::<S>(fs, directory).ok_not_found()? else {
            return Ok(None);
        };

        let storage = StoredBitSlice::<S>::open(
            fs,
            directory.join(FLAGS_FILE),
            open_options(Populate::No),
            Default::default(),
        )?;

        Ok(Some(Self {
            bitmap: OnceLock::new(),
            storage,
            len,
            directory: directory.to_path_buf(),
        }))
    }

    /// The in-memory bitmap of set positions, scanning the flags file to build
    /// it on the first call and returning the cached one afterwards.
    ///
    /// This is the whole-file read that [`Self::open`] avoids. It is deferred
    /// to the first query rather than paid per segment open — many segments
    /// hold flag indexes (every payload field carries a null index) that no
    /// query ever touches.
    fn bitmap(&self) -> OperationResult<&RoaringBitmap> {
        // `OnceLock::get_or_try_init` is still unstable, so build outside the
        // lock and let `get_or_init` arbitrate. A racing thread's bitmap is
        // simply dropped: both are built from the same bytes.
        if let Some(bitmap) = self.bitmap.get() {
            return Ok(bitmap);
        }

        let bitmap = RoaringBitmap::from_sorted_iter(
            self.storage.iter_ones()?.map(|i| i as PointOffsetType),
        )
        .expect("iter_ones iterates in sorted order");

        Ok(self.bitmap.get_or_init(|| bitmap))
    }

    /// Refresh to the current on-disk state.
    ///
    /// Deliberately *not* an impl of [`LiveReload`][1]: this storage holds
    /// arbitrary flags with no notion of points, so a point-delta interface
    /// (deleted/new points) does not belong here — `open` never applied
    /// deleted points either. The on-disk flags are the sole source of truth.
    ///
    /// The flags file is preallocated (power-of-two capacity) and mutated in
    /// place within its length, which the held handle's `reopen()` — an
    /// append-only-growth contract — never picks up on caching backends. So
    /// instead a *fresh* handle is opened (a fresh open always mirrors the
    /// current remote bytes), the materialized bitmap, if any, is resynced
    /// from it, and it replaces the old handle.
    ///
    /// While the bitmap is still unmaterialized there is nothing to resync:
    /// the eventual first scan reads the fresh storage installed here.
    ///
    /// [1]: crate::common::live_reload::LiveReload
    pub fn live_reload(&mut self, fs: &impl UniversalReadFs<File = S>) -> OperationResult<()> {
        let storage = StoredBitSlice::<S>::open(
            fs,
            self.directory.join(FLAGS_FILE),
            open_options(Populate::No),
            Default::default(),
        )?;

        if let Some(bitmap) = self.bitmap.get_mut() {
            *bitmap =
                RoaringBitmap::from_sorted_iter(storage.iter_ones()?.map(|i| i as PointOffsetType))
                    .expect("iter_ones iterates in sorted order");
        }

        self.storage = storage;

        // The logical length grows as points are appended; refresh it so
        // length-driven readers (the null index's `iter_falses`) stay correct.
        // Once the index exists its status file always does, so absence here is
        // a genuine not-found (segment removed mid-reload), not a lazy file.
        self.len = read_status_len::<S>(fs, &self.directory)?;

        Ok(())
    }
}

impl<S: UniversalRead> RoaringFlagsRead for ReadOnlyRoaringFlags<S> {
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
        vec![
            status_file(&self.directory),
            self.directory.join(FLAGS_FILE),
        ]
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common::universal_io::{
        DiskCache, DiskCacheConfig, DiskCacheFs, DiskCacheFsContext, MmapFile, MmapFs,
        UniversalReadFileOps,
    };
    use tempfile::Builder;

    use super::*;
    use crate::common::flags::dynamic_stored_flags::DynamicStoredFlags;

    /// The flags file is preallocated (power-of-two capacity) and mutated in
    /// place within its length, so a reader over a caching backend cannot rely
    /// on `reopen()`: bits the writer changes inside already-cached blocks
    /// would stay stale forever. `live_reload` opens a fresh handle instead —
    /// this drives it over `DiskCacheFs`, where the stale-cache failure
    /// actually reproduces (mmap readers are read-through and can't catch it).
    #[test]
    fn live_reload_over_disk_cache_sees_in_place_bit_writes() {
        let tmp = Builder::new().prefix("roaring_reload").tempdir().unwrap();
        let remote_root = tmp.path().join("remote");
        let local_root = tmp.path().join("local");
        let dir = remote_root.join("flags");
        fs_err::create_dir_all(&dir).unwrap();
        fs_err::create_dir_all(&local_root).unwrap();

        // The writer works on the "remote" directly; the reader mirrors it
        // into `local_root` through the disk cache.
        let mut writer = DynamicStoredFlags::<MmapFile>::open(&MmapFs, &dir, Populate::No).unwrap();
        writer.set_len(&MmapFs, 100).unwrap();
        writer.set(5, true).unwrap();
        writer.flusher()().unwrap();

        let cache_fs = DiskCacheFs::<MmapFile>::from_context(DiskCacheFsContext {
            config: Arc::new(DiskCacheConfig::new(remote_root, local_root).unwrap()),
            remote: Default::default(),
        })
        .unwrap();
        let mut flags = ReadOnlyRoaringFlags::<DiskCache<MmapFile>>::open(&cache_fs, &dir)
            .unwrap()
            .unwrap();

        // Materialize the bitmap: scans (and locally caches) the whole
        // preallocated flags file — the pre-write state this test must escape.
        assert!(flags.get_bitmap().unwrap().contains(5));
        assert_eq!(flags.len(), 100);

        // In-place bit writes within the already-cached capacity, plus growth.
        writer.set(6, true).unwrap();
        writer.set(50, true).unwrap();
        writer.set(5, false).unwrap();
        writer.set_len(&MmapFs, 120).unwrap();
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
    /// swaps in the fresh handle, and the eventual first scan reads it.
    #[test]
    fn live_reload_before_materialization_scans_fresh_state() {
        let tmp = Builder::new().prefix("roaring_reload").tempdir().unwrap();
        let remote_root = tmp.path().join("remote");
        let local_root = tmp.path().join("local");
        let dir = remote_root.join("flags");
        fs_err::create_dir_all(&dir).unwrap();
        fs_err::create_dir_all(&local_root).unwrap();

        let mut writer = DynamicStoredFlags::<MmapFile>::open(&MmapFs, &dir, Populate::No).unwrap();
        writer.set_len(&MmapFs, 100).unwrap();
        writer.set(5, true).unwrap();
        writer.flusher()().unwrap();

        let cache_fs = DiskCacheFs::<MmapFile>::from_context(DiskCacheFsContext {
            config: Arc::new(DiskCacheConfig::new(remote_root, local_root).unwrap()),
            remote: Default::default(),
        })
        .unwrap();
        let mut flags = ReadOnlyRoaringFlags::<DiskCache<MmapFile>>::open(&cache_fs, &dir)
            .unwrap()
            .unwrap();

        // No `get_bitmap` here: the bitmap stays unmaterialized.
        writer.set(6, true).unwrap();
        writer.flusher()().unwrap();

        flags.live_reload(&cache_fs).unwrap();

        let bitmap = flags.get_bitmap().unwrap();
        assert!(bitmap.contains(5));
        assert!(bitmap.contains(6));
    }
}
