use std::assert_matches;
use std::borrow::Cow;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fs_err as fs;

use super::pipeline::DiskCachePipeline;
use super::{
    BLOCK_SIZE, DiskCache, DiskCacheConfig, DiskCacheFs, DiskCacheFsContext, DiskCacheRemote,
};
use crate::generic_consts::{Random, Sequential};
use crate::mmap::AdviceSetting;
use crate::universal_io::cached_fs::FileInfo;
use crate::universal_io::{
    CachedFs, CachedReadFs, MmapFile, OpenOptions, Populate, ReadPipeline, ReadRange,
    UniversalAppend, UniversalFlush, UniversalIoError, UniversalRead, UniversalReadFileOps,
    UniversalReadFs, UniversalWrite, UniversalWriteFileOps,
};

// The disk cache is strictly read-only: mutating it must stay a
// compile-time error, on top of writeable opens being rejected at runtime
// (covered per backend variant below). This holds at the filesystem level
// too — creating, removing and appending to files goes to the backing
// storage, never through the cache.
static_assertions::assert_not_impl_any!(
    DiskCache<MmapFile>: UniversalAppend, UniversalFlush, UniversalWrite
);
static_assertions::assert_not_impl_any!(DiskCacheFs<MmapFile>: UniversalWriteFileOps);

fn make_test_data(n_bytes: usize) -> Vec<u8> {
    (0..n_bytes).map(|i| (i % 251) as u8).collect()
}

struct Scenario {
    _tmp: tempfile::TempDir,
    remote_path: PathBuf,
    data: Vec<u8>,
    config: Arc<DiskCacheConfig>,
}

impl Scenario {
    fn new(n_bytes: usize) -> Self {
        let tmp = tempfile::Builder::new()
            .prefix("disk_cache_tests")
            .tempdir()
            .unwrap();
        let remote_dir = tmp.path().join("remote");
        let local_dir = tmp.path().join("local");
        fs::create_dir_all(&remote_dir).unwrap();
        fs::create_dir_all(&local_dir).unwrap();

        let remote_path = remote_dir.join("data.bin");
        let data = make_test_data(n_bytes);
        fs::write(&remote_path, &data).unwrap();

        Self {
            _tmp: tmp,
            remote_path,
            data,
            config: Arc::new(DiskCacheConfig::new(remote_dir, local_dir).unwrap()),
        }
    }

    /// Base mirror path for the remote; every open appends a unique suffix.
    fn local_path_base(&self) -> PathBuf {
        self.config.local_path_for(&self.remote_path).unwrap()
    }

    fn fs<R>(&self) -> DiskCacheFs<R>
    where
        R: DiskCacheRemote,
        <R::Fs as UniversalReadFileOps>::ContextConfig: Default,
    {
        DiskCacheFs::<R>::from_context(DiskCacheFsContext {
            config: self.config.clone(),
            remote: Default::default(),
        })
        .unwrap()
    }

    fn open<R>(&self, prefill: bool) -> DiskCache<R>
    where
        R: DiskCacheRemote,
        <R::Fs as UniversalReadFileOps>::ContextConfig: Default,
    {
        let populate = if prefill {
            Populate::PreferBackground
        } else {
            Populate::No
        };

        self.fs()
            .open(
                &self.remote_path,
                OpenOptions {
                    writeable: false,
                    populate,
                    need_sequential: false,
                    advice: AdviceSetting::Global,
                },
                Default::default(),
            )
            .unwrap()
    }

    /// Open with [`Populate::Partial`] over `range`, prefetching just that
    /// (block-aligned) byte range at open.
    fn open_partial<R>(&self, range: std::ops::Range<u64>) -> DiskCache<R>
    where
        R: DiskCacheRemote,
        <R::Fs as UniversalReadFileOps>::ContextConfig: Default,
    {
        let fs = DiskCacheFs::<R>::from_context(DiskCacheFsContext {
            config: self.config.clone(),
            remote: Default::default(),
        })
        .unwrap();
        fs.open(
            &self.remote_path,
            OpenOptions {
                writeable: false,
                populate: Populate::Partial(ReadRange::new(range.start, range.end - range.start)),
                need_sequential: false,
                advice: AdviceSetting::Global,
            },
            Default::default(),
        )
        .unwrap()
    }

    /// Slice of the remote data corresponding to `range`.
    fn slice(&self, range: &std::ops::Range<u64>) -> &[u8] {
        &self.data[range.start as usize..range.end as usize]
    }

    /// A `get_file_info` for `live_preload`, backed by a `CachedFs`
    /// listing snapshot taken now — the schedule-phase view of the remote.
    /// Take a fresh one after growing the remote, as a refresh pass would.
    fn snapshot_file_info<R>(&self) -> impl Fn(&Path) -> Option<FileInfo>
    where
        R: DiskCacheRemote,
        <R::Fs as UniversalReadFileOps>::ContextConfig: Default,
    {
        let mut cached_fs = CachedFs::new(self.fs::<R>(), &self.remote_path).unwrap();
        cached_fs.cache_file_info().unwrap();
        move |path| cached_fs.file_info(path).cloned()
    }

    /// Append `additional_bytes` bytes to the remote file in-place.
    /// Returns the full new remote contents.
    fn grow_remote(&mut self, additional_bytes: usize) -> Vec<u8> {
        use std::io::Write;

        let old_len = self.data.len();
        let new_data = make_test_data(old_len + additional_bytes);
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&self.remote_path)
            .unwrap();
        file.write_all(&new_data[old_len..]).unwrap();
        self.data = new_data.clone();
        new_data
    }
}

/// Drain `pipeline` until `wait` returns `None`, collecting results by user data.
fn drain_pipeline<R: DiskCacheRemote>(
    pipeline: &mut DiskCachePipeline<'_, R, u32>,
) -> HashMap<u32, Vec<u8>> {
    let mut results = HashMap::new();
    while let Some((user_data, bytes)) = pipeline.wait().unwrap() {
        let previous = results.insert(user_data, bytes.to_vec());
        assert!(previous.is_none(), "duplicate result for {user_data}");
    }
    results
}

#[duplicate::duplicate_item(
    tests_mod       R               cfg_predicate               _PREFILL;
    [tests_prefill] [MmapFile]      [cfg(all())]                [true];
    [tests_mmap]    [MmapFile]      [cfg(all())]                [false];
)]
#[cfg_predicate]
#[cfg(test)]
mod tests_mod {
    use std::sync::atomic::Ordering;

    use super::*;
    #[cfg_predicate]
    use crate::universal_io::R;
    use crate::universal_io::UioResult;

    const PREFILL: bool = _PREFILL;

    #[test]
    fn basic_read_returns_remote_bytes() {
        let scn = Scenario::new(BLOCK_SIZE * 3 + 100);
        let file = scn.open::<R>(PREFILL);

        // Read inside the first block.
        let bytes = file
            .read::<_, u8>(ReadRange::new(10, 20), Sequential)
            .unwrap();
        assert_eq!(&*bytes, &scn.data[10..30]);

        // Last block includes the 100-byte tail.
        let last = scn.data.len() as u64;
        let bytes = file
            .read::<_, u8>(ReadRange::new(last - 50, 50), Sequential)
            .unwrap();
        assert_eq!(&*bytes, &scn.data[scn.data.len() - 50..]);
    }

    /// `read_whole` on a zero-length remote must return an empty slice, not
    /// panic. The whole-object prefill schedules nothing for an empty file, so
    /// `init_from_open_prefill` resolves to `None` and falls back to a
    /// zero-length mirror (on io_uring `schedule_whole` returns without
    /// scheduling; on mmap it yields an empty read).
    #[test]
    fn read_whole_empty_remote_returns_empty() {
        let scn = Scenario::new(0);
        let file = scn.open::<R>(PREFILL);

        let bytes = file.read_whole::<u8>().unwrap();
        assert!(bytes.is_empty());
        assert_eq!(file.len::<u8>().unwrap(), 0);
    }

    #[test]
    fn read_spanning_multiple_blocks_is_contiguous() {
        let scn = Scenario::new(BLOCK_SIZE * 3 + 100);
        let file = scn.open::<R>(PREFILL);

        let start = (BLOCK_SIZE - 50) as u64;
        let len = (BLOCK_SIZE + 100) as u64;
        let bytes = file
            .read::<_, u8>(ReadRange::new(start, len), Sequential)
            .unwrap();
        let start = start as usize;
        let end = start + len as usize;
        assert_matches!(bytes, Cow::Borrowed(_));
        assert_eq!(bytes.as_ref(), &scn.data[start..end]);
    }

    #[test]
    fn local_file_is_created_on_first_read() {
        let scn = Scenario::new(BLOCK_SIZE * 2);

        let file = scn.open::<R>(PREFILL);
        let expected_local = file.local_path.clone();
        assert!(
            expected_local
                .to_str()
                .unwrap()
                .starts_with(scn.local_path_base().to_str().unwrap()),
            "unique mirror name must derive from the configured mapping",
        );

        // Before the first read, the local file doesn't exist yet.
        assert!(
            !expected_local.exists(),
            "local file should not exist before first read: {}",
            expected_local.display(),
        );

        // Trigger one read. This must bring up the local file.
        let _ = file.read::<_, u8>(ReadRange::one(0), Sequential).unwrap();

        assert!(
            expected_local.exists(),
            "local file should exist after first read"
        );
        assert_eq!(
            fs::metadata(&expected_local).unwrap().len(),
            scn.data.len() as u64,
            "local file should be sized to the remote",
        );
    }

    #[test]
    fn populate_fetches_every_block() {
        let scn = Scenario::new(BLOCK_SIZE * 3 + 100);
        let file = scn.open::<R>(PREFILL);

        file.populate().unwrap();

        let bytes = file
            .read::<_, u8>(ReadRange::new(0, scn.data.len() as u64), Sequential)
            .unwrap();
        assert_eq!(&*bytes, &scn.data[..]);
    }

    #[test]
    fn read_past_end_returns_out_of_bounds() {
        let scn = Scenario::new(1024);
        let file = scn.open::<R>(PREFILL);

        let err = file
            .read::<_, u8>(ReadRange::new(1000, 100), Sequential)
            .unwrap_err();
        assert_matches!(
            err,
            crate::universal_io::UniversalIoError::OutOfBounds { .. },
        );
    }

    /// Two live instances for the same remote path must not share a mirror:
    /// each open gets a unique local name, so the second open cannot truncate
    /// the first instance's mirror out from under it. This is what makes
    /// refresh-by-fresh-open (live-reload) safe while the old handle is alive.
    #[test]
    fn concurrent_instances_have_independent_mirrors() {
        let scn = Scenario::new(BLOCK_SIZE * 2);

        let first = scn.open::<R>(PREFILL);
        let read_all = |cache: &DiskCache<R>| {
            cache
                .read::<_, u8>(ReadRange::new(0, scn.data.len() as u64), Sequential)
                .unwrap()
                .to_vec()
        };
        assert_eq!(read_all(&first), scn.data);

        let second = scn.open::<R>(PREFILL);
        assert_ne!(first.local_path, second.local_path);
        assert_eq!(read_all(&second), scn.data);

        // The first instance's mirror survived the second open.
        assert_eq!(read_all(&first), scn.data);
    }

    /// Dropping an instance removes its mirror file: names are unique per
    /// open, so a leftover would never be reused.
    #[test]
    fn drop_removes_local_mirror() {
        let scn = Scenario::new(BLOCK_SIZE);
        let cache = scn.open::<R>(PREFILL);

        let _ = cache.read::<_, u8>(ReadRange::one(0), Sequential).unwrap();

        let local_path = cache.local_path.clone();
        assert!(local_path.exists());

        drop(cache);
        assert!(!local_path.exists());
    }

    /// Reopen on an unchanged remote must not resize, repopulate, or mutate
    /// the fetched bitmap.
    #[test]
    fn reopen_no_growth_does_not_repopulate() {
        let scn = Scenario::new(BLOCK_SIZE * 3);
        let mut cache = scn.open::<R>(PREFILL);

        let _ = cache.read::<_, u8>(ReadRange::one(0), Sequential).unwrap();

        let (len_before, populated_before, fetched_before) = {
            let local = cache.state().expect("local initialized after read").local;
            (
                local.mmap().len::<u8>().unwrap(),
                local.fully_populated.load(Ordering::Acquire),
                local.fetched.lock().clone(),
            )
        };

        cache.live_reload().unwrap();

        let local = if PREFILL {
            // in case of Populate::PreferBackground, we need to await for
            // completion to get the local_state back.
            cache.state().unwrap().local
        } else {
            // in case of Populate::No, local_state should still be there
            // without forcing (re)initialization.
            assert!(cache.is_ready(), "local must still be initialized");
            cache.state().unwrap().local
        };

        assert_eq!(local.mmap().len::<u8>().unwrap(), len_before);
        assert_eq!(
            local.fully_populated.load(Ordering::Acquire),
            populated_before,
        );
        assert_eq!(local.fetched.lock().clone(), fetched_before);
    }

    /// Reads into the new section must fail before reopen (local mirror is at
    /// the old length) and succeed after reopen.
    #[test]
    fn reopen_growth_visible_after_reopen() {
        let mut scn = Scenario::new(BLOCK_SIZE * 2);
        let mut cache = scn.open::<R>(PREFILL);

        let original_len = scn.data.len() as u64;

        let _ = cache.read::<_, u8>(ReadRange::one(0), Sequential).unwrap();

        let new_data = scn.grow_remote(BLOCK_SIZE);

        let err = cache
            .read::<_, u8>(ReadRange::new(original_len, BLOCK_SIZE as u64), Sequential)
            .unwrap_err();
        assert_matches!(
            err,
            crate::universal_io::UniversalIoError::OutOfBounds { .. },
        );

        cache.live_reload().unwrap();

        let bytes = cache
            .read::<_, u8>(ReadRange::new(original_len, BLOCK_SIZE as u64), Sequential)
            .unwrap();
        assert_eq!(&*bytes, &new_data[original_len as usize..]);
    }

    /// When the remote grows and the original tail block was only partially
    /// populated, reopen must invalidate that block so the next read re-fetches
    /// it instead of returning the zero-filled bytes left by `set_len`.
    #[test]
    fn reopen_growth_refetches_partial_tail_block() {
        // Non-block-aligned remote: block 1 holds only 100 real bytes.
        let mut scn = Scenario::new(BLOCK_SIZE + 100);
        let mut cache = scn.open::<R>(PREFILL);

        // Touch the partial tail so block 1 ends up in the `fetched` bitmap
        // (its fetch is clamped to the old EOF).
        let _ = cache
            .read::<_, u8>(ReadRange::one(BLOCK_SIZE as u64), Sequential)
            .unwrap();

        // Grow remote past the old tail block boundary.
        let new_data = scn.grow_remote(BLOCK_SIZE);

        cache.live_reload().unwrap();

        // Read covers both the originally-partial range [BLOCK_SIZE..old_len)
        // and the newly-grown tail [old_len..BLOCK_SIZE*2). Without the
        // invalidation, the second half would be zeros from `set_len`.
        let bytes = cache
            .read::<_, u8>(
                ReadRange::new(BLOCK_SIZE as u64, BLOCK_SIZE as u64),
                Sequential,
            )
            .unwrap();
        assert_eq!(&*bytes, &new_data[BLOCK_SIZE..BLOCK_SIZE * 2]);
    }

    /// Staging must be invisible to readers: the mirror keeps its old length
    /// (so components keep `len()` as their growth signal) until `reopen`
    /// applies the staged work.
    #[test]
    fn reopen_schedule_is_invisible_until_reopen() {
        let mut scn = Scenario::new(BLOCK_SIZE * 2);
        let mut cache = scn.open::<R>(PREFILL);

        let original_len = scn.data.len() as u64;
        let _ = cache.read::<_, u8>(ReadRange::one(0), Sequential).unwrap();

        let new_data = scn.grow_remote(BLOCK_SIZE);
        let _ = cache.live_preload(scn.snapshot_file_info::<R>());

        // Nothing changed yet: same length, and the appended region is still
        // out of bounds.
        assert_eq!(cache.len::<u8>().unwrap(), original_len);
        let err = cache
            .read::<_, u8>(ReadRange::new(original_len, BLOCK_SIZE as u64), Sequential)
            .unwrap_err();
        assert_matches!(err, UniversalIoError::OutOfBounds { .. });

        cache.live_reload().unwrap();

        assert_eq!(cache.len::<u8>().unwrap(), new_data.len() as u64);
        // A populated cache staged the tail fetch, so the appended block is
        // already local; a lazy one only learned the new length.
        let new_block = (original_len / BLOCK_SIZE as u64) as u32;
        assert_eq!(
            cache
                .state()
                .unwrap()
                .local
                .contains(new_block..new_block + 1),
            PREFILL,
        );

        let bytes = cache
            .read::<_, u8>(ReadRange::new(original_len, BLOCK_SIZE as u64), Sequential)
            .unwrap();
        assert_eq!(&*bytes, &new_data[original_len as usize..]);
    }

    /// Staging against a cold cache materializes the mirror at the known
    /// length, and applying it changes nothing.
    #[test]
    fn reopen_schedule_materializes_cold_mirror() {
        let scn = Scenario::new(BLOCK_SIZE * 2 + 100);
        let mut cache = scn.open::<R>(PREFILL);
        assert!(!cache.is_ready());

        let _ = cache.live_preload(scn.snapshot_file_info::<R>());

        assert!(cache.is_ready());
        assert_eq!(cache.len::<u8>().unwrap(), scn.data.len() as u64);

        // A later reopen (nothing staged — the length didn't grow) must
        // leave the mirror alone.
        cache.live_reload().unwrap();
        assert_eq!(cache.len::<u8>().unwrap(), scn.data.len() as u64);

        let bytes = cache.read_whole::<u8>().unwrap();
        assert_eq!(&*bytes, &scn.data[..]);
    }

    /// Scheduling an unchanged length stages nothing; the schedule/apply pair
    /// must neither resize nor invalidate.
    #[test]
    fn reopen_schedule_no_growth_does_not_repopulate() {
        let scn = Scenario::new(BLOCK_SIZE * 3);
        let mut cache = scn.open::<R>(PREFILL);

        let _ = cache.read::<_, u8>(ReadRange::one(0), Sequential).unwrap();
        let (len_before, fetched_before) = {
            let local = cache.state().unwrap().local;
            (
                local.mmap().len::<u8>().unwrap(),
                local.fetched.lock().clone(),
            )
        };

        let _ = cache.live_preload(scn.snapshot_file_info::<R>());
        cache.live_reload().unwrap();

        let local = cache.state().unwrap().local;
        assert_eq!(local.mmap().len::<u8>().unwrap(), len_before);
        assert_eq!(*local.fetched.lock(), fetched_before);
    }

    /// Two schedules without an apply in between: the second supersedes the
    /// first, so one `reopen` lands all the growth.
    #[test]
    fn reopen_schedule_twice_without_apply() {
        let mut scn = Scenario::new(BLOCK_SIZE * 2);
        let mut cache = scn.open::<R>(PREFILL);

        let _ = cache.read::<_, u8>(ReadRange::one(0), Sequential).unwrap();

        scn.grow_remote(BLOCK_SIZE);
        let _ = cache.live_preload(scn.snapshot_file_info::<R>());
        let new_data = scn.grow_remote(BLOCK_SIZE);
        let _ = cache.live_preload(scn.snapshot_file_info::<R>());

        cache.live_reload().unwrap();

        assert_eq!(cache.len::<u8>().unwrap(), new_data.len() as u64);
        let bytes = cache.read_whole::<u8>().unwrap();
        assert_eq!(&*bytes, &new_data[..]);
    }

    /// Re-scheduling the same length keeps the first staging as is — the
    /// staged tail's in-flight read must survive to be applied.
    #[test]
    fn reopen_schedule_twice_with_same_length() {
        let mut scn = Scenario::new(BLOCK_SIZE * 2);
        let mut cache = scn.open::<R>(PREFILL);

        let _ = cache.read::<_, u8>(ReadRange::one(0), Sequential).unwrap();

        let new_data = scn.grow_remote(BLOCK_SIZE);
        let get_file_info = scn.snapshot_file_info::<R>();

        let _ = cache.live_preload(&get_file_info);
        let _ = cache.live_preload(&get_file_info);

        cache.live_reload().unwrap();

        assert_eq!(cache.len::<u8>().unwrap(), new_data.len() as u64);
        let bytes = cache.read_whole::<u8>().unwrap();
        assert_eq!(&*bytes, &new_data[..]);
    }

    /// Scheduling against a snapshot that does not cover the file fails with
    /// `NotFound` — the file resolves its own remote path, so there is no
    /// path argument to mispair.
    #[test]
    fn reopen_schedule_missing_from_snapshot_errors() {
        let scn = Scenario::new(BLOCK_SIZE);
        let cache = scn.open::<R>(PREFILL);

        // `map(drop)` discards the staged future, which is not `Debug`.
        let err = cache.live_preload(|_| None).map(drop).unwrap_err();
        assert_matches!(err, UniversalIoError::NotFound { .. });
    }

    /// `Populate::Partial` prefetches only the requested (block-aligned) range;
    /// blocks outside it stay unfetched until read, then fault in lazily.
    #[test]
    fn partial_populate_fetches_only_requested_range() {
        // 4 blocks: 0, 1, 2, and a partial tail block 3.
        let scn = Scenario::new(BLOCK_SIZE * 3 + 100);
        // Request blocks 0 and 1 (range spills 50 bytes into block 1).
        let file = scn.open_partial::<R>(0..(BLOCK_SIZE as u64 + 50));

        // The mirror is materialized lazily; nothing exists before the first read.
        let expected_local = file.local_path.clone();
        assert!(!expected_local.exists());
        assert!(!file.is_ready());

        // A read within the prefetched range is served from the local mirror.
        let bytes = file
            .read::<_, u8>(ReadRange::new(10, 20), Sequential)
            .unwrap();
        assert_eq!(&*bytes, &scn.data[10..30]);

        // The mirror now exists, and exactly the requested blocks {0, 1} are
        // cached — not the whole file, proving the populate was partial.
        assert!(expected_local.exists());
        {
            let local = file.state().unwrap().local;
            assert!(!local.fully_populated.load(Ordering::Acquire));
            assert!(local.fetched.lock().contains_range(0..2));
            assert!(!local.fetched.lock().contains(2));
            assert!(!local.fetched.lock().contains(3));
        }

        // A read outside the prefetched range faults its block in on demand.
        let start = (BLOCK_SIZE * 2) as u64;
        let bytes = file
            .read::<_, u8>(ReadRange::new(start, 30), Sequential)
            .unwrap();
        assert_eq!(&*bytes, &scn.data[start as usize..start as usize + 30]);
        assert!(file.state().unwrap().local.fetched.lock().contains(2));
    }

    /// An empty `Populate::Partial` range prefetches nothing but still opens a
    /// correctly-sized mirror that serves reads by faulting blocks in lazily.
    #[test]
    fn partial_populate_empty_range_is_lazy() {
        let scn = Scenario::new(BLOCK_SIZE * 2 + 100);
        let file = scn.open_partial::<R>(10..10);

        // Nothing prefetched, so the first read must fault its block in.
        let bytes = file
            .read::<_, u8>(ReadRange::new(0, 16), Sequential)
            .unwrap();
        assert_eq!(&*bytes, &scn.data[0..16]);
        assert_eq!(file.len::<u8>().unwrap(), scn.data.len() as u64);
    }

    /// A `Populate::Partial` range starting past EOF has nothing valid to
    /// prefetch; the mirror is still sized correctly and serves reads lazily.
    #[test]
    fn partial_populate_range_past_eof_is_lazy() {
        let scn = Scenario::new(100);
        let file = scn.open_partial::<R>(BLOCK_SIZE as u64 * 4..BLOCK_SIZE as u64 * 5);

        let bytes = file
            .read::<_, u8>(ReadRange::new(0, 100), Sequential)
            .unwrap();
        assert_eq!(&*bytes, &scn.data[..]);
        assert_eq!(file.len::<u8>().unwrap(), 100);
    }

    #[test]
    fn same_block_reads_share_one_fetch() {
        let scn = Scenario::new(BLOCK_SIZE * 3 + 100);
        let file = scn.open::<R>(false);

        let mut pipeline = DiskCachePipeline::<R, u32>::new().unwrap();
        pipeline.schedule::<Random>(0, &file, 10..30, 1).unwrap();
        // Same block as above: piggybacks even if the remote queue is full.
        pipeline.schedule::<Random>(1, &file, 100..200, 1).unwrap();
        assert_eq!(pipeline.in_flight_fetches(), 1);

        let results = drain_pipeline(&mut pipeline);
        assert_eq!(results.len(), 2);
        assert_eq!(results[&0], &scn.data[10..30]);
        assert_eq!(results[&1], &scn.data[100..200]);
    }

    #[test]
    fn spanning_fetch_covers_contained_reads() {
        let scn = Scenario::new(BLOCK_SIZE * 3 + 100);
        let file = scn.open::<R>(false);

        let mut pipeline = DiskCachePipeline::<R, u32>::new().unwrap();
        // Spans blocks 0..2.
        let spanning = (BLOCK_SIZE - 50) as u64..(BLOCK_SIZE + 50) as u64;
        // Contained in block 1.
        let contained = (BLOCK_SIZE + 100) as u64..(BLOCK_SIZE + 200) as u64;
        pipeline
            .schedule::<Random>(0, &file, spanning.clone(), 1)
            .unwrap();
        pipeline
            .schedule::<Random>(1, &file, contained.clone(), 1)
            .unwrap();
        assert_eq!(pipeline.in_flight_fetches(), 1);

        let results = drain_pipeline(&mut pipeline);
        assert_eq!(results[&0], scn.slice(&spanning));
        assert_eq!(results[&1], scn.slice(&contained));
    }

    /// Piggybacked reads may themselves span multiple blocks, as long as the
    /// in-flight fetch fully covers them — including reads reaching into the
    /// EOF-clamped partial tail block.
    #[test]
    fn multi_block_reads_share_one_fetch() {
        // Three full blocks plus a 100-byte partial tail block.
        let scn = Scenario::new(BLOCK_SIZE * 3 + 100);
        let file = scn.open::<R>(false);
        let eof = scn.data.len() as u64;

        let mut pipeline = DiskCachePipeline::<R, u32>::new().unwrap();
        // Spans all four blocks; the fetch's byte range is clamped to EOF.
        let spanning = 100u64..eof - 10;
        // Crosses the block 1/2 boundary.
        let middle = (BLOCK_SIZE + 200) as u64..(BLOCK_SIZE * 2 + 200) as u64;
        // Reaches the partial tail block, ending exactly at EOF.
        let tail = (BLOCK_SIZE * 3 - 50) as u64..eof;
        pipeline
            .schedule::<Random>(0, &file, spanning.clone(), 1)
            .unwrap();
        pipeline
            .schedule::<Random>(1, &file, middle.clone(), 1)
            .unwrap();
        pipeline
            .schedule::<Random>(2, &file, tail.clone(), 1)
            .unwrap();
        assert_eq!(pipeline.in_flight_fetches(), 1);

        let results = drain_pipeline(&mut pipeline);
        assert_eq!(results[&0], scn.slice(&spanning));
        assert_eq!(results[&1], scn.slice(&middle));
        assert_eq!(results[&2], scn.slice(&tail));
    }

    /// A read only partially covered by an in-flight fetch must not piggyback
    /// on it: it goes to the remote queue like any other fetch — never
    /// resolving against blocks the in-flight fetch doesn't cover.
    #[test]
    fn partially_covered_read_does_not_piggyback() {
        let scn = Scenario::new(BLOCK_SIZE * 3 + 100);
        let file = scn.open::<R>(false);

        let mut pipeline = DiskCachePipeline::<R, u32>::new().unwrap();
        // Fetch covers blocks 0..2.
        let first = (BLOCK_SIZE - 50) as u64..(BLOCK_SIZE + 50) as u64;
        // Needs blocks 1..3: block 2 is not covered by the fetch above.
        let second = (BLOCK_SIZE + 100) as u64..(BLOCK_SIZE * 2 + 100) as u64;
        pipeline
            .schedule::<Random>(0, &file, first.clone(), 1)
            .unwrap();

        let mut results = HashMap::new();
        match pipeline.schedule::<Random>(1, &file, second.clone(), 1) {
            // Queued backends: a separate fetch was scheduled.
            Ok(()) => assert_eq!(pipeline.in_flight_fetches(), 2),
            // Single-slot backends (mmap remote): the read went for the remote
            // queue and found it full — either way, it did not piggyback.
            Err(UniversalIoError::QueueIsFull) => {
                assert_eq!(pipeline.in_flight_fetches(), 1);
                // Free the queue and retry; the retried read must still fetch,
                // as block 2 is not local even after the first fetch commits.
                results.extend(drain_pipeline(&mut pipeline));
                pipeline
                    .schedule::<Random>(1, &file, second.clone(), 1)
                    .unwrap();
                assert_eq!(pipeline.in_flight_fetches(), 1);
            }
            Err(err) => panic!("unexpected error: {err}"),
        }

        results.extend(drain_pipeline(&mut pipeline));
        assert_eq!(results[&0], scn.slice(&first));
        assert_eq!(results[&1], scn.slice(&second));
    }

    /// Reads on different files never share a fetch, even for identical ranges.
    #[test]
    fn different_files_do_not_share_fetches() {
        let scn = Scenario::new(BLOCK_SIZE * 2);
        let file_a = scn.open::<R>(false);
        let file_b = scn.open::<R>(false);

        let mut pipeline = DiskCachePipeline::<R, u32>::new().unwrap();
        pipeline.schedule::<Random>(0, &file_a, 10..30, 1).unwrap();
        match pipeline.schedule::<Random>(1, &file_b, 10..30, 1) {
            Ok(()) => assert_eq!(pipeline.in_flight_fetches(), 2),
            // Single-slot backends: the identical range on another file went
            // to the remote queue instead of piggybacking on `file_a`'s fetch.
            Err(UniversalIoError::QueueIsFull) => {
                assert_eq!(pipeline.in_flight_fetches(), 1);
            }
            Err(err) => panic!("unexpected error: {err}"),
        }

        let results = drain_pipeline(&mut pipeline);
        assert_eq!(results[&0], &scn.data[10..30]);
    }

    /// Once a fetch commits its blocks to the mirror, later reads of those
    /// blocks are served locally without scheduling another fetch.
    #[test]
    fn committed_blocks_serve_later_reads_locally() {
        let scn = Scenario::new(BLOCK_SIZE * 2);
        let file = scn.open::<R>(false);

        let mut pipeline = DiskCachePipeline::<R, u32>::new().unwrap();
        pipeline.schedule::<Random>(0, &file, 10..30, 1).unwrap();
        let results = drain_pipeline(&mut pipeline);
        assert_eq!(results[&0], &scn.data[10..30]);

        pipeline.schedule::<Random>(1, &file, 40..60, 1).unwrap();
        // Served locally: no remote fetch in flight.
        assert_eq!(pipeline.in_flight_fetches(), 0);
        let results = drain_pipeline(&mut pipeline);
        assert_eq!(results[&1], &scn.data[40..60]);
    }

    /// Multiple pipelines on different threads reading the same remote blocks
    /// piggyback on a single remote fetch.
    #[test]
    fn cross_pipeline_reads_share_one_fetch() {
        let scn = Scenario::new(BLOCK_SIZE * 3 + 100);
        let file = Arc::new(scn.open::<R>(false));
        let file_clone = file.clone();
        let expected_1 = scn.data[10..50].to_vec();
        let expected_2 = scn.data[20..40].to_vec();

        let (t1_ready_tx, t1_ready_rx) = std::sync::mpsc::channel();
        let (t2_scheduled_tx, t2_scheduled_rx) = std::sync::mpsc::channel();

        let handle1 = std::thread::spawn(move || {
            let mut pipeline = DiskCachePipeline::<R, u32>::new().unwrap();
            pipeline.schedule::<Random>(1, &file, 10..50, 1).unwrap();
            assert_eq!(pipeline.in_flight_fetches(), 1);

            t1_ready_tx.send(()).unwrap();
            t2_scheduled_rx.recv().unwrap();

            let results = drain_pipeline(&mut pipeline);
            assert_eq!(results[&1], expected_1);
        });

        let handle2 = std::thread::spawn(move || {
            t1_ready_rx.recv().unwrap();
            let mut pipeline = DiskCachePipeline::<R, u32>::new().unwrap();
            pipeline
                .schedule::<Random>(2, &file_clone, 20..40, 1)
                .unwrap();
            assert_eq!(pipeline.in_flight_fetches(), 0);

            t2_scheduled_tx.send(()).unwrap();

            let results = drain_pipeline(&mut pipeline);
            assert_eq!(results[&2], expected_2);
        });

        handle1.join().unwrap();
        handle2.join().unwrap();
    }

    /// A piggybacked pipeline calling wait() before the leader completes
    /// blocks until the leader commits the remote fetch.
    #[test]
    fn cross_pipeline_wait_before_leader_completes() {
        let scn = Scenario::new(BLOCK_SIZE * 2);
        let file = Arc::new(scn.open::<R>(false));
        let file_clone = file.clone();
        let expected_1 = scn.data[10..50].to_vec();
        let expected_2 = scn.data[20..40].to_vec();

        let (t1_scheduled_tx, t1_scheduled_rx) = std::sync::mpsc::channel();
        let (t2_waiting_tx, t2_waiting_rx) = std::sync::mpsc::channel();

        let handle1 = std::thread::spawn(move || {
            let mut pipeline = DiskCachePipeline::<R, u32>::new().unwrap();
            pipeline.schedule::<Random>(1, &file, 10..50, 1).unwrap();
            assert_eq!(pipeline.in_flight_fetches(), 1);

            t1_scheduled_tx.send(()).unwrap();
            t2_waiting_rx.recv().unwrap();
            std::thread::sleep(std::time::Duration::from_millis(10));

            let results = drain_pipeline(&mut pipeline);
            assert_eq!(results[&1], expected_1);
        });

        let handle2 = std::thread::spawn(move || {
            t1_scheduled_rx.recv().unwrap();
            let mut pipeline = DiskCachePipeline::<R, u32>::new().unwrap();
            pipeline
                .schedule::<Random>(2, &file_clone, 20..40, 1)
                .unwrap();
            assert_eq!(pipeline.in_flight_fetches(), 0);

            t2_waiting_tx.send(()).unwrap();
            let results = drain_pipeline(&mut pipeline);
            assert_eq!(results[&2], expected_2);
        });

        handle1.join().unwrap();
        handle2.join().unwrap();
    }

    /// When the leader pipeline drops without completing, the waiting follower is
    /// promoted to leader, retries the fetch, and succeeds.
    #[test]
    fn cross_pipeline_leader_abandons_follower_promotes_and_succeeds() {
        let scn = Scenario::new(BLOCK_SIZE * 2);
        let file = Arc::new(scn.open::<R>(false));
        let file_clone = file.clone();
        let expected_2 = scn.data[20..40].to_vec();

        let (t1_scheduled_tx, t1_scheduled_rx) = std::sync::mpsc::channel();
        let (t2_waiting_tx, t2_waiting_rx) = std::sync::mpsc::channel();

        let handle1 = std::thread::spawn(move || {
            let mut pipeline = DiskCachePipeline::<R, u32>::new().unwrap();
            pipeline.schedule::<Random>(1, &file, 10..50, 1).unwrap();
            assert_eq!(pipeline.in_flight_fetches(), 1);

            t1_scheduled_tx.send(()).unwrap();
            t2_waiting_rx.recv().unwrap();
            std::thread::sleep(std::time::Duration::from_millis(10));
            drop(pipeline);
        });

        let handle2 = std::thread::spawn(move || {
            t1_scheduled_rx.recv().unwrap();
            let mut pipeline = DiskCachePipeline::<R, u32>::new().unwrap();
            pipeline
                .schedule::<Random>(2, &file_clone, 20..40, 1)
                .unwrap();
            assert_eq!(pipeline.in_flight_fetches(), 0);

            t2_waiting_tx.send(()).unwrap();
            let results = drain_pipeline(&mut pipeline);
            assert_eq!(results[&2], expected_2);
        });

        handle1.join().unwrap();
        handle2.join().unwrap();
    }

    /// When leader drops and first promoted follower also drops, the succession
    /// chain continues to the next waiting follower.
    #[test]
    fn cross_pipeline_multi_follower_chain_succession() {
        let scn = Scenario::new(BLOCK_SIZE * 2);
        let file = Arc::new(scn.open::<R>(false));
        let file_2 = file.clone();
        let file_3 = file.clone();
        let expected_3 = scn.data[30..45].to_vec();

        let (t1_sched_tx, t1_sched_rx) = std::sync::mpsc::channel();
        let (t2_wait_tx, t2_wait_rx) = std::sync::mpsc::channel();
        let (t3_wait_tx, t3_wait_rx) = std::sync::mpsc::channel();

        let handle1 = std::thread::spawn(move || {
            let mut pipeline = DiskCachePipeline::<R, u32>::new().unwrap();
            pipeline.schedule::<Random>(1, &file, 10..50, 1).unwrap();
            t1_sched_tx.send(()).unwrap();
            t2_wait_rx.recv().unwrap();
            t3_wait_rx.recv().unwrap();
            std::thread::sleep(std::time::Duration::from_millis(10));
            drop(pipeline);
        });

        let handle2 = std::thread::spawn(move || {
            t1_sched_rx.recv().unwrap();
            let mut pipeline = DiskCachePipeline::<R, u32>::new().unwrap();
            pipeline.schedule::<Random>(2, &file_2, 20..40, 1).unwrap();
            t2_wait_tx.send(()).unwrap();

            // Enters wait, gets promoted when handle1 drops, but drops pipeline without completing
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = pipeline.wait();
            }));
        });

        let handle3 = std::thread::spawn(move || {
            let mut pipeline = DiskCachePipeline::<R, u32>::new().unwrap();
            pipeline.schedule::<Random>(3, &file_3, 30..45, 1).unwrap();
            t3_wait_tx.send(()).unwrap();

            let results = drain_pipeline(&mut pipeline);
            assert_eq!(results[&3], expected_3);
        });

        handle1.join().unwrap();
        let _ = handle2.join();
        handle3.join().unwrap();
    }

    /// When the leader drops before the follower enters wait(), the follower
    /// detects abandonment upon calling wait(), promotes itself, and succeeds.
    #[test]
    fn follower_calls_wait_after_leader_dropped_promotes_and_succeeds() {
        let scn = Scenario::new(BLOCK_SIZE * 2);
        let file = Arc::new(scn.open::<R>(false));
        let file_clone = file.clone();
        let expected_2 = scn.data[20..40].to_vec();

        let (t1_sched_tx, t1_sched_rx) = std::sync::mpsc::channel();
        let (t2_sched_tx, t2_sched_rx) = std::sync::mpsc::channel();

        let handle1 = std::thread::spawn(move || {
            let mut pipeline1 = DiskCachePipeline::<R, u32>::new().unwrap();
            pipeline1.schedule::<Random>(1, &file, 10..50, 1).unwrap();
            assert_eq!(pipeline1.in_flight_fetches(), 1);
            t1_sched_tx.send(()).unwrap();
            t2_sched_rx.recv().unwrap();
            // Leader drops BEFORE follower enters wait()!
            drop(pipeline1);
        });

        let handle2 = std::thread::spawn(move || {
            t1_sched_rx.recv().unwrap();
            let mut pipeline2 = DiskCachePipeline::<R, u32>::new().unwrap();
            pipeline2
                .schedule::<Random>(2, &file_clone, 20..40, 1)
                .unwrap();
            assert_eq!(pipeline2.in_flight_fetches(), 0);
            t2_sched_tx.send(()).unwrap();

            // Give thread 1 time to drop
            std::thread::sleep(std::time::Duration::from_millis(20));

            // Follower calls wait() after abandonment: it should self-promote and succeed!
            let results = drain_pipeline(&mut pipeline2);
            assert_eq!(results[&2], expected_2);
        });

        handle1.join().unwrap();
        handle2.join().unwrap();
    }

    /// When a leader hangs or is too slow, piggybacking followers time out
    /// and fetch the requested blocks independently without waiting indefinitely.
    #[test]
    fn follower_times_out_and_fetches_independently_when_leader_hangs() {
        let scn = Scenario::new(BLOCK_SIZE * 2);
        let file = Arc::new(scn.open::<R>(false));
        let file_clone = file.clone();
        let expected_2 = scn.data[20..40].to_vec();

        let (t1_sched_tx, t1_sched_rx) = std::sync::mpsc::channel();
        let (t2_done_tx, t2_done_rx) = std::sync::mpsc::channel();

        let handle1 = std::thread::spawn(move || {
            let mut pipeline = DiskCachePipeline::<R, u32>::new().unwrap();
            pipeline.schedule::<Random>(1, &file, 10..50, 1).unwrap();
            t1_sched_tx.send(()).unwrap();

            // Keep pipeline alive without driving it
            t2_done_rx.recv().unwrap();
            drop(pipeline);
        });

        let handle2 = std::thread::spawn(move || {
            t1_sched_rx.recv().unwrap();
            let mut pipeline = DiskCachePipeline::<R, u32>::new().unwrap();
            pipeline
                .schedule::<Random>(2, &file_clone, 20..40, 1)
                .unwrap();

            // Follower enters wait(), times out waiting for handle1 (100ms in test),
            // and completes independently!
            let start = std::time::Instant::now();
            let results = drain_pipeline(&mut pipeline);
            let elapsed = start.elapsed();

            assert_eq!(results[&2], expected_2);
            assert!(elapsed >= std::time::Duration::from_millis(90));
            t2_done_tx.send(()).unwrap();
        });

        handle1.join().unwrap();
        handle2.join().unwrap();
    }

    /// When two distinct pipelines run on the same thread (e.g. nested calls),
    /// the second pipeline does not piggyback to prevent same-thread deadlock.
    #[test]
    fn nested_pipelines_on_same_thread_do_not_deadlock() {
        let scn = Scenario::new(BLOCK_SIZE * 2);
        let file = scn.open::<R>(false);

        let mut outer = DiskCachePipeline::<R, u32>::new().unwrap();
        outer.schedule::<Random>(1, &file, 10..50, 1).unwrap();
        assert_eq!(outer.in_flight_fetches(), 1);

        let mut inner = DiskCachePipeline::<R, u32>::new().unwrap();
        inner.schedule::<Random>(2, &file, 20..40, 1).unwrap();
        assert_eq!(inner.in_flight_fetches(), 1);

        let inner_res = drain_pipeline(&mut inner);
        assert_eq!(inner_res[&2], &scn.data[20..40]);

        let outer_res = drain_pipeline(&mut outer);
        assert_eq!(outer_res[&1], &scn.data[10..50]);
    }

    /// End-to-end `read_batch` with many reads clustered in shared blocks:
    /// every read resolves with its own user data and correct bytes.
    #[test]
    fn read_batch_with_shared_blocks_resolves_every_read() {
        let scn = Scenario::new(BLOCK_SIZE * 3 + 100);
        let file = scn.open::<R>(false);

        let ranges: Vec<(usize, ReadRange)> = (0..64)
            .map(|i| {
                let range = ReadRange {
                    byte_offset: (i * 700) as u64,
                    length: 100,
                };
                (i, range)
            })
            .collect();

        let mut seen = vec![false; ranges.len()];
        file.read_batch(ranges.clone(), Random, |i, bytes: &[u8]| {
            let start = ranges[i].1.byte_offset as usize;
            assert_eq!(bytes, &scn.data[start..start + 100]);
            assert!(!seen[i]);
            seen[i] = true;
            UioResult::Ok(())
        })
        .unwrap();
        assert!(seen.iter().all(|&s| s));
    }

    /// The cache is strictly read-only: writeable opens (the append
    /// vehicle on other backends) are rejected outright — appends must go
    /// directly to the backing storage.
    #[test]
    fn writeable_open_is_rejected() {
        let scn = Scenario::new(10);

        let err = scn
            .fs::<R>()
            .open(
                &scn.remote_path,
                OpenOptions {
                    writeable: true,
                    populate: Populate::No,
                    need_sequential: false,
                    advice: AdviceSetting::Global,
                },
                Default::default(),
            )
            .unwrap_err();
        assert_matches!(
            err,
            crate::universal_io::UniversalIoError::Uninitialized { .. },
        );
    }
}

/// `read_bytes_async` against a remote whose sync read surface always errors:
/// proves cache misses are fetched via the remote's `read_bytes_async`, never
/// its (pipelined) sync reads.
#[cfg(test)]
mod tests_async {
    use std::marker::PhantomData;
    use std::ops::Range;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::ext::aligned_vec::ACow;
    use crate::generic_consts::AccessPattern;
    use crate::universal_io::{
        ListedFile, MmapFs, UioResult, UniversalKind, UniversalReadAsync, UniversalReadFsAsync,
        UserData,
    };

    fn sync_read_error() -> UniversalIoError {
        UniversalIoError::Io(std::io::Error::other("sync read on async-only remote"))
    }

    /// Mmap-backed remote that serves reads only through `read_bytes_async`,
    /// counting them; every sync read path errors.
    #[derive(Debug)]
    struct AsyncOnlyRemote {
        inner: MmapFile,
        async_reads: AtomicUsize,
        fail_on_completion: std::sync::atomic::AtomicBool,
    }

    struct AsyncOnlyPipeline<'file, U>(PhantomData<fn(&'file AsyncOnlyRemote, U)>);

    impl<'file, U: UserData> ReadPipeline<'file, U> for AsyncOnlyPipeline<'file, U> {
        type File = AsyncOnlyRemote;

        fn new() -> UioResult<Self> {
            Ok(Self(PhantomData))
        }

        fn can_schedule(&mut self) -> bool {
            true
        }

        fn schedule<P: AccessPattern>(
            &mut self,
            _user_data: U,
            _file: &'file AsyncOnlyRemote,
            _range: Range<u64>,
            _align: usize,
        ) -> UioResult<()> {
            Err(sync_read_error())
        }

        fn schedule_whole(
            &mut self,
            _user_data: U,
            _file: &'file AsyncOnlyRemote,
            _from: u64,
        ) -> UioResult<()> {
            Err(sync_read_error())
        }

        fn wait(&mut self) -> UioResult<Option<(U, ACow<'file>)>> {
            Ok(None)
        }
    }

    #[derive(Debug, Clone)]
    struct AsyncOnlyFs(MmapFs);

    impl UniversalReadFileOps for AsyncOnlyFs {
        type ContextConfig = ();

        fn from_context(ctx: ()) -> UioResult<Self> {
            Ok(Self(MmapFs::from_context(ctx)?))
        }

        fn list_files(&self, prefix_path: &Path) -> UioResult<Vec<ListedFile>> {
            self.0.list_files(prefix_path)
        }

        fn exists(&self, path: &Path) -> UioResult<bool> {
            self.0.exists(path)
        }
    }

    impl UniversalReadFs for AsyncOnlyFs {
        type File = AsyncOnlyRemote;
        type OpenExtra = ();

        fn open(
            &self,
            path: impl AsRef<Path>,
            options: OpenOptions,
            extra: (),
        ) -> UioResult<AsyncOnlyRemote> {
            Ok(AsyncOnlyRemote {
                inner: self.0.open(path, options, extra)?,
                async_reads: AtomicUsize::new(0),
                fail_on_completion: std::sync::atomic::AtomicBool::new(false),
            })
        }
    }

    impl UniversalReadFsAsync for AsyncOnlyFs {
        async fn open_async(
            &self,
            path: PathBuf,
            options: OpenOptions,
            extra: Self::OpenExtra,
        ) -> UioResult<Self::File> {
            Ok(AsyncOnlyRemote {
                inner: self.0.open_async(path, options, extra).await?,
                async_reads: AtomicUsize::new(0),
                fail_on_completion: std::sync::atomic::AtomicBool::new(false),
            })
        }
    }

    impl UniversalRead for AsyncOnlyRemote {
        type Fs = AsyncOnlyFs;

        type ReadPipeline<'a, U>
            = AsyncOnlyPipeline<'a, U>
        where
            Self: 'a,
            U: UserData;

        fn live_reload(&mut self) -> UioResult<()> {
            self.inner.live_reload()
        }

        fn read_bytes<P: AccessPattern>(
            &self,
            _range: Range<u64>,
            _access_pattern: P,
            _align: usize,
        ) -> UioResult<ACow<'_>> {
            Err(sync_read_error())
        }

        fn len<T>(&self) -> UioResult<u64> {
            self.inner.len::<T>()
        }

        fn populate(&self) -> UioResult<()> {
            Ok(())
        }

        fn populate_auto() -> bool {
            false
        }

        fn clear_ram_cache(&self) -> UioResult<()> {
            Ok(())
        }

        fn kind() -> UniversalKind {
            UniversalKind::Mmap
        }
    }

    impl UniversalReadAsync for AsyncOnlyRemote {
        async fn read_bytes_async<P: AccessPattern>(
            &self,
            range: Range<u64>,
            access_pattern: P,
            align: usize,
        ) -> UioResult<ACow<'_>> {
            // Suspend once so concurrent reads interleave like a real async remote.
            tokio::task::yield_now().await;
            self.async_reads.fetch_add(1, Ordering::Relaxed);
            if self.fail_on_completion.load(Ordering::Relaxed) {
                return Err(sync_read_error());
            }
            self.inner.read_bytes(range, access_pattern, align)
        }
    }

    #[tokio::test]
    async fn statistics_async_error_and_cancellation() {
        let scn = Scenario::new(100);
        let file = scn.open::<AsyncOnlyRemote>(false);
        let stats = file.stats.clone();
        file.state()
            .unwrap()
            .remote
            .fail_on_completion
            .store(true, Ordering::Relaxed);
        assert!(file.read_bytes_async(0..1, Random, 1).await.is_err());
        {
            let mut future = std::pin::pin!(file.read_bytes_async(0..1, Random, 1));
            assert!(futures::poll!(&mut future).is_pending());
        }
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.remote_fetches_started, 2);
        assert_eq!(snapshot.remote_fetch_errors, 1);
        assert_eq!(snapshot.remote_fetches_abandoned, 1);
        assert_eq!(snapshot.remote_fetches_completed, 0);
        assert_eq!(snapshot.downloaded_bytes, 0);
        assert!(snapshot.avg_fetch_duration().is_none());
    }

    /// A cache miss must fetch through the remote's async read and commit the
    /// covering blocks to the local mirror.
    #[tokio::test]
    async fn miss_fetches_via_remote_async_read() {
        let scn = Scenario::new(BLOCK_SIZE * 3 + 100);
        let file = scn.open::<AsyncOnlyRemote>(false);

        let range = 10u64..30;
        let bytes = file
            .read_bytes_async(range.clone(), Random, 1)
            .await
            .unwrap();
        assert_eq!(&*bytes, scn.slice(&range));

        let state = file.state().unwrap();
        assert_eq!(state.remote.async_reads.load(Ordering::Relaxed), 1);
        assert!(state.local.contains(0..1));
    }

    /// Blocks committed by an earlier fetch serve later async reads locally,
    /// without touching the remote again.
    #[tokio::test]
    async fn cached_blocks_skip_remote() {
        let scn = Scenario::new(BLOCK_SIZE * 2);
        let file = scn.open::<AsyncOnlyRemote>(false);

        let first = 10u64..30;
        let bytes = file
            .read_bytes_async(first.clone(), Random, 1)
            .await
            .unwrap();
        assert_eq!(&*bytes, scn.slice(&first));

        // Different range, same block: no second fetch.
        let second = 100u64..200;
        let bytes = file
            .read_bytes_async(second.clone(), Random, 1)
            .await
            .unwrap();
        assert_eq!(&*bytes, scn.slice(&second));

        let state = file.state().unwrap();
        assert_eq!(state.remote.async_reads.load(Ordering::Relaxed), 1);
    }

    /// A read spanning a block boundary into the EOF-clamped partial tail
    /// block resolves in one fetch covering all its blocks.
    #[tokio::test]
    async fn spanning_read_commits_all_blocks() {
        let scn = Scenario::new(BLOCK_SIZE * 3 + 100);
        let file = scn.open::<AsyncOnlyRemote>(false);
        let eof = scn.data.len() as u64;

        let range = (BLOCK_SIZE * 3 - 50) as u64..eof;
        let bytes = file
            .read_bytes_async(range.clone(), Sequential, 1)
            .await
            .unwrap();
        assert_eq!(&*bytes, scn.slice(&range));

        let state = file.state().unwrap();
        assert_eq!(state.remote.async_reads.load(Ordering::Relaxed), 1);
        assert!(state.local.contains(2..4));
    }

    /// Parity with the sync path for the no-fetch branches: empty reads
    /// resolve to an empty slice, out-of-bounds reads error.
    #[tokio::test]
    async fn empty_and_out_of_bounds_need_no_fetch() {
        let scn = Scenario::new(1024);
        let file = scn.open::<AsyncOnlyRemote>(false);

        let bytes = file.read_bytes_async(5..5, Sequential, 1).await.unwrap();
        assert!(bytes.is_empty());

        let err = file
            .read_bytes_async(1000..1100, Sequential, 1)
            .await
            .unwrap_err();
        assert_matches!(err, UniversalIoError::OutOfBounds { .. });

        let state = file.state().unwrap();
        assert_eq!(state.remote.async_reads.load(Ordering::Relaxed), 0);
    }

    /// A zero-length object has nothing to populate: a populating async open
    /// must succeed without issuing a remote read, because a bounded `0..0`
    /// range is rejected by real backends rather than answered with an empty
    /// body (see `AsyncRead::read_range`).
    #[tokio::test]
    async fn empty_object_populating_open_issues_no_fetch() {
        let scn = Scenario::new(0);
        let file = scn
            .fs::<AsyncOnlyRemote>()
            .open_async(
                scn.remote_path.clone(),
                OpenOptions {
                    writeable: false,
                    need_sequential: false,
                    populate: Populate::PreferBackground,
                    advice: AdviceSetting::Global,
                },
                Default::default(),
            )
            .await
            .unwrap();

        let state = file.state().unwrap();
        assert_eq!(state.remote.async_reads.load(Ordering::Relaxed), 0);
        assert!(
            file.read_bytes_async(0..0, Sequential, 1)
                .await
                .unwrap()
                .is_empty()
        );
    }
}

mod statistics {
    use super::*;
    use crate::universal_io::{UniversalReadAsync, UniversalReadFsAsync};

    fn options(populate: Populate) -> OpenOptions {
        OpenOptions {
            writeable: false,
            populate,
            need_sequential: false,
            advice: AdviceSetting::Global,
        }
    }

    #[test]
    fn remote_fetches_are_shared_and_not_counted_again_for_cached_reads() {
        let scn = Scenario::new(BLOCK_SIZE + 100);
        let fs = scn.fs::<MmapFile>();
        let observer = fs.stats();
        let file = fs
            .clone()
            .open(&scn.remote_path, options(Populate::No), Default::default())
            .unwrap();
        let mut pipeline = DiskCachePipeline::<MmapFile, u32>::new().unwrap();
        pipeline
            .schedule::<Random>(0, &file, BLOCK_SIZE as u64..BLOCK_SIZE as u64 + 10, 1)
            .unwrap();
        pipeline
            .schedule::<Random>(1, &file, BLOCK_SIZE as u64 + 20..BLOCK_SIZE as u64 + 30, 1)
            .unwrap();
        assert_eq!(drain_pipeline(&mut pipeline).len(), 2);
        let cold = observer.snapshot();
        assert_eq!(cold.remote_fetches_started, 1);
        assert_eq!(cold.remote_fetches_completed, 1);
        assert_eq!(cold.fetch_duration_histogram.iter().sum::<u64>(), 1);
        assert_eq!(cold.downloaded_bytes, 100);
        file.read_bytes(BLOCK_SIZE as u64..BLOCK_SIZE as u64 + 1, Random, 1)
            .unwrap();
        file.read_bytes(0..0, Random, 1).unwrap();
        let warm = observer.snapshot().delta_since(&cold);
        assert_eq!(warm.remote_fetches_started, 0);
        assert_eq!(warm.fetch_duration_histogram.iter().sum::<u64>(), 0);
        assert_eq!(warm.downloaded_bytes, 0);
        drop(pipeline);
        drop(file);
        let second = fs
            .open(&scn.remote_path, options(Populate::No), Default::default())
            .unwrap();
        second.read_bytes(0..1, Random, 1).unwrap();
        drop(second);
        drop(fs);
        assert_eq!(observer.snapshot().remote_fetches_completed, 2);
        assert_eq!(
            observer.snapshot().downloaded_bytes,
            BLOCK_SIZE as u64 + 100
        );
    }

    #[test]
    fn dropped_pipeline_and_prefill_are_abandoned() {
        let scn = Scenario::new(BLOCK_SIZE);
        let fs = scn.fs::<MmapFile>();
        let stats = fs.stats();
        let file = fs
            .open(&scn.remote_path, options(Populate::No), Default::default())
            .unwrap();
        let mut pipeline = DiskCachePipeline::<MmapFile, ()>::new().unwrap();
        pipeline.schedule::<Random>((), &file, 0..1, 1).unwrap();
        drop(pipeline);
        let prefilled = fs
            .open(
                &scn.remote_path,
                options(Populate::PreferBackground),
                Default::default(),
            )
            .unwrap();
        drop(prefilled);
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.remote_fetches_started, 2);
        assert_eq!(snapshot.remote_fetches_abandoned, 2);
        assert_eq!(snapshot.remote_fetches_completed, 0);
        assert_eq!(snapshot.downloaded_bytes, 0);
    }

    #[test]
    fn sync_prefills_and_read_whole_are_counted_once() {
        let scn = Scenario::new(BLOCK_SIZE + 100);
        for populate in [
            Populate::Blocking,
            Populate::PreferBackground,
            Populate::Partial(ReadRange::new(BLOCK_SIZE as u64, 10)),
            Populate::No,
        ] {
            let fs = scn.fs::<MmapFile>();
            let stats = fs.stats();
            let file = fs
                .open(&scn.remote_path, options(populate), Default::default())
                .unwrap();
            let expected = if matches!(populate, Populate::Partial(_)) {
                file.len::<u8>().unwrap();
                100
            } else {
                file.read_whole::<u8>().unwrap();
                scn.data.len() as u64
            };
            let snapshot = stats.snapshot();
            assert_eq!(snapshot.remote_fetches_started, 1);
            assert_eq!(snapshot.remote_fetches_completed, 1);
            assert_eq!(snapshot.downloaded_bytes, expected);
            assert_eq!(snapshot.remote_fetches_abandoned, 0);
        }
    }

    #[tokio::test]
    async fn async_prefills_and_misses_are_counted() {
        let scn = Scenario::new(BLOCK_SIZE + 100);
        for populate in [
            Populate::Blocking,
            Populate::PreferBackground,
            Populate::Partial(ReadRange::new(BLOCK_SIZE as u64, 10)),
            Populate::No,
        ] {
            let fs = scn.fs::<MmapFile>();
            let stats = fs.stats();
            let file = fs
                .open_async(
                    scn.remote_path.clone(),
                    options(populate),
                    Default::default(),
                )
                .await
                .unwrap();
            file.read_bytes_async(BLOCK_SIZE as u64..BLOCK_SIZE as u64 + 1, Random, 1)
                .await
                .unwrap();
            file.read_bytes_async(BLOCK_SIZE as u64..BLOCK_SIZE as u64 + 1, Random, 1)
                .await
                .unwrap();
            let snapshot = stats.snapshot();
            assert_eq!(snapshot.remote_fetches_started, 1);
            assert_eq!(snapshot.remote_fetches_completed, 1);
            assert_eq!(
                snapshot.downloaded_bytes,
                if matches!(populate, Populate::Partial(_) | Populate::No) {
                    100
                } else {
                    scn.data.len() as u64
                }
            );
        }
    }

    #[test]
    fn reload_tail_is_counted_once_even_when_future_is_shared() {
        let mut scn = Scenario::new(BLOCK_SIZE + 100);
        let fs = scn.fs::<MmapFile>();
        let stats = fs.stats();
        let mut file = fs
            .open(
                &scn.remote_path,
                options(Populate::Blocking),
                Default::default(),
            )
            .unwrap();
        let before = stats.snapshot();
        // Close the append handle before listing: Windows can otherwise report
        // the old size in the directory snapshot and skip the tail fetch.
        scn.grow_remote(100);
        let first = file
            .live_preload(scn.snapshot_file_info::<MmapFile>())
            .unwrap();
        let second = file
            .live_preload(scn.snapshot_file_info::<MmapFile>())
            .unwrap();
        futures::executor::block_on(async {
            futures::join!(first, second);
        });
        file.live_reload().unwrap();
        let delta = stats.snapshot().delta_since(&before);
        assert_eq!(delta.remote_fetches_started, 1);
        assert_eq!(delta.remote_fetches_completed, 1);
        assert_eq!(delta.downloaded_bytes, 200); // includes the old partial block
    }

    #[test]
    fn cross_pipeline_remote_fetches_are_shared_and_counted_once() {
        let scn = Scenario::new(BLOCK_SIZE + 100);
        let fs = scn.fs::<MmapFile>();
        let observer = fs.stats();
        let file = Arc::new(
            fs.clone()
                .open(&scn.remote_path, options(Populate::No), Default::default())
                .unwrap(),
        );
        let file_clone = file.clone();
        let expected_1 = scn.data[10..50].to_vec();
        let expected_2 = scn.data[20..40].to_vec();

        let (t1_ready_tx, t1_ready_rx) = std::sync::mpsc::channel();
        let (t2_scheduled_tx, t2_scheduled_rx) = std::sync::mpsc::channel();

        let handle1 = std::thread::spawn(move || {
            let mut pipeline = DiskCachePipeline::<MmapFile, u32>::new().unwrap();
            pipeline.schedule::<Random>(0, &file, 10..50, 1).unwrap();
            t1_ready_tx.send(()).unwrap();
            t2_scheduled_rx.recv().unwrap();

            let results = drain_pipeline(&mut pipeline);
            assert_eq!(results[&0], expected_1);
        });

        let handle2 = std::thread::spawn(move || {
            t1_ready_rx.recv().unwrap();
            let mut pipeline = DiskCachePipeline::<MmapFile, u32>::new().unwrap();
            pipeline
                .schedule::<Random>(1, &file_clone, 20..40, 1)
                .unwrap();
            t2_scheduled_tx.send(()).unwrap();

            let results = drain_pipeline(&mut pipeline);
            assert_eq!(results[&1], expected_2);
        });

        handle1.join().unwrap();
        handle2.join().unwrap();

        let snapshot = observer.snapshot();
        assert_eq!(snapshot.remote_fetches_started, 1);
        assert_eq!(snapshot.remote_fetches_completed, 1);
        assert_eq!(snapshot.remote_fetches_abandoned, 0);
        assert_eq!(snapshot.downloaded_bytes, BLOCK_SIZE as u64);
    }
}
