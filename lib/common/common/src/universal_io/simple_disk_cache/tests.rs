use std::assert_matches;
use std::borrow::Cow;
use std::collections::HashMap;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use fs_err as fs;

use super::pipeline::DiskCachePipeline;
use super::{
    BLOCK_SIZE, DiskCache, DiskCacheConfig, DiskCacheFs, DiskCacheFsContext, DiskCacheRemote,
};
use crate::ext::aligned_vec::ACow;
use crate::generic_consts::{AccessPattern, Random, Sequential};
use crate::mmap::AdviceSetting;
use crate::universal_io::{
    ListedFile, MmapFile, MmapFs, OpenOptions, Populate, ReadPipeline, ReadRange, Result,
    UniversalIoError, UniversalKind, UniversalRead, UniversalReadFileOps, UniversalReadFs,
    UserData,
};

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

        let fs = DiskCacheFs::<R>::from_context(DiskCacheFsContext {
            config: self.config.clone(),
            remote: Default::default(),
        })
        .unwrap();
        fs.open(
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

/// A remote backend delegating to [`MmapFile`], whose completed reads fail at
/// `wait` while the shared `fail` flag is set. Mirrors the io_uring per-op
/// error path: the op's user data is consumed and the error identifies no
/// specific fetch.
#[derive(Debug, Clone)]
struct FailingRemote {
    inner: MmapFile,
    fail: Arc<AtomicBool>,
}

#[derive(Debug, Clone)]
struct FailingRemoteFs {
    fail: Arc<AtomicBool>,
}

impl UniversalReadFileOps for FailingRemoteFs {
    type ContextConfig = Arc<AtomicBool>;

    fn from_context(fail: Arc<AtomicBool>) -> Result<Self> {
        Ok(Self { fail })
    }

    fn list_files(&self, prefix_path: &Path) -> Result<Vec<ListedFile>> {
        MmapFs.list_files(prefix_path)
    }

    fn exists(&self, path: &Path) -> Result<bool> {
        MmapFs.exists(path)
    }
}

impl UniversalReadFs for FailingRemoteFs {
    type File = FailingRemote;
    type OpenExtra = ();

    fn open(
        &self,
        path: impl AsRef<Path>,
        options: OpenOptions,
        _extra: (),
    ) -> Result<FailingRemote> {
        Ok(FailingRemote {
            inner: MmapFs.open(path, options, ())?,
            fail: self.fail.clone(),
        })
    }
}

impl UniversalRead for FailingRemote {
    type Fs = FailingRemoteFs;

    type ReadPipeline<'a, U>
        = FailingPipeline<'a, U>
    where
        Self: 'a,
        U: UserData;

    fn reopen(&mut self) -> Result<()> {
        self.inner.reopen()
    }

    fn read_bytes<P: AccessPattern>(&self, range: Range<u64>, align: usize) -> Result<ACow<'_>> {
        self.inner.read_bytes::<P>(range, align)
    }

    fn len<T>(&self) -> Result<u64> {
        self.inner.len::<T>()
    }

    fn populate(&self) -> Result<()> {
        self.inner.populate()
    }

    fn populate_auto() -> bool {
        MmapFile::populate_auto()
    }

    fn clear_ram_cache(&self) -> Result<()> {
        self.inner.clear_ram_cache()
    }

    fn kind() -> UniversalKind {
        UniversalKind::Mmap
    }
}

struct FailingPipeline<'file, U: UserData> {
    inner: <MmapFile as UniversalRead>::ReadPipeline<'file, U>,
    fail: Option<Arc<AtomicBool>>,
}

impl<'file, U: UserData> ReadPipeline<'file, U> for FailingPipeline<'file, U> {
    type File = FailingRemote;

    fn new() -> Result<Self> {
        Ok(Self {
            inner: ReadPipeline::new()?,
            fail: None,
        })
    }

    fn can_schedule(&mut self) -> bool {
        self.inner.can_schedule()
    }

    fn schedule<P: AccessPattern>(
        &mut self,
        user_data: U,
        file: &'file FailingRemote,
        range: Range<u64>,
        align: usize,
    ) -> Result<()> {
        self.fail = Some(file.fail.clone());
        self.inner
            .schedule::<P>(user_data, &file.inner, range, align)
    }

    fn schedule_whole(
        &mut self,
        user_data: U,
        file: &'file FailingRemote,
        from: u64,
    ) -> Result<()> {
        self.fail = Some(file.fail.clone());
        self.inner.schedule_whole(user_data, &file.inner, from)
    }

    fn wait(&mut self) -> Result<Option<(U, ACow<'file>)>> {
        let next = self.inner.wait()?;
        let injected = self
            .fail
            .as_ref()
            .is_some_and(|fail| fail.load(Ordering::Relaxed));
        if next.is_some() && injected {
            return Err(UniversalIoError::Io(std::io::Error::other(
                "injected fetch failure",
            )));
        }
        Ok(next)
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
    [tests_uring]   [IoUringFile]   [cfg(target_os = "linux")]  [false];
)]
#[cfg_predicate]
#[cfg(test)]
mod tests_mod {
    use std::sync::atomic::Ordering;

    use super::*;
    #[cfg_predicate]
    use crate::universal_io::R;

    const PREFILL: bool = _PREFILL;

    #[test]
    fn basic_read_returns_remote_bytes() {
        let scn = Scenario::new(BLOCK_SIZE * 3 + 100);
        let file = scn.open::<R>(PREFILL);

        // Read inside the first block.
        let bytes = file
            .read::<Sequential, u8>(ReadRange {
                byte_offset: 10,
                length: 20,
            })
            .unwrap();
        assert_eq!(&*bytes, &scn.data[10..30]);

        // Last block includes the 100-byte tail.
        let last = scn.data.len() as u64;
        let bytes = file
            .read::<Sequential, u8>(ReadRange {
                byte_offset: last - 50,
                length: 50,
            })
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
            .read::<Sequential, u8>(ReadRange {
                byte_offset: start,
                length: len,
            })
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
        let _ = file
            .read::<Sequential, u8>(ReadRange {
                byte_offset: 0,
                length: 1,
            })
            .unwrap();

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
            .read::<Sequential, u8>(ReadRange {
                byte_offset: 0,
                length: scn.data.len() as u64,
            })
            .unwrap();
        assert_eq!(&*bytes, &scn.data[..]);
    }

    #[test]
    fn read_past_end_returns_out_of_bounds() {
        let scn = Scenario::new(1024);
        let file = scn.open::<R>(PREFILL);

        let err = file
            .read::<Sequential, u8>(ReadRange {
                byte_offset: 1000,
                length: 100,
            })
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
                .read::<Sequential, u8>(ReadRange {
                    byte_offset: 0,
                    length: scn.data.len() as u64,
                })
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

        let _ = cache
            .read::<Sequential, u8>(ReadRange {
                byte_offset: 0,
                length: 1,
            })
            .unwrap();

        let local_path = cache.local_path.clone();
        assert!(local_path.exists());

        drop(cache);
        assert!(!local_path.exists());
    }

    /// Reopen with no prior reads leaves the local mirror untouched.
    #[test]
    fn reopen_without_prior_reads_keeps_local_uninitialized() {
        let scn = Scenario::new(BLOCK_SIZE * 2);
        let mut cache = scn.open::<R>(PREFILL);
        let expected_local = cache.local_path.clone();
        assert!(!expected_local.exists());

        cache.reopen().unwrap();

        // it it was scheduled for prefill, it materializes the local file
        if PREFILL {
            assert!(expected_local.exists());
        } else {
            assert!(!expected_local.exists());
        }

        // In both Populate::No and Populate::PreferBackground, we still
        // have local marked as uninitialized at this point
        assert!(!cache.is_ready());
    }

    /// Reopen on an unchanged remote must not resize, repopulate, or mutate
    /// the fetched bitmap.
    #[test]
    fn reopen_no_growth_does_not_repopulate() {
        let scn = Scenario::new(BLOCK_SIZE * 3);
        let mut cache = scn.open::<R>(PREFILL);

        let _ = cache
            .read::<Sequential, u8>(ReadRange {
                byte_offset: 0,
                length: 1,
            })
            .unwrap();

        let (len_before, populated_before, fetched_before) = {
            let local = cache.state().expect("local initialized after read").local;
            (
                local.mmap().len::<u8>().unwrap(),
                local.fully_populated.load(Ordering::Acquire),
                local.fetched.lock().clone(),
            )
        };

        cache.reopen().unwrap();

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

        let _ = cache
            .read::<Sequential, u8>(ReadRange {
                byte_offset: 0,
                length: 1,
            })
            .unwrap();

        let new_data = scn.grow_remote(BLOCK_SIZE);

        let err = cache
            .read::<Sequential, u8>(ReadRange {
                byte_offset: original_len,
                length: BLOCK_SIZE as u64,
            })
            .unwrap_err();
        assert_matches!(
            err,
            crate::universal_io::UniversalIoError::OutOfBounds { .. },
        );

        cache.reopen().unwrap();

        let bytes = cache
            .read::<Sequential, u8>(ReadRange {
                byte_offset: original_len,
                length: BLOCK_SIZE as u64,
            })
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
            .read::<Sequential, u8>(ReadRange {
                byte_offset: BLOCK_SIZE as u64,
                length: 1,
            })
            .unwrap();

        // Grow remote past the old tail block boundary.
        let new_data = scn.grow_remote(BLOCK_SIZE);

        cache.reopen().unwrap();

        // Read covers both the originally-partial range [BLOCK_SIZE..old_len)
        // and the newly-grown tail [old_len..BLOCK_SIZE*2). Without the
        // invalidation, the second half would be zeros from `set_len`.
        let bytes = cache
            .read::<Sequential, u8>(ReadRange {
                byte_offset: BLOCK_SIZE as u64,
                length: BLOCK_SIZE as u64,
            })
            .unwrap();
        assert_eq!(&*bytes, &new_data[BLOCK_SIZE..BLOCK_SIZE * 2]);
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
            .read::<Sequential, u8>(ReadRange {
                byte_offset: 10,
                length: 20,
            })
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
            .read::<Sequential, u8>(ReadRange {
                byte_offset: start,
                length: 30,
            })
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
            .read::<Sequential, u8>(ReadRange {
                byte_offset: 0,
                length: 16,
            })
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
            .read::<Sequential, u8>(ReadRange {
                byte_offset: 0,
                length: 100,
            })
            .unwrap();
        assert_eq!(&*bytes, &scn.data[..]);
        assert_eq!(file.len::<u8>().unwrap(), 100);
    }
}

/// White-box tests for [`DiskCachePipeline`]'s fetch deduplication: reads whose
/// blocks are fully covered by an in-flight fetch on the same file piggyback on
/// it instead of scheduling a duplicate remote fetch.
#[duplicate::duplicate_item(
    tests_mod               R               cfg_predicate;
    [pipeline_tests_mmap]   [MmapFile]      [cfg(all())];
    [pipeline_tests_uring]  [IoUringFile]   [cfg(target_os = "linux")];
)]
#[cfg_predicate]
#[cfg(test)]
mod tests_mod {
    use super::*;
    #[cfg_predicate]
    use crate::universal_io::R;

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
        file.read_batch::<Random, u8, usize>(ranges.clone(), |i, bytes| {
            let start = ranges[i].1.byte_offset as usize;
            assert_eq!(bytes, &scn.data[start..start + 100]);
            assert!(!seen[i]);
            seen[i] = true;
            Ok(())
        })
        .unwrap();
        assert!(seen.iter().all(|&s| s));
    }
}

/// How [`DiskCachePipeline`] resolves remote fetch errors: the failure can't be
/// attributed to a specific fetch, so every pending read fails with it and the
/// pipeline resets to a clean, reusable state.
#[cfg(test)]
mod pipeline_error_tests {
    use super::*;

    fn open_failing(scn: &Scenario) -> (DiskCache<FailingRemote>, Arc<AtomicBool>) {
        let fail = Arc::new(AtomicBool::new(false));
        let fs = DiskCacheFs::<FailingRemote>::from_context(DiskCacheFsContext {
            config: scn.config.clone(),
            remote: fail.clone(),
        })
        .unwrap();
        let file = fs
            .open(
                &scn.remote_path,
                OpenOptions {
                    writeable: false,
                    populate: Populate::No,
                    need_sequential: false,
                    advice: AdviceSetting::Global,
                },
                Default::default(),
            )
            .unwrap();
        (file, fail)
    }

    /// A failed fetch fails all its reads — the originator and the
    /// piggybacked ones. None are left stranded, and the pipeline stays
    /// usable: a retry schedules a fresh fetch instead of piggybacking on
    /// the dead one.
    #[test]
    fn failed_fetch_fails_piggybacked_reads() {
        let scn = Scenario::new(BLOCK_SIZE * 2);
        let (file, fail) = open_failing(&scn);

        let mut pipeline = DiskCachePipeline::<FailingRemote, u32>::new().unwrap();
        pipeline.schedule::<Random>(0, &file, 10..30, 1).unwrap();
        pipeline.schedule::<Random>(1, &file, 100..200, 1).unwrap();
        assert_eq!(pipeline.in_flight_fetches(), 1);

        // The fetch fails: the error surfaces once, failing both reads.
        fail.store(true, Ordering::Relaxed);
        assert!(pipeline.wait().is_err());

        // Neither read is left stranded: the pipeline reports empty.
        assert!(pipeline.wait().unwrap().is_none());

        fail.store(false, Ordering::Relaxed);
        pipeline.schedule::<Random>(2, &file, 100..200, 1).unwrap();
        // A fresh fetch, not a piggyback on the dead entry or a local hit.
        assert_eq!(pipeline.in_flight_fetches(), 1);
        let results = drain_pipeline(&mut pipeline);
        assert_eq!(results.len(), 1);
        assert_eq!(results[&2], &scn.data[100..200]);
    }

    /// Reads resolved before the failure are still returned; only pending
    /// ones fail with the error.
    #[test]
    fn resolved_reads_survive_fetch_failure() {
        let scn = Scenario::new(BLOCK_SIZE * 2);
        let (file, fail) = open_failing(&scn);

        let mut pipeline = DiskCachePipeline::<FailingRemote, u32>::new().unwrap();
        // Commit block 0 to the mirror.
        pipeline.schedule::<Random>(0, &file, 10..30, 1).unwrap();
        drain_pipeline(&mut pipeline);

        // A local hit queued ahead of a remote fetch that will fail.
        pipeline.schedule::<Random>(1, &file, 40..60, 1).unwrap();
        pipeline
            .schedule::<Random>(2, &file, BLOCK_SIZE as u64..BLOCK_SIZE as u64 + 50, 1)
            .unwrap();
        fail.store(true, Ordering::Relaxed);

        // The already-resolved read is still returned...
        let (user_data, bytes) = pipeline.wait().unwrap().unwrap();
        assert_eq!(user_data, 1);
        assert_eq!(&*bytes, &scn.data[40..60]);

        // ...then the failed fetch surfaces its error, and nothing is stranded.
        assert!(pipeline.wait().is_err());
        assert!(pipeline.wait().unwrap().is_none());
    }
}
