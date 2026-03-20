use std::io;
use std::ops::Range;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};

use fs_err as fs;
use itertools::Itertools;
use memmap2::MmapRaw;
use parking_lot::{Condvar, Mutex};
use quick_cache::UnitWeighter;
use quick_cache::sync::{GuardResult, PlaceholderGuard};

use super::{BLOCK_SIZE, BlockId, BlockOffset, BlockRequest, FileId};
use crate::fs::clear_disk_cache;
use crate::generic_consts::Random;
use crate::universal_io::io_uring::IoUringFile;
use crate::universal_io::{self, ReadRange, OpenOptions, UniversalRead};

const UNUSED_BLOCKS_MARGIN: u64 = 16;

type CacheGuard<'a> =
    PlaceholderGuard<'a, BlockId, BlockOffset, UnitWeighter, ahash::RandomState, BlocksLifecycle>;

/// Caching layer for files in a slow disk, to be mapped into a file in a fast disk.
///
/// It only supports immutable files, so it does not handle dirty pages or manual eviction.
#[derive(Debug)]
pub struct CacheController<SlowFile: UniversalRead<u8> = IoUringFile> {
    /// Mapping from the assigned file id, to its file descriptor
    files: papaya::HashMap<FileId, SlowFile>,

    /// Used to assign file ids on new files.
    file_id_counter: AtomicU32,

    cache: quick_cache::sync::Cache<
        BlockId,
        BlockOffset,
        UnitWeighter,
        ahash::RandomState,
        BlocksLifecycle,
    >,

    /// Tracks the unused blocks in the cache file.
    ///
    /// It is hooked up to the `cache` so it can free up blocks on eviction.
    blocks_lifecycle: BlocksLifecycle,

    /// Cache file, aka hot storage. (mmap)
    cache_mmap: memmap2::MmapRaw,
}

impl<SlowFile: UniversalRead<u8>> CacheController<SlowFile> {
    /// Create a new cache instance.
    pub fn new(cache_path: &Path, size_bytes: u64) -> io::Result<Arc<Self>> {
        let size_bytes = size_bytes.next_multiple_of(BLOCK_SIZE as u64);
        let size_blocks = size_bytes / BLOCK_SIZE as u64;

        let cache_file = fs::File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(cache_path)?;

        cache_file.set_len(size_bytes)?;

        let cache_mmap = MmapRaw::map_raw(&cache_file)?;

        let size_blocks_u32: u32 = size_blocks.try_into().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("cache size too large: {size_blocks} blocks exceeds u32::MAX"),
            )
        })?;
        let unused_blocks = Arc::new(Mutex::new(
            (0..size_blocks_u32).rev().map(BlockOffset).collect(),
        ));

        let blocks_lifecycle = BlocksLifecycle::new(unused_blocks.clone());

        // Reserve some margin so that we don't frequently need to wait for a block to be evicted
        // during insertion of a new one. `UNUSED_BLOCKS_MARGIN` means (more or less) that this amount
        // of threads can have an available block if they all want to insert at the same time
        let cache_capacity = size_blocks.saturating_sub(UNUSED_BLOCKS_MARGIN);

        let cache = quick_cache::sync::Cache::with_options(
            quick_cache::OptionsBuilder::new()
                .weight_capacity(cache_capacity)
                .estimated_items_capacity(cache_capacity as usize)
                .build()
                .unwrap(),
            UnitWeighter,
            ahash::RandomState::default(),
            blocks_lifecycle.clone(),
        );

        Ok(Arc::new(CacheController {
            files: papaya::HashMap::new(),
            file_id_counter: AtomicU32::new(0),
            cache,
            blocks_lifecycle,
            cache_mmap,
        }))
    }

    /// Opens a new file, registers it with the cache, and returns its
    /// internal id and byte length.
    pub(super) fn open_file(&self, path: &Path) -> universal_io::Result<(FileId, usize)> {
        // FIXME: clear_disk_cache is no-op on macos
        clear_disk_cache(path)?;

        let opts = OpenOptions {
            writeable: false,
            need_sequential: false,
            disk_parallel: None,
            populate: Some(false),
            advice: None,
            prevent_caching: Some(true),
        };

        let f = SlowFile::open(path, opts)?;

        let len = f.len()? as usize;

        // TODO: this will wrap when we have > 4.29 billion files, what to do then?
        let file_id = self.file_id_counter.fetch_add(1, Ordering::SeqCst);
        let file_id = FileId(file_id);

        self.files.pin().insert(file_id, f);

        Ok((file_id, len))
    }

    /// Read a slice from the hot-storage mmap at the given block offset and byte range.
    ///
    /// # Safety
    /// The caller must ensure `block_offset` and `range` refer to a valid,
    /// previously-written region of `cache_mmap`.
    unsafe fn read_cache_mmap(&self, block_offset: BlockOffset, range: Range<usize>) -> &[u8] {
        let start = block_offset.bytes() + range.start;
        let len = range.end - range.start;
        unsafe { std::slice::from_raw_parts(self.cache_mmap.as_ptr().add(start), len) }
    }

    /// Write a block's data into hot storage at the given offset,
    /// zero-padding if the data is shorter than `BLOCK_SIZE`.
    ///
    /// # Safety
    /// The caller must ensure `allocated_offset` points to a region that is not
    /// concurrently read or written (guaranteed by the block lifecycle + insert guard).
    unsafe fn write_cache_mmap(&self, allocated_offset: BlockOffset, data: &[u8]) {
        unsafe {
            let dst = self.cache_mmap.as_mut_ptr().add(allocated_offset.bytes());
            let data_len = data.len();
            dst.copy_from(data.as_ptr(), data_len);
            if data_len < BLOCK_SIZE {
                dst.add(data_len).write_bytes(0, BLOCK_SIZE - data_len);
            }
        }
    }

    /// Fetch a block from cache, or read it from cold storage on miss.
    ///
    /// On hit, returns a borrowed slice from the mmap. On miss, calls
    /// `on_miss` with the raw `&[u8]` so the caller can produce an owned
    /// value with the right type/alignment, avoiding a redundant copy.
    pub(super) fn get_from_cache<O>(
        &self,
        req: BlockRequest,
        on_miss: impl FnOnce(&[u8]) -> O,
    ) -> universal_io::Result<CacheRead<'_, O>> {
        let BlockRequest { key, range } = req;

        match self.cache.get_value_or_guard(&key, None) {
            // Cache hit.
            GuardResult::Value(block_offset) => {
                // TODO: Pin the block_id, track the lifetime of the borrow,
                // then release the block_id when the borrow ends
                let slice = unsafe { self.read_cache_mmap(block_offset, range) };
                Ok(CacheRead::Hit(slice))
            }
            // Cache miss.
            GuardResult::Guard(guard) => {
                // 1. Read from cold storage.
                // --------------------------

                let files = self.files.pin();
                let file = files
                    .get(&key.file_id)
                    .expect("cached file descriptor is not open");

                // Always request a full BLOCK_SIZE so that the read length stays
                // sector-aligned (required by O_DIRECT). The kernel will return a
                // short read for the last block of the file.
                let elem_range = ReadRange {
                    byte_offset: key.offset.bytes() as u64,
                    length: BLOCK_SIZE as u64,
                };

                let cow = file.read::<Random>(elem_range)?;

                // 2. Write to hot storage.
                // ------------------------

                let allocated_offset = self.blocks_lifecycle.pop_unused_block();
                unsafe { self.write_cache_mmap(allocated_offset, &cow) };

                // 3. Commit.
                // ----------

                // FIXME: unwrap panics when `key` deleted while guard is still alive.
                guard.insert(allocated_offset).unwrap();

                Ok(CacheRead::Miss(on_miss(&cow[range])))
            }
            GuardResult::Timeout => unreachable!("We didn't set a timeout"),
        }
    }

    /// Fetch multiple blocks from cache, batching cold-storage reads via `read_batch`.
    ///
    /// Cache hits are served immediately from the mmap. Cache misses are collected
    /// and submitted as a single `read_batch` call per file for asynchronous I/O,
    /// then written to hot storage and committed to the cache.
    ///
    /// For each request, calls `on_result(index, &[u8])` with the requested byte
    /// slice. The `index` corresponds to the position in the `requests` iterator.
    pub(super) fn get_from_cache_batch(
        &self,
        requests: impl IntoIterator<Item = BlockRequest>,
        mut on_result: impl FnMut(usize, &[u8]),
    ) -> universal_io::Result<()> {
        // Phase 1: Check cache, serve hits immediately, collect misses.
        struct SlowReadOp<'a> {
            req_idx: usize,
            req: BlockRequest,
            insert_guard: Option<CacheGuard<'a>>,
        }
        let mut misses: Vec<SlowReadOp> = Vec::new();

        for (idx, req) in requests.into_iter().enumerate() {
            let BlockRequest { key, ref range } = req;

            match self.cache.get_value_or_guard(&key, None) {
                GuardResult::Value(block_offset) => {
                    let slice = unsafe { self.read_cache_mmap(block_offset, range.clone()) };
                    on_result(idx, slice);
                }
                GuardResult::Guard(guard) => {
                    misses.push(SlowReadOp {
                        req_idx: idx,
                        req,
                        insert_guard: Some(guard),
                    });
                }
                GuardResult::Timeout => unreachable!("We didn't set a timeout"),
            }
        }

        if misses.is_empty() {
            return Ok(());
        }

        // Phase 2: Batch read all misses from cold storage.
        //
        // Group misses by file_id so each group can be submitted as a single
        // `read_batch` call. Collect ranges upfront to avoid borrowing `misses`
        // while the callback also needs mutable access to it.

        let files = self.files.pin();

        // We expect usage to be batched for a single file at a time anyway, but chunk to be sure.
        let per_file = misses.into_iter().chunk_by(|op| op.req.key.file_id);

        for (file_id, ops) in per_file.into_iter() {
            let file = files
                .get(&file_id)
                .expect("cached file descriptor is not open");

            let mut ops = ops.collect_vec();
            let ranges = ops
                .iter()
                .map(|op| ElementsRange {
                    start: op.req.key.offset.bytes() as u64,
                    // Always request entire block, if we get to EOF, UniversalRead should return a short read.
                    // We don't want to request less than this because O_DIRECT requires to be aligned to some block size
                    length: BLOCK_SIZE as u64,
                })
                .collect_vec();

            file.read_batch::<false>(ranges, |op_idx, data: &[u8]| {
                let SlowReadOp {
                    req_idx,
                    ref req,
                    ref mut insert_guard,
                } = ops[op_idx];

                // Write to hot storage.
                let allocated_offset = self.blocks_lifecycle.pop_unused_block();
                unsafe { self.write_cache_mmap(allocated_offset, data) };

                // Commit to cache.
                let guard = insert_guard.take();
                guard.expect("each read is only carried out once").insert(allocated_offset).expect("There are no remove operations happening, and we always insert through placeholder");

                on_result(req_idx, &data[req.range.clone()]);
                Ok(())
            })?;
        }

        Ok(())
    }
}

/// Result of a cache lookup.
pub(super) enum CacheRead<'a, O> {
    /// Cache hit: zero-copy borrow from the mmap.
    Hit(&'a [u8]),
    /// Cache miss: caller-produced owned value from the `on_miss` closure.
    Miss(O),
}

/// Global cache controller. Will be used in production.
static GLOBAL: OnceLock<Arc<CacheController>> = OnceLock::new();

impl CacheController {
    pub fn initialize_global(path: &Path, size_bytes: u64) {
        assert!(GLOBAL.get().is_none(), "disk cacher is already initialized");
        let cacher = Self::new(path, size_bytes).expect("failed to initialize disk cacher");
        GLOBAL
            .set(cacher)
            .expect("disk cacher is already initialized");
    }

    pub fn global() -> Option<&'static Arc<CacheController>> {
        GLOBAL.get()
    }
}

#[derive(Clone, Debug)]
pub(super) struct BlocksLifecycle {
    unused_blocks: Arc<Mutex<Vec<BlockOffset>>>,
    blocks_available: Arc<Condvar>,
}

impl BlocksLifecycle {
    fn new(unused_blocks: Arc<Mutex<Vec<BlockOffset>>>) -> Self {
        Self {
            unused_blocks,
            blocks_available: Arc::new(Condvar::new()),
        }
    }

    fn pop_unused_block(&self) -> BlockOffset {
        let mut pool = self.unused_blocks.lock();
        loop {
            if let Some(offset) = pool.pop() {
                return offset;
            }
            let timed_out = self
                .blocks_available
                .wait_for(&mut pool, std::time::Duration::from_secs(10))
                .timed_out();
            if timed_out {
                log::warn!(
                    "Disk cache: waiting for a free block for over 10s ({} blocks in pool)",
                    pool.len(),
                );
            }
        }
    }
}

impl quick_cache::Lifecycle<BlockId, BlockOffset> for BlocksLifecycle {
    // With `UnitWeighter` every item weighs 1, so inserting one item
    // evicts at most one item. `Option` is sufficient.
    type RequestState = Option<BlockOffset>;

    fn begin_request(&self) -> Self::RequestState {
        None
    }

    fn on_evict(&self, state: &mut Self::RequestState, _key: BlockId, val: BlockOffset) {
        debug_assert!(
            state.is_none(),
            "multiple evictions per request with UnitWeighter"
        );
        *state = Some(val);
    }

    fn is_pinned(&self, _key: &BlockId, _val: &BlockOffset) -> bool {
        // TODO: pin blocks that are being read
        false
    }

    fn before_evict(
        &self,
        _state: &mut Self::RequestState,
        _key: &BlockId,
        _val: &mut BlockOffset,
    ) {
        // do nothing
    }

    fn end_request(&self, state: Self::RequestState) {
        if let Some(offset) = state {
            let mut pool = self.unused_blocks.lock();
            pool.push(offset);
            self.blocks_available.notify_one();
        }
    }
}
