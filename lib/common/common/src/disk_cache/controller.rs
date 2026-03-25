use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};

use fs_err as fs;
use fs_err::os::unix::fs::FileExt;
#[cfg(target_os = "linux")]
use fs_err::os::unix::fs::OpenOptionsExt;
use memmap2::MmapRaw;
use parking_lot::{Condvar, Mutex, RwLock};
use quick_cache::UnitWeighter;
use quick_cache::sync::GuardResult;

use super::{BLOCK_SIZE, BlockId, BlockOffset, BlockRequest, FileId};
use crate::fs::clear_disk_cache;

const UNUSED_BLOCKS_MARGIN: u64 = 16;

/// Caching layer for files in a slow disk, to be mapped into a file in a fast disk.
///
/// It only supports immutable files, so it does not handle dirty pages or manual eviction.
#[derive(Debug)]
pub struct CacheController {
    /// Mapping from the assigned file id, to its file descriptor
    // TODO: use nested `impl UniversalRead` instead
    files: RwLock<HashMap<FileId, fs::File>>,

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

impl CacheController {
    /// Create a new cache instance.
    pub fn new(cache_path: &Path, size_bytes: u64) -> io::Result<Arc<CacheController>> {
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
            files: RwLock::new(HashMap::new()),
            file_id_counter: AtomicU32::new(0),
            cache,
            blocks_lifecycle,
            cache_mmap,
        }))
    }

    /// Opens a new file, registers it with the cache, and returns its
    /// internal id and byte length.
    pub(super) fn open_file(&self, path: &Path) -> io::Result<(FileId, usize)> {
        // FIXME: clear_disk_cache is no-op on macos
        clear_disk_cache(path)?;

        let mut opts = fs::File::options();
        opts.read(true);
        #[cfg(target_os = "linux")]
        opts.custom_flags(nix::libc::O_DIRECT);
        let f = opts.open(path)?;
        #[cfg(target_os = "macos")]
        {
            // TODO: add this to `nix` crate
            use std::os::fd::AsRawFd;
            // SAFETY: valid fd and known fcntl command
            let ret = unsafe { nix::libc::fcntl(f.as_raw_fd(), nix::libc::F_NOCACHE, 1) };
            if ret == -1 {
                return Err(io::Error::last_os_error());
            }
        }

        let len = f.metadata()?.len() as usize;

        // TODO: this will wrap when we have > 4.29 billion files, what to do then?
        let file_id = self.file_id_counter.fetch_add(1, Ordering::SeqCst);
        let file_id = FileId(file_id);

        self.files.write().insert(file_id, f);
        Ok((file_id, len))
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
    ) -> io::Result<CacheRead<'_, O>> {
        let BlockRequest { key, range } = req;

        match self.cache.get_value_or_guard(&key, None) {
            // Cache hit.
            GuardResult::Value(block_offset) => {
                // Read from hot mmap.
                let range = block_offset.bytes() + range.start..block_offset.bytes() + range.end;

                // TODO: Pin the block_id, track the lifetime of the borrow,
                // then release the block_id when the borrow ends
                let slice = unsafe {
                    std::slice::from_raw_parts(
                        self.cache_mmap.as_ptr().add(range.start),
                        range.len(),
                    )
                };
                Ok(CacheRead::Hit(slice))
            }
            // Cache miss.
            GuardResult::Guard(guard) => {
                // 1. Read from cold storage.
                // --------------------------

                let mut buf = [0u8; BLOCK_SIZE];

                let files = self.files.read();
                let file = files
                    .get(&key.file_id)
                    .expect("cached file descriptor is not open");
                if range.len() == BLOCK_SIZE {
                    file.read_exact_at(&mut buf, key.offset.bytes() as u64)?;
                } else {
                    let bytes_read = file.read_at(&mut buf, key.offset.bytes() as u64)?;
                    if bytes_read < range.len() {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "short read from cold storage",
                        ));
                    }
                }

                // 2. Write to hot storage.
                // ------------------------

                let allocated_offset = self.blocks_lifecycle.pop_unused_block();

                let offset = allocated_offset.bytes();
                // SAFETY: non-overlapping regions are guaranteed by the block allocation logic;
                // the insert guard prevent concurrent writes to the same block.
                unsafe {
                    self.cache_mmap
                        .as_mut_ptr()
                        .add(offset)
                        .copy_from(buf.as_ptr(), BLOCK_SIZE);
                }

                // 3. Commit.
                // ----------

                // FIXME: unwrap panics when `key` deleted while guard is still alive.
                guard.insert(allocated_offset).unwrap();

                Ok(CacheRead::Miss(on_miss(&buf[range])))
            }
            GuardResult::Timeout => unreachable!("We didn't set a timeout"),
        }
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
