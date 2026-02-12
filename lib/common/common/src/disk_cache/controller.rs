#![allow(dead_code)] // for now

use std::borrow::Cow;
use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::sync::{Arc, OnceLock};

use fs_err as fs;
use fs_err::os::unix::fs::FileExt;
#[cfg(target_os = "linux")]
use fs_err::os::unix::fs::OpenOptionsExt;
use parking_lot::{Mutex, RwLock};
use quick_cache::UnitWeighter;
use quick_cache::sync::GuardResult;

use super::cached_file::CachedFile;
use super::{BLOCK_SIZE, BlockId, BlockOffset, BlockRequest, FileId};
use crate::fs::clear_disk_cache;

/// Number to subtract to the cache capacity, so that we always make sure to
/// have an empty block to write.
const CACHE_CAPACITY_SAFETY_MARGIN: u64 = 2048; // FIXME

/// Caching layer stub.
///
/// Once we implement it, it would be used to read static files from slow
/// network-attached disks while caching recently accessed file pages on fast
/// local SSD.
///
/// But for now the current implementation is a stub that provides rough
/// public interface, but none of desired performance characteristics.
#[derive(Debug)]
pub struct CacheController {
    /// Mapping from the assigned file id, to its file descriptor
    pub(super) files: RwLock<HashMap<FileId, fs::File>>,

    /// Used to assign file ids on new files.
    pub(super) file_id_counter: Mutex<FileId>,

    pub(super) cache: quick_cache::sync::Cache<
        BlockId,
        BlockOffset,
        UnitWeighter,
        ahash::RandomState,
        BlocksLifecycle,
    >,

    /// Tracks the unused blocks in the cache file.
    ///
    /// It is hooked up to the `cache` so it can free up blocks on eviction.
    pub(super) blocks_lifecycle: BlocksLifecycle,

    /// Cache file, aka hot storage. (file descriptor)
    pub(super) cache_file: fs::File,

    /// Cache file, aka hot storage. (mmap)
    pub(super) cache_mmap: memmap2::Mmap,
}

#[derive(Debug)]
pub struct CacherFd(Arc<CacheController>, FileId);

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

        let cache_mmap = unsafe { memmap2::Mmap::map(&cache_file)? };

        let unused_blocks = Arc::new(Mutex::new(
            (0..size_blocks as u32).rev().map(BlockOffset).collect(),
        ));

        let blocks_lifecycle = BlocksLifecycle::new(unused_blocks.clone());

        let cache_capacity = size_blocks - CACHE_CAPACITY_SAFETY_MARGIN;

        let cache = quick_cache::sync::Cache::with_options(
            quick_cache::OptionsBuilder::new()
                .weight_capacity(cache_capacity)
                .estimated_items_capacity(cache_capacity as usize)
                // TODO(luis): Depending on this number we can increase/decrease CACHE_CAPACITY_SAFETY_MARGIN.
                //       The default number is num_detected_cores * 4.
                //       We need to optimize unused_offsets so that it can also leverage sharding, otherwise it
                //       might become a source of contention.
                .shards(1)
                .build()
                .unwrap(),
            UnitWeighter,
            ahash::RandomState::default(),
            blocks_lifecycle.clone(),
        );

        Ok(Arc::new(CacheController {
            files: RwLock::new(HashMap::new()),
            file_id_counter: Mutex::new(FileId(0)),
            cache,
            blocks_lifecycle,
            cache_file,
            cache_mmap,
        }))
    }

    /// Opens a new file and returns its descriptor.
    pub(crate) fn open_file(self: &Arc<Self>, path: &Path) -> io::Result<CachedFile> {
        // FIXME: clear_disk_cache is no-op on macos
        clear_disk_cache(path)?;

        let mut opts = fs::File::options();
        opts.read(true);
        #[cfg(target_os = "linux")]
        opts.custom_flags(nix::libc::O_DIRECT);
        let f = opts.open(path)?;
        #[cfg(target_os = "macos")]
        unsafe {
            // TODO: add this to `nix`
            use std::os::fd::AsRawFd;
            nix::libc::fcntl(f.as_raw_fd(), nix::libc::F_NOCACHE, 1);
        }

        let len = f.metadata()?.len() as usize;

        let file_id = {
            let mut file_id_counter = self.file_id_counter.lock();
            *file_id_counter = FileId(file_id_counter.0.checked_add(1).unwrap_or_else(|| {
                // TODO: drain the entire cache so that we don't corrupt stuff
                //
                // (luis) actually, it won't be enough to drain. Already "open" files
                // can still try to access their cached data, and we don't have a mechanism to prevent this.
                // I think we need to return an error
                //
                // (xzfc) Maybe this draining should block on items that `is_pinned`.
                // Anyway, it's rare case and we can decide it later.
                unimplemented!("What to do when we have > 4.29 billion files?")
            }));
            *file_id_counter
        };

        self.files.write().insert(file_id, f);
        Ok(CachedFile {
            file_id,
            len,
            controller: Arc::clone(self),
        })
    }

    pub(super) fn get_from_cache(&self, req: BlockRequest) -> io::Result<Cow<'_, [u8]>> {
        let BlockRequest { key, range } = req;

        match self.cache.get_value_or_guard(&key, None) {
            // Cache hit.
            GuardResult::Value(block_offset) => {
                // Read from hot mmap.
                let range = block_offset.bytes() + range.start..block_offset.bytes() + range.end;

                // TODO: Pin the block_id, track the lifetime of the borrow,
                // then release the block_id when the borrow ends
                Ok(Cow::Borrowed(&self.cache_mmap[range]))
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
                    file.read_at(&mut buf, key.offset.bytes() as u64)?;
                }

                // 2. Write to hot storage.
                // ------------------------

                let allocated_offset = self.blocks_lifecycle.pop_unused_block();

                self.cache_file
                    .write_all_at(&buf, allocated_offset.bytes() as u64)?;

                // 3. Commit.
                // ----------

                // FIXME: unwrap panics when `key` deleted while guard is still alive.
                guard.insert(allocated_offset).unwrap();

                Ok(Cow::Owned(buf[range].to_vec()))
            }
            GuardResult::Timeout => unreachable!("We didn't set a timeout"),
        }
    }
}

/// Global cache controller. Will be used in production.
static GLOBAL: OnceLock<Arc<CacheController>> = OnceLock::new();

impl CacheController {
    pub fn initialize_global(path: &Path, size_bytes: u64) {
        assert!(GLOBAL.get().is_none(), "cacher is already initialized");
        let cacher = Self::new(path, size_bytes).expect("failed to initialize cacher");
        GLOBAL.set(cacher).expect("cacher is already initialized");
    }

    pub fn global() -> Option<&'static Arc<CacheController>> {
        GLOBAL.get()
    }
}

#[derive(Clone, Debug)]
pub(super) struct BlocksLifecycle {
    unused_blocks: Arc<Mutex<Vec<BlockOffset>>>,
}

impl BlocksLifecycle {
    fn new(unused_blocks: Arc<Mutex<Vec<BlockOffset>>>) -> Self {
        Self { unused_blocks }
    }

    fn pop_unused_block(&self) -> BlockOffset {
        // TODO: perhaps wait if unused offsets are empty?
        self.unused_blocks
            .lock()
            .pop()
            .expect("There is always at least one unused block")
    }
}

impl quick_cache::Lifecycle<BlockId, BlockOffset> for BlocksLifecycle {
    type RequestState = Option<(BlockId, BlockOffset)>;

    fn begin_request(&self) -> Self::RequestState {
        None
    }

    fn on_evict(&self, state: &mut Self::RequestState, key: BlockId, val: BlockOffset) {
        // putting the evicted value in the state allows for quick_cache to hold its locks
        // for less time. And allows to defer the handling for later.
        //
        // TODO(luis): We might want to put it back at this moment, so that we ensure there is always
        //         at least one unused block. Downside is larger critical section within the quick_cache locks.
        //      Although, even if we put it back here, quick_cache is sharded internally, so
        //        we would need a CACHE_CAPACITY_SAFETY_MARGIN of at least the number of shards to be sure.
        if state.is_none() {
            *state = Some((key, val));
        }
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
        // Insert evicted block back to unused pool.
        match state {
            None => (),
            Some((_, off)) => self.unused_blocks.lock().push(off),
        }
    }
}

impl CacherFd {
    /// Read file chunks.
    pub fn read(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        match self.0.files.read().get(&self.1) {
            Some(f) => f.read_exact_at(buf, offset),
            None => Err(io::Error::other("unexpected error")),
        }
    }
}

impl Drop for CacherFd {
    fn drop(&mut self) {
        // self.0.files.lock().remove(&self.1);
    }
}
