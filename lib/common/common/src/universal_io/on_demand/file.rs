use std::borrow::Cow;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use fs_err as fs;
use memmap2::MmapRaw;
use parking_lot::Mutex;
use roaring::RoaringBitmap;

use super::BLOCK_SIZE;
use super::config::OnDemandConfig;
use crate::generic_consts::{AccessPattern, Sequential};
use crate::universal_io::{
    OpenOptions, ReadRange, Result, UniversalIoError, UniversalKind, UniversalRead,
    UniversalReadFileOps,
};

/// A lazily-populated local mirror of an immutable remote file.
///
/// - On [`open`](UniversalRead::open): the remote is opened with
///   `prevent_caching: true`; no double caching should happen.
/// - On the first read: a zero-filled file of the same size is created
///   under [`OnDemandConfig`]'s mirror path, writable-mmapped, and kept
///   around for the lifetime of the value.
/// - Every subsequent read that touches a new block fetches that block
///   from the remote, copies it into the local mmap, and records it in
///   a [`RoaringBitmap`]. Reads that only touch already-fetched blocks
///   are served directly from the local mmap (zero-copy borrows).
/// - On drop: the local cache file is unlinked.
///
/// The remote is assumed to be immutable for the lifetime of the file;
/// this type implements [`UniversalRead`] only, never [`UniversalWrite`].
#[derive(Debug)]
pub struct OnDemandFile<R> {
    remote: R,
    len_bytes: u64,
    local_path: PathBuf,
    local: OnceLock<LocalState>,
    /// Serializes fallible initialization of `local` across threads.
    init_lock: Mutex<()>,
}

#[derive(Debug)]
struct LocalState {
    /// `memmap2` duplicates the backing fd internally, so we don't need
    /// to hold the original `File` alongside.
    mmap: MmapRaw,
    path: PathBuf,
    fetched: Mutex<RoaringBitmap>,
}

impl<R: UniversalReadFileOps> UniversalReadFileOps for OnDemandFile<R> {
    fn list_files(prefix_path: &Path) -> Result<Vec<PathBuf>> {
        R::list_files(prefix_path)
    }

    fn exists(path: &Path) -> Result<bool> {
        R::exists(path)
    }
}

impl<R, T> UniversalRead<T> for OnDemandFile<R>
where
    R: UniversalRead<u8>,
    T: bytemuck::Pod,
{
    fn open(path: impl AsRef<Path>, options: OpenOptions) -> Result<Self> {
        let config = OnDemandConfig::global().ok_or_else(|| {
            UniversalIoError::uninitialized(
                "OnDemandConfig must be initialized via `OnDemandConfig::initialize_global` \
                 before opening an OnDemandFile",
            )
        })?;
        Self::open_with_config(config, path, options)
    }

    fn read<P: AccessPattern>(&self, range: ReadRange) -> Result<Cow<'_, [T]>> {
        let ReadRange {
            byte_offset,
            length,
        } = range;
        if length == 0 {
            return Ok(Cow::Borrowed(&[]));
        }

        let t_size = size_of::<T>() as u64;
        let byte_len = length
            .checked_mul(t_size)
            .ok_or_else(|| out_of_bounds::<T>(byte_offset, u64::MAX, self.len_bytes))?;
        let byte_end = byte_offset
            .checked_add(byte_len)
            .ok_or_else(|| out_of_bounds::<T>(byte_offset, u64::MAX, self.len_bytes))?;
        if byte_end > self.len_bytes {
            return Err(out_of_bounds::<T>(byte_offset, byte_end, self.len_bytes));
        }

        let state = self.local_state()?;
        self.ensure_range(state, byte_offset..byte_end)?;

        // SAFETY: `state.mmap` is `self.len_bytes` bytes long, and every
        // byte in `byte_offset..byte_end` was populated by `ensure_range`
        // while holding `state.fetched`. The release of that mutex
        // synchronizes with this thread's subsequent read. Because the
        // remote is immutable, no block is ever written twice.
        let mmap_bytes =
            unsafe { std::slice::from_raw_parts(state.mmap.as_ptr(), state.mmap.len()) };
        let slice = &mmap_bytes[byte_offset as usize..byte_end as usize];
        Ok(Cow::Borrowed(bytemuck::cast_slice(slice)))
    }

    fn read_batch<'a, P: AccessPattern, Meta: 'a>(
        &'a self,
        ranges: impl IntoIterator<Item = (Meta, ReadRange)>,
        mut callback: impl FnMut(Meta, &[T]) -> Result<()>,
    ) -> Result<()> {
        for (meta, range) in ranges {
            let data = self.read::<P>(range)?;
            callback(meta, &data)?;
        }
        Ok(())
    }

    fn len(&self) -> Result<u64> {
        let t_size = size_of::<T>() as u64;
        debug_assert!(t_size > 0, "zero-sized types are not supported");
        Ok(self.len_bytes / t_size)
    }

    fn populate(&self) -> Result<()> {
        if self.len_bytes == 0 {
            return Ok(());
        }
        if crate::low_memory::low_memory_mode().skip_populate() {
            return Ok(());
        }
        let state = self.local_state()?;
        self.ensure_range(state, 0..self.len_bytes)?;
        Ok(())
    }

    fn clear_ram_cache(&self) -> Result<()> {
        // TODO: issue madvise(DONTNEED) on the local mmap — pages are
        // always reloadable from the local file.
        Ok(())
    }

    fn kind() -> UniversalKind {
        UniversalKind::OnDemand
    }
}

impl<R: UniversalRead<u8>> OnDemandFile<R> {
    /// Open an [`OnDemandFile`] with an explicit configuration instead of
    /// the globally-installed one.
    pub fn open_with_config(
        config: &OnDemandConfig,
        path: impl AsRef<Path>,
        options: OpenOptions,
    ) -> Result<Self> {
        debug_assert!(
            !options.writeable,
            "OnDemandFile only supports immutable files",
        );

        let remote_path = path.as_ref();
        let local_path = config.local_path_for(remote_path);

        let remote_options = OpenOptions {
            writeable: false,
            prevent_caching: Some(true),
            populate: Some(false),
            ..options
        };
        let remote = R::open(remote_path, remote_options)?;
        let len_bytes = UniversalRead::<u8>::len(&remote)?;

        Ok(Self {
            remote,
            len_bytes,
            local_path,
            local: OnceLock::new(),
            init_lock: Mutex::new(()),
        })
    }

    /// Return the cached [`LocalState`], initializing it on first call.
    fn local_state(&self) -> Result<&LocalState> {
        if let Some(state) = self.local.get() {
            return Ok(state);
        }

        // Serialise the (fallible) initialisation so two concurrent
        // first-readers don't both truncate the same backing file.
        let _guard = self.init_lock.lock();
        if let Some(state) = self.local.get() {
            return Ok(state);
        }

        let state = self.init_local_state()?;
        if self.local.set(state).is_err() {
            unreachable!("OnceLock::set must succeed while holding init_lock");
        }
        Ok(self.local.get().expect("just initialized"))
    }

    fn init_local_state(&self) -> Result<LocalState> {
        if let Some(parent) = self.local_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.local_path)?;
        file.set_len(self.len_bytes)?;
        let mmap = MmapRaw::map_raw(&file)?;

        Ok(LocalState {
            mmap,
            path: self.local_path.clone(),
            fetched: Mutex::new(RoaringBitmap::new()),
        })
    }

    // TODO: Fine-grain locking. `state.fetched` is currently held across
    // the entire remote batch, so concurrent misses serialise through
    // this one mutex. A per-block lock (or RwLock + per-block OnceLock)
    // would let independent misses run in parallel.
    fn ensure_range(&self, state: &LocalState, byte_range: Range<u64>) -> Result<()> {
        if byte_range.is_empty() {
            return Ok(());
        }

        let first_block = byte_range.start / BLOCK_SIZE as u64;
        let last_block = (byte_range.end - 1) / BLOCK_SIZE as u64;
        let first_block =
            u32::try_from(first_block).expect("file larger than 70 TiB is not supported");
        let last_block =
            u32::try_from(last_block).expect("file larger than 70 TiB is not supported");

        let mut fetched = state.fetched.lock();

        let missing: Vec<u32> = (first_block..=last_block)
            .filter(|b| !fetched.contains(*b))
            .collect();
        if missing.is_empty() {
            return Ok(());
        }

        let len_bytes = self.len_bytes;
        // Emit one BLOCK_SIZE-capped range per missing block. Backends
        // that benefit from coalescing (e.g. S3) can merge contiguous ranges inside their own
        // `read_batch` implementation; backends that benefit from keeping them
        // separate (e.g. io_uring, where queue depth is the source of
        // parallelism) can submit them individually.
        let ranges = missing.iter().map(|&block| {
            let byte_offset = u64::from(block) * BLOCK_SIZE as u64;
            let length = (BLOCK_SIZE as u64).min(len_bytes - byte_offset);
            (
                block,
                ReadRange {
                    byte_offset,
                    length,
                },
            )
        });

        UniversalRead::<u8>::read_batch::<Sequential, _>(&self.remote, ranges, |block, bytes| {
            let byte_offset = u64::from(block) * BLOCK_SIZE as u64;
            debug_assert_eq!(
                bytes.len() as u64,
                (BLOCK_SIZE as u64).min(len_bytes - byte_offset),
            );

            // SAFETY: `[byte_offset, byte_offset + bytes.len())` lies
            // within the mmap. This block was not in `fetched`, so
            // no prior writer touched it; `state.fetched` is held
            // for the whole batch, excluding concurrent writers.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    bytes.as_ptr(),
                    state.mmap.as_mut_ptr().add(byte_offset as usize),
                    bytes.len(),
                );
            }
            fetched.insert(block);
            Ok(())
        })?;

        Ok(())
    }
}

// For now, let's remove the files on drop
impl<R> Drop for OnDemandFile<R> {
    fn drop(&mut self) {
        let Some(state) = self.local.take() else {
            return;
        };
        let path = state.path.clone();
        // Drop the mmap before unlinking so platforms other than Unix
        // don't hold the file open while we remove it.
        drop(state);
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => log::warn!(
                "failed to remove on-demand cache file {}: {err}",
                path.display(),
            ),
        }
    }
}

fn out_of_bounds<T>(start: u64, end: u64, len_bytes: u64) -> UniversalIoError {
    let t_size = size_of::<T>().max(1) as u64;
    UniversalIoError::OutOfBounds {
        start,
        end,
        elements: (len_bytes / t_size) as usize,
    }
}
