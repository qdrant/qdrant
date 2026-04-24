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
use crate::mmap::{AdviceSetting, Madviseable};
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
    /// Open options for when it gets initialized
    open_options: OpenOptions,
    /// Path to the local mmap file
    local_path: PathBuf,
    local: OnceLock<LocalState>,
    /// Prevents concurrent initialization of `local` across threads.
    ///
    /// TODO: Switch to [`OnceLock::get_or_try_init`][1] once it stabilizes
    ///
    /// [1]: https://github.com/rust-lang/rust/issues/109737
    init_lock: Mutex<()>,
}

#[derive(Debug)]
struct LocalState {
    mmap: MmapRaw,
    /// Bitmask to know which blocks are have been fetched
    fetched: Mutex<RoaringBitmap>,
}

impl LocalState {
    /// # Safety
    /// `byte_range` must have been populated via [`OnDemandFile::ensure_byte_ranges`] first.
    unsafe fn read_mmap_bytes(&self, byte_range: Range<u64>) -> &[u8] {
        let bytes = unsafe { std::slice::from_raw_parts(self.mmap.as_ptr(), self.mmap.len()) };
        &bytes[byte_range.start as usize..byte_range.end as usize]
    }
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
        let byte_range = self.compute_byte_range::<T>(range)?;
        if byte_range.is_empty() {
            return Ok(Cow::Borrowed(&[]));
        }

        let state = self.local_state()?;
        self.ensure_byte_ranges(state, std::iter::once(byte_range.clone()))?;

        // SAFETY: `ensure_byte_ranges` above populated `byte_range`.
        let bytes = unsafe { state.read_mmap_bytes(byte_range) };
        Ok(Cow::Borrowed(bytemuck::cast_slice(bytes)))
    }

    fn read_batch<'a, P: AccessPattern, Meta: 'a>(
        &'a self,
        ranges: impl IntoIterator<Item = (Meta, ReadRange)>,
        mut callback: impl FnMut(Meta, &[T]) -> Result<()>,
    ) -> Result<()> {
        for (meta, range) in ranges {
            let cow = self.read::<P>(range)?;
            callback(meta, cow.as_ref())?;
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
        self.ensure_byte_ranges(state, std::iter::once(0..self.len_bytes))?;
        Ok(())
    }

    fn clear_ram_cache(&self) -> Result<()> {
        if let Some(state) = self.local.get() {
            state.mmap.clear_cache();
        }
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
        let local_path = config.local_path_for(remote_path)?;

        let remote_options = OpenOptions {
            writeable: false,
            prevent_caching: Some(true),
            populate: Some(false),
            need_sequential: false,
            disk_parallel: None,
            advice: None,
        };

        let remote = R::open(remote_path, remote_options)?;
        let len_bytes = UniversalRead::<u8>::len(&remote)?;

        Ok(Self {
            remote,
            len_bytes,
            open_options: options,
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

        // Only first thread is able to initialize
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

        let OpenOptions {
            writeable: _,       // always needs to be writeable
            need_sequential: _, // TODO: add sequential mmap
            disk_parallel: _,   // unsupported
            populate: _,        // this is handled in populate() function
            advice,
            prevent_caching: _, // TODO: use o_direct
        } = self.open_options;

        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.local_path)?;
        file.set_len(self.len_bytes)?;
        let mmap = MmapRaw::map_raw(&file)?;

        mmap.madvise(advice.unwrap_or(AdviceSetting::Global).resolve())?;

        Ok(LocalState {
            mmap,
            fetched: Mutex::new(RoaringBitmap::new()),
        })
    }

    /// Validate a typed [`ReadRange`] and return its byte bounds
    /// `[start, end)`. Zero-length ranges produce `[offset, offset)`.
    fn compute_byte_range<T: bytemuck::Pod>(&self, range: ReadRange) -> Result<Range<u64>> {
        let ReadRange {
            byte_offset,
            length,
        } = range;
        if length == 0 {
            return Ok(byte_offset..byte_offset);
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
        Ok(byte_offset..byte_end)
    }

    // TODO: Fine-grain locking. `state.fetched` is currently held
    // across the whole remote batch, so concurrent misses serialise
    // through this one mutex. An atomic bitvec should allow misses to not
    // block reads.
    //
    /// Ensure every byte in the union of `byte_ranges` is present in
    /// the local mmap. All blocks missing across the input set are
    /// fetched in a single `R::read_batch` call so the backend sees
    /// the full batch and can reorder / pipeline / coalesce it however
    /// it prefers.
    fn ensure_byte_ranges(
        &self,
        state: &LocalState,
        byte_ranges: impl IntoIterator<Item = Range<u64>>,
    ) -> Result<()> {
        let mut fetched = state.fetched.lock();

        // Union of all block indices touched by the input.
        let mut wanted = RoaringBitmap::new();
        for br in byte_ranges {
            if br.is_empty() {
                continue;
            }
            let first = br.start / BLOCK_SIZE as u64;
            let last = (br.end - 1) / BLOCK_SIZE as u64;
            let first = u32::try_from(first).expect("file larger than 70 TiB is not supported");
            let last = u32::try_from(last).expect("file larger than 70 TiB is not supported");
            wanted.insert_range(first..=last);
        }

        let to_fetch = wanted - &*fetched;
        if to_fetch.is_empty() {
            return Ok(());
        }

        // Emit one BLOCK_SIZE-capped range per missing block. Backends
        // that benefit from coalescing (e.g. high-latency object
        // stores) can merge contiguous ranges inside their own
        // `read_batch`; backends that benefit from keeping them
        // separate (e.g. io_uring, where queue depth is the source of
        // parallelism) can submit them individually.
        let len_bytes = self.len_bytes;
        let ranges = to_fetch.iter().map(|block| {
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

        self.remote
            .read_batch::<Sequential, _>(ranges, |block, bytes| {
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

fn out_of_bounds<T>(start: u64, end: u64, len_bytes: u64) -> UniversalIoError {
    let t_size = size_of::<T>().max(1) as u64;
    UniversalIoError::OutOfBounds {
        start,
        end,
        elements: (len_bytes / t_size) as usize,
    }
}
