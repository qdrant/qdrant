use std::borrow::Cow;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use fs_err as fs;
use memmap2::MmapRaw;
use parking_lot::Mutex;
use roaring::RoaringBitmap;

use super::BLOCK_SIZE;
use super::config::DiskCacheConfig;
use crate::generic_consts::{AccessPattern, Sequential};
use crate::mmap::{AdviceSetting, Madviseable};
use crate::universal_io::pipeline::DiskCachePipeline;
use crate::universal_io::{
    OpenOptions, Populate, ReadRange, Result, UniversalIoError, UniversalKind, UniversalRead, UniversalReadFileOps
};

/// A lazily-populated local mirror of an immutable remote file.
///
/// The remote is assumed to be immutable for the lifetime of the file;
/// this type implements [`UniversalRead`] only, but not [`UniversalWrite`].
///
/// WARN: There should be only a single instance of DiskCache per path.
/// Initializing multiple instances will not reuse the same mmap and can cause UB.
#[derive(Debug)]
pub struct DiskCache<R> {
    pub(super) remote: R,
    /// Open options for when it gets initialized
    open_options: OpenOptions,
    /// Path to the local mmap file
    local_path: PathBuf,
    pub(super) local: OnceLock<LocalState>,
    /// Prevents concurrent initialization of `local` across threads.
    ///
    /// TODO: Switch to [`OnceLock::get_or_try_init`][1] once it stabilizes
    ///
    /// [1]: https://github.com/rust-lang/rust/issues/109737
    init_lock: Mutex<()>,
}

#[derive(Debug)]
pub(super) struct LocalState {
    pub mmap: MmapRaw,
    /// Bitmask to know which blocks have been fetched so far.
    pub fetched: Mutex<RoaringBitmap>,
}

impl LocalState {
    /// # Safety
    /// `byte_range` must have been populated first, caller must ensure `self.fetched` references the
    /// blocks for the byte range.
    pub(super) unsafe fn read_mmap_bytes(&self, byte_range: Range<u64>) -> &[u8] {
        let bytes = unsafe { std::slice::from_raw_parts(self.mmap.as_ptr(), self.mmap.len()) };
        &bytes[byte_range.start as usize..byte_range.end as usize]
    }

    /// # Safety
    /// `DiskCache` is only used in immutable files. Since `blocks_range` can include already-fetched
    /// data, it is possible that some sections get overwritten; however, it should be the same data,
    /// so it is fine.
    ///
    /// Assumes the bytes slice covers the entirety of `blocks_range`.
    pub(super) unsafe fn write_mmap_bytes(&self, bytes: &[u8], blocks_range: Range<u32>) {
        let mut fetched = self.fetched.lock();
        if fetched.contains_range(blocks_range.clone()) {
            return;
        }

        let byte_offset = blocks_range.start as usize * BLOCK_SIZE;

        let max_len = self.mmap.len().saturating_sub(byte_offset);
        assert!(bytes.len() == max_len.min(blocks_range.len() * BLOCK_SIZE));

        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                self.mmap.as_mut_ptr().add(byte_offset),
                bytes.len(),
            );
        }
        fetched.insert_range(blocks_range);
    }
}

impl<R: UniversalRead> DiskCache<R> {
    /// Open an [`DiskCache`] with an explicit configuration
    pub fn open_with_config(
        config: &DiskCacheConfig,
        path: impl AsRef<Path>,
        options: OpenOptions,
    ) -> Result<Self> {
        if options.writeable {
            return Err(UniversalIoError::Uninitialized {
                description:
                    "DiskCache only supports immutable files, writeable option is not allowed"
                        .to_string(),
            });
        }

        let remote_path = path.as_ref();
        let local_path = config.local_path_for(remote_path)?;

        let remote_options = OpenOptions {
            writeable: false,
            prevent_caching: Some(true),
            populate: Populate::No,
            need_sequential: false,
            disk_parallel: None,
            advice: None,
        };

        let remote = R::open(remote_path, remote_options)?;

        Ok(Self {
            remote,
            open_options: options,
            local_path,
            local: OnceLock::new(),
            init_lock: Mutex::new(()),
        })
    }

    /// Return the cached [`LocalState`], initializing it on first call.
    pub(super) fn local_state(&self) -> Result<&LocalState> {
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

        let remote_len = self.remote.len::<u8>()?;
        file.set_len(remote_len)?;
        let mmap = MmapRaw::map_raw(&file)?;

        mmap.madvise(advice.unwrap_or(AdviceSetting::Global).resolve())?;

        Ok(LocalState {
            mmap,
            fetched: Mutex::new(RoaringBitmap::new()),
        })
    }
}

impl<R: UniversalReadFileOps> UniversalReadFileOps for DiskCache<R> {
    fn list_files(prefix_path: &Path) -> Result<Vec<PathBuf>> {
        R::list_files(prefix_path)
    }

    fn exists(path: &Path) -> Result<bool> {
        R::exists(path)
    }
}

impl<R> UniversalRead for DiskCache<R>
where
    R: UniversalRead,
{
    type ReadPipeline<'a, T, Meta>
        = DiskCachePipeline<'a, T, Meta, R>
    where
        R: 'a,
        T: bytemuck::Pod;

    fn open(path: impl AsRef<Path>, options: OpenOptions) -> Result<Self> {
        let config = DiskCacheConfig::global().ok_or_else(|| {
            UniversalIoError::uninitialized(
                "DiskCacheConfig must be initialized via `DiskCacheConfig::initialize_global` \
                 before opening an DiskCache",
            )
        })?;
        Self::open_with_config(config, path, options)
    }

    fn read<P, T>(&self, range: ReadRange) -> Result<Cow<'_, [T]>>
    where
        P: AccessPattern,
        T: bytemuck::Pod,
    {
        let (_, read) = self
            .read_iter::<P, T, _>(std::iter::once(((), range)))?
            .next()
            .expect("there's exactly one read")?;

        Ok(read)
    }

    fn len<T>(&self) -> Result<u64> {
        let t_size = size_of::<T>();
        debug_assert!(t_size > 0, "zero-sized types are not supported");

        let bytes_len = if let Some(local) = self.local.get() {
            local.mmap.len() as u64
        } else {
            self.remote.len::<u8>()?
        };

        Ok(bytes_len / t_size as u64)
    }

    fn populate(&self) -> Result<()> {
        if crate::low_memory::low_memory_mode().skip_populate() {
            return Ok(());
        }

        let remote_len = self.remote.len::<u8>()?;
        if remote_len == 0 {
            return Ok(());
        }

        let one_byte_per_block = (0..remote_len as usize)
            .step_by(BLOCK_SIZE)
            .map(|byte_offset| ((), ReadRange::one(byte_offset as u64)));

        for result in self.read_iter::<Sequential, u8, ()>(one_byte_per_block)? {
            result?;
        }

        Ok(())
    }

    fn clear_ram_cache(&self) -> Result<()> {
        if let Some(state) = self.local.get() {
            state.mmap.clear_cache();
        }
        Ok(())
    }

    fn kind() -> UniversalKind {
        UniversalKind::SimpleDiskCache
    }
}

pub(super) fn to_block_range(range: Range<u64>) -> Range<u32> {
    let start = (range.start / BLOCK_SIZE as u64) as u32;
    let end = range.end.div_ceil(BLOCK_SIZE as u64) as u32;
    start..end
}
