use std::borrow::Cow;
use std::fmt::Debug;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use parking_lot::Mutex;

use super::BLOCK_SIZE;
use super::config::DiskCacheConfig;
use crate::generic_consts::{AccessPattern, Sequential};
use crate::mmap::{AdviceSetting, Madviseable};
use crate::universal_io::simple_disk_cache::local_state::LocalState;
use crate::universal_io::simple_disk_cache::pipeline::{DiskCachePipeline, OwnedDiskCachePipeline};
use crate::universal_io::simple_disk_cache::to_block_range;
use crate::universal_io::{
    Item, OpenOptions, OpenOptionsExtra, OwnedReadPipeline, Populate, ReadRange, Result,
    UniversalIoError, UniversalKind, UniversalRead, UniversalReadFileOps, UserData,
};

/// A lazily-populated local mirror of an immutable remote file.
///
/// The remote is assumed to be immutable for the lifetime of the file;
/// this type implements [`UniversalRead`] only, but not [`UniversalWrite`].
///
/// The local mirror can either be initialized lazily on first read (filling
/// blocks on demand from the remote) or eagerly if populate is set.
///
/// WARN: There should be only a single instance of DiskCache per path.
/// Initializing multiple instances will try to re-read from remote. Cloning is
/// safe because of Arcs, though.
#[derive(Clone)]
pub struct DiskCache<R>
where
    R: UniversalRead,
{
    /// Path to the remote file. Used to lazily open `remote`.
    remote_path: PathBuf,
    /// Lazily-opened remote handle. Only initialized when needed (cache miss
    /// or `len()` before local is set).
    ///
    // We could switch to LazyLock, but there is no fallible initialization
    remote: OnceLock<R>,
    /// Open options for when the local mmap is initialized.
    pub(super) open_options: OpenOptions,
    /// Path to the local mmap file.
    pub(super) local_path: PathBuf,
    /// Lazily-initialized local mirror.
    pub(super) local: Arc<OnceLock<LocalState>>,
    /// Guards initialization of `local` and carries the source of init.
    init_lock: Arc<Mutex<InitSource<R>>>,
}

impl<R> Debug for DiskCache<R>
where
    R: UniversalRead,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskCache")
            .field("remote_path", &self.remote_path)
            .field("remote", &self.remote)
            .field("open_options", &self.open_options)
            .field("local_path", &self.local_path)
            .field("local", &self.local)
            .finish_non_exhaustive()
    }
}

/// Where the [`LocalState`] comes from on first init.
enum InitSource<R: UniversalRead> {
    /// Build an empty local mmap and let reads fill blocks on demand.
    FromScratch,
    /// Wait for the prefill pipeline.
    FromPrefiller(R::OwnedReadPipeline<u8, Range<u32>>),
}

impl<R> DiskCache<R>
where
    R: UniversalRead,
{
    /// Open a [`DiskCache`] with an explicit configuration.
    pub fn open_with_config(
        config: &DiskCacheConfig,
        remote_path: impl AsRef<Path>,
        options: OpenOptions,
    ) -> Result<Self> {
        if options.writeable {
            return Err(UniversalIoError::Uninitialized {
                description:
                    "DiskCache only supports immutable files, writeable option is not allowed"
                        .to_string(),
            });
        }

        let local_path = config.local_path_for(remote_path.as_ref())?;

        let populate = if crate::low_memory::low_memory_mode().skip_populate() {
            Populate::No
        } else {
            options.populate
        };

        let init_source = match populate {
            Populate::Auto | Populate::No => InitSource::FromScratch,
            Populate::Blocking | Populate::PreferBackground => {
                let remote = R::open(
                    &remote_path,
                    OpenOptions {
                        writeable: false,
                        need_sequential: true,
                        populate: Populate::No,
                        advice: AdviceSetting::Global,
                        extra: OpenOptionsExtra {
                            prevent_caching: true,
                        },
                    },
                )?;

                let remote_len = remote.len::<u8>()?;

                let range = ReadRange {
                    byte_offset: 0,
                    length: remote_len,
                };

                let blocks_range = to_block_range(0..remote_len);

                let mut pipeline = R::OwnedReadPipeline::new(remote)?;

                // FIXME: check `can_schedule` in a loop first
                pipeline.schedule::<Sequential>(blocks_range, range)?;

                InitSource::FromPrefiller(pipeline)
            }
        };

        let cache = {
            let remote_path = remote_path.as_ref().to_owned();
            Self {
                remote_path,
                remote: OnceLock::new(),
                open_options: options,
                local_path,
                local: Arc::new(OnceLock::new()),
                init_lock: Arc::new(Mutex::new(init_source)),
            }
        };

        if matches!(populate, Populate::Blocking) {
            // Force the prefill to resolve before returning.
            cache.local_state()?;
        }

        Ok(cache)
    }

    pub(super) fn new(
        remote_path: impl AsRef<Path>,
        local_path: PathBuf,
        options: OpenOptions,
    ) -> Self {
        {
            Self {
                remote_path: remote_path.as_ref().to_owned(),
                remote: OnceLock::new(),
                open_options: options,
                local_path,
                local: Arc::new(OnceLock::new()),
                init_lock: Arc::new(Mutex::new(InitSource::FromScratch)),
            }
        }
    }

    /// Lazily open the remote handle. The remote is only needed for cache
    /// misses and for `len()` before the local mmap exists.
    pub(super) fn remote(&self) -> Result<&R> {
        if let Some(r) = self.remote.get() {
            return Ok(r);
        }

        let remote_options = OpenOptions {
            writeable: false,
            extra: OpenOptionsExtra {
                prevent_caching: true,
            },
            populate: Populate::No,
            need_sequential: false,
            advice: AdviceSetting::Global,
        };

        let opened = R::open(&self.remote_path, remote_options)?;
        // If another thread set this concurrently, let our R be dropped.
        //
        // OnceLock::get_or_try_init would be better but it is not available on stable
        let _ = self.remote.set(opened);
        Ok(self.remote.get().expect("just set or already set"))
    }

    /// Return the cached [`LocalState`], initializing it on first call.
    pub(super) fn local_state(&self) -> Result<&LocalState> {
        if let Some(state) = self.local.get() {
            return Ok(state);
        }

        // Only the first thread is able to initialize.
        let mut guard = self.init_lock.lock();
        if let Some(state) = self.local.get() {
            return Ok(state);
        }

        let local = match std::mem::replace(&mut *guard, InitSource::FromScratch) {
            InitSource::FromScratch => self.init_local_state_from_scratch()?,
            InitSource::FromPrefiller(mut pipe) => {
                match pipe.wait()? {
                    Some((blocks_range, bytes)) => {
                        let local = LocalState::new(
                            &self.local_path,
                            // TODO: if we want partial prefill, we should still create local state with
                            // entire length of remote file, not the partial data.
                            bytes.len() as u64,
                            self.open_options,
                        )?;
                        unsafe { local.write_mmap_bytes(&bytes, blocks_range.clone()) };
                        local.mark_fully_populated(blocks_range.end);
                        local
                    }
                    None => {
                        debug_assert!(
                            false,
                            "Looks like the request for prefill bytes was incorrect"
                        );
                        // init from scratch
                        self.init_local_state_from_scratch()?
                    }
                }
            }
        };

        self.local
            .set(local)
            .expect("OnceLock::set must succeed while holding init_lock");

        Ok(self.local.get().expect("just initialized"))
    }
    fn init_local_state_from_scratch(&self) -> Result<LocalState> {
        let len = self.remote()?.len::<u8>()?;
        LocalState::new(&self.local_path, len, self.open_options)
    }
}

impl<R> UniversalReadFileOps for DiskCache<R>
where
    R: UniversalRead,
{
    fn list_files(prefix_path: &Path) -> Result<Vec<PathBuf>> {
        R::list_files(prefix_path)
    }

    fn exists(path: &Path) -> Result<bool> {
        R::exists(path)
    }
}

impl<R> UniversalRead for DiskCache<R>
where
    R: UniversalRead + Clone,
{
    type BorrowedReadPipeline<'a, T, U>
        = DiskCachePipeline<'a, R, T, U>
    where
        R: 'a,
        T: Item,
        U: UserData;

    type OwnedReadPipeline<T, U>
        = OwnedDiskCachePipeline<R, T, U>
    where
        T: Item,
        U: UserData;

    fn open(path: impl AsRef<Path>, options: OpenOptions) -> Result<Self> {
        let config = DiskCacheConfig::global().ok_or_else(|| {
            UniversalIoError::uninitialized(
                "DiskCacheConfig must be initialized via `DiskCacheConfig::initialize_global` \
                 before opening an DiskCache",
            )
        })?;
        let local_path = config.local_path_for(path.as_ref())?;
        Ok(Self::new(path.as_ref(), local_path, options))
    }

    fn reopen(&mut self) -> Result<()> {
        todo!();
    }

    fn read<P, T>(&self, range: ReadRange) -> Result<Cow<'_, [T]>>
    where
        P: AccessPattern,
        T: Item,
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
            self.remote()?.len::<u8>()?
        };

        Ok(bytes_len / t_size as u64)
    }

    fn populate(&self) -> Result<()> {
        if crate::low_memory::low_memory_mode().skip_populate() {
            return Ok(());
        }

        let remote_len = self.remote()?.len::<u8>()?;
        if remote_len == 0 {
            return Ok(());
        }

        let one_byte_per_block = (0..remote_len as usize)
            .step_by(BLOCK_SIZE)
            .map(|byte_offset| ((), ReadRange::one(byte_offset as u64)));

        for result in self.read_iter::<Sequential, u8, ()>(one_byte_per_block)? {
            result?;
        }

        let local = self.local.get().expect("just populated it");
        let last_block = remote_len.div_ceil(BLOCK_SIZE as u64) as u32;
        local.mark_fully_populated(last_block);

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
