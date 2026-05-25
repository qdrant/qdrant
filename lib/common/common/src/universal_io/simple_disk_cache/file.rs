use std::borrow::Cow;
use std::fmt::Debug;
use std::io::{self, ErrorKind};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use parking_lot::Mutex;

use super::BLOCK_SIZE;
use super::config::DiskCacheConfig;
use crate::generic_consts::{AccessPattern, Sequential};
use crate::mmap::AdviceSetting;
use crate::universal_io::simple_disk_cache::local_state::LocalState;
use crate::universal_io::simple_disk_cache::pipeline::{DiskCachePipeline, OwnedDiskCachePipeline};
use crate::universal_io::simple_disk_cache::to_block_range;
use crate::universal_io::{
    Item, OpenOptions, OwnedReadPipeline, Populate, ReadRange, Result, UniversalIoError,
    UniversalKind, UniversalRead, UniversalReadFileOps, UniversalReadFs, UserData,
};

/// Filesystem handle for the simple disk cache. Carries the remote-side
/// filesystem used to fetch missing blocks.
///
/// `DiskCacheFs` is parameterized by the remote *file* type `R`; the
/// underlying remote filesystem is `R::Fs`. This indirection lets the
/// `UniversalRead::Fs = DiskCacheFs<R>` constraint on
/// [`DiskCache<R>`] line up without an extra generic parameter on the
/// file type.
pub struct DiskCacheFs<R>
where
    R: UniversalRead,
{
    remote_fs: R::Fs,
}

impl<R> Clone for DiskCacheFs<R>
where
    R: UniversalRead,
    R::Fs: Clone,
{
    fn clone(&self) -> Self {
        Self {
            remote_fs: self.remote_fs.clone(),
        }
    }
}

impl<R> Debug for DiskCacheFs<R>
where
    R: UniversalRead,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskCacheFs")
            .field("remote_fs", &self.remote_fs)
            .finish()
    }
}

impl<R> UniversalReadFileOps for DiskCacheFs<R>
where
    R: UniversalRead,
{
    type ContextConfig = <R::Fs as UniversalReadFileOps>::ContextConfig;

    fn from_context(ctx: Self::ContextConfig) -> Result<Self> {
        Ok(Self {
            remote_fs: R::Fs::from_context(ctx)?,
        })
    }

    fn list_files(&self, prefix_path: &Path) -> Result<Vec<PathBuf>> {
        self.remote_fs.list_files(prefix_path)
    }

    fn exists(&self, path: &Path) -> Result<bool> {
        self.remote_fs.exists(path)
    }
}

impl<R> UniversalReadFs for DiskCacheFs<R>
where
    R: UniversalRead + Clone,
    R::Fs: Clone + Send + Sync,
    <R::Fs as UniversalReadFs>::OpenExtra: Clone + Send + Sync,
    R::OwnedReadPipeline<u8, Range<u32>>: Send,
{
    type File = DiskCache<R>;
    type OpenExtra = <R::Fs as UniversalReadFs>::OpenExtra;

    fn open(
        &self,
        path: impl AsRef<Path>,
        options: OpenOptions,
        extra: Self::OpenExtra,
    ) -> Result<DiskCache<R>> {
        let config = DiskCacheConfig::global().ok_or_else(|| {
            UniversalIoError::uninitialized(
                "DiskCacheConfig must be initialized via `DiskCacheConfig::initialize_global` \
                 before opening a DiskCache",
            )
        })?;
        let local_path = config.local_path_for(path.as_ref())?;
        Ok(DiskCache::new(
            self.remote_fs.clone(),
            extra,
            path.as_ref(),
            local_path,
            options,
        ))
    }
}

/// A lazily-populated local mirror of an immutable remote file.
///
/// The remote is assumed to be immutable for the lifetime of the file;
/// this type implements [`UniversalRead`] only, but not [`UniversalWrite`].
///
/// The local mirror can either be initialized lazily on first read (filling
/// blocks on demand from the remote) or eagerly if populate is set.
///
/// WARN: There should be only a single instance of DiskCache per path.
/// Initializing multiple instances will try to re-read from remote.
pub struct DiskCache<R>
where
    R: UniversalRead,
{
    /// Clone of the remote filesystem handle, used to lazily open `remote`.
    remote_fs: R::Fs,
    /// Backend-specific per-open extras for the remote.
    remote_extra: <R::Fs as UniversalReadFs>::OpenExtra,
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
    pub(super) local: OnceLock<LocalState>,
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
    R: UniversalRead + Clone,
    R::Fs: Clone + Send + Sync,
    <R::Fs as UniversalReadFs>::OpenExtra: Clone + Send + Sync,
    R::OwnedReadPipeline<u8, Range<u32>>: Send,
{
    /// Open a [`DiskCache`] with an explicit configuration.
    pub fn open_with_config(
        config: &DiskCacheConfig,
        remote_fs: R::Fs,
        remote_extra: <R::Fs as UniversalReadFs>::OpenExtra,
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
                let remote = remote_fs.open(
                    &remote_path,
                    OpenOptions {
                        writeable: false,
                        need_sequential: true,
                        populate: Populate::No,
                        advice: AdviceSetting::Global,
                    },
                    remote_extra.clone(),
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

        let cache = Self {
            remote_fs,
            remote_extra,
            remote_path: remote_path.as_ref().to_owned(),
            remote: OnceLock::new(),
            open_options: options,
            local_path,
            local: OnceLock::new(),
            init_lock: Arc::new(Mutex::new(init_source)),
        };

        if matches!(populate, Populate::Blocking) {
            // Force the prefill to resolve before returning.
            cache.local_state()?;
        }

        Ok(cache)
    }

    pub(super) fn new(
        remote_fs: R::Fs,
        remote_extra: <R::Fs as UniversalReadFs>::OpenExtra,
        remote_path: impl AsRef<Path>,
        local_path: PathBuf,
        options: OpenOptions,
    ) -> Self {
        Self {
            remote_fs,
            remote_extra,
            remote_path: remote_path.as_ref().to_owned(),
            remote: OnceLock::new(),
            open_options: options,
            local_path,
            local: OnceLock::new(),
            init_lock: Arc::new(Mutex::new(InitSource::FromScratch)),
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
            populate: Populate::No,
            need_sequential: false,
            advice: AdviceSetting::Global,
        };

        let opened = self
            .remote_fs
            .open(&self.remote_path, remote_options, self.remote_extra.clone())?;
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

        self.init_local_state(true)?;

        Ok(self.local.get().expect("just initialized"))
    }

    /// Initialize the local state depending on `InitSource`
    ///
    /// If `allow_from_scratch` is false, this method will avoid initializing if `InitSource::FromScratch` is set.
    /// This is helpful for [`Self::reopen`] scenario where we can avoid work if no reads have taken place.
    fn init_local_state(&self, allow_from_scratch: bool) -> Result<()> {
        // Only the first thread is able to initialize.
        let mut guard = self.init_lock.lock();
        if self.local.get().is_some() {
            return Ok(());
        }

        let local = match std::mem::replace(&mut *guard, InitSource::FromScratch) {
            InitSource::FromScratch => {
                if !allow_from_scratch {
                    return Ok(());
                }
                self.new_local_state_from_scratch()?
            }
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
                        unsafe { local.write_mmap_bytes(&bytes, blocks_range) };
                        local
                    }
                    None => {
                        debug_assert!(
                            false,
                            "Looks like the request for prefill bytes was incorrect"
                        );
                        if !allow_from_scratch {
                            return Ok(());
                        }
                        // init from scratch
                        self.new_local_state_from_scratch()?
                    }
                }
            }
        };

        self.local
            .set(local)
            .expect("OnceLock::set must succeed while holding init_lock");

        Ok(())
    }

    fn new_local_state_from_scratch(&self) -> Result<LocalState> {
        let len = self.remote()?.len::<u8>()?;
        LocalState::new(&self.local_path, len, self.open_options)
    }

    /// Make sure every byte in the range `byte_start..remote_len` is present on the local file
    fn populate_from(&self, byte_start: u64) -> std::result::Result<(), UniversalIoError> {
        if crate::low_memory::low_memory_mode().skip_populate() {
            return Ok(());
        }

        let remote_len = self.remote()?.len::<u8>()?;
        if remote_len == 0 {
            return Ok(());
        }

        let one_byte_per_block = (byte_start..remote_len)
            .step_by(BLOCK_SIZE)
            .map(|byte_offset| ((), ReadRange::one(byte_offset)));

        for result in self.read_iter::<Sequential, u8, ()>(one_byte_per_block)? {
            result?;
        }

        Ok(())
    }
}

impl<R> UniversalRead for DiskCache<R>
where
    R: UniversalRead + Clone,
    R::Fs: Clone + Send + Sync,
    <R::Fs as UniversalReadFs>::OpenExtra: Clone + Send + Sync,
    R::OwnedReadPipeline<u8, Range<u32>>: Send,
{
    type Fs = DiskCacheFs<R>;

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

    fn reopen(&mut self) -> Result<()> {
        // Wait for InitSource::Prefill, if set.
        self.init_local_state(false)?;

        if self.local.get().is_none() {
            return Ok(());
        }

        // Reopen the remote so `len()` reflects the current file size
        if let Some(remote) = self.remote.get_mut() {
            remote.reopen()?;
        }
        let remote_len = self.remote()?.len::<u8>()?;

        let local = self
            .local
            .get_mut()
            .expect("We just ruled out `is_none` above, and we are holding &mut self");

        let local_len = local.mmap().len::<u8>()?;
        if local_len > remote_len {
            return Err(UniversalIoError::Io(io::Error::new(
                ErrorKind::UnexpectedEof,
                format!(
                    "Reopen encountered a smaller file than expected; old_len: {local_len}, new_len: {remote_len}"
                ),
            )));
        }
        if local_len == remote_len {
            return Ok(());
        }

        local.resize(self.local_path.clone(), remote_len)?;

        match self.open_options.populate {
            Populate::Auto | Populate::No => {}
            Populate::Blocking => self.populate_from(local_len)?,
            Populate::PreferBackground => {
                // TODO: prefill old_len..new_len on background
            }
        }

        Ok(())
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
        let len = if let Some(local) = self.local.get() {
            local.mmap().len::<T>()?
        } else {
            self.remote()?.len::<T>()?
        };

        Ok(len)
    }

    fn populate(&self) -> Result<()> {
        self.populate_from(0)
    }

    fn clear_ram_cache(&self) -> Result<()> {
        if let Some(state) = self.local.get() {
            state.mmap().clear_ram_cache()?;
        }
        Ok(())
    }

    fn kind() -> UniversalKind {
        UniversalKind::SimpleDiskCache
    }
}
