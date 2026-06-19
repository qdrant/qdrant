use std::fmt::Debug;
use std::io::{self, ErrorKind};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use parking_lot::{Mutex, MutexGuard};

use super::BLOCK_SIZE;
use super::fs::DiskCacheFs;
use crate::ext::aligned_vec::ACow;
use crate::generic_consts::{AccessPattern, Sequential};
use crate::mmap::AdviceSetting;
use crate::universal_io::simple_disk_cache::local_state::LocalState;
use crate::universal_io::simple_disk_cache::pipeline::{DiskCachePipeline, OwnedDiskCachePipeline};
use crate::universal_io::simple_disk_cache::{DiskCacheRemote, to_block_range};
use crate::universal_io::{
    BorrowedReadPipeline, OpenOptions, OwnedReadPipeline, Populate, ReadRange, Result,
    UniversalIoError, UniversalKind, UniversalRead, UniversalReadFs, UserData,
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
    pub(super) init_lock: Arc<Mutex<InitSource<R>>>,
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
pub(super) enum InitSource<R: UniversalRead> {
    /// Build an empty local mmap and let reads fill blocks on demand.
    FromScratch,
    /// Wait for the prefill pipeline.
    FromPrefiller(R::OwnedReadPipeline<()>),
}

impl<R> DiskCache<R>
where
    R: DiskCacheRemote,
{
    pub(super) fn new(
        remote_fs: R::Fs,
        remote_extra: <R::Fs as UniversalReadFs>::OpenExtra,
        remote_path: impl AsRef<Path>,
        local_path: PathBuf,
        options: OpenOptions,
        init_source: InitSource<R>,
    ) -> Self {
        Self {
            remote_fs,
            remote_extra,
            remote_path: remote_path.as_ref().to_owned(),
            remote: OnceLock::new(),
            open_options: options,
            local_path,
            local: OnceLock::new(),
            init_lock: Arc::new(Mutex::new(init_source)),
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

        let opened =
            self.remote_fs
                .open(&self.remote_path, remote_options, self.remote_extra.clone())?;
        // If another thread set this concurrently, let our R be dropped.
        //
        // OnceLock::get_or_try_init would be better but it is not available on stable
        let _ = self.remote.set(opened);
        Ok(self.remote.get().expect("just set or already set"))
    }

    /// Drop the cached remote handle, releasing any mapping it holds on the
    /// remote file. The next access re-opens it lazily via [`Self::remote`].
    ///
    /// Test-only: required so tests can shrink the remote on Windows, where a
    /// file with an active memory mapping cannot be resized.
    #[cfg(test)]
    pub(super) fn release_remote(&mut self) {
        self.remote = OnceLock::new();
    }

    /// Return the cached [`LocalState`], initializing it on first call.
    pub(super) fn local_state(&self) -> Result<&LocalState> {
        if let Some(state) = self.local.get() {
            return Ok(state);
        }

        let mut init_guard = self.init_lock.lock();

        // Try again now that we have the lock, in case another thread initialized it first.
        if self.local.get().is_none() {
            self.init_local_state(&mut init_guard, true, None)?;
        }

        Ok(self.local.get().expect("just initialized"))
    }

    /// Initialize the local state depending on `InitSource`
    ///
    /// If `allow_from_scratch` is false, this method will avoid initializing if `InitSource::FromScratch` is set.
    /// This is helpful for [`Self::reopen`] scenario where we can avoid work if no reads have taken place.
    pub(super) fn init_local_state(
        &self,
        init_guard: &mut MutexGuard<'_, InitSource<R>>,
        allow_from_scratch: bool,
        known_length: Option<u64>,
    ) -> Result<()> {
        // Only the first thread is able to initialize.

        let local = match std::mem::replace(&mut **init_guard, InitSource::FromScratch) {
            InitSource::FromScratch => {
                if !allow_from_scratch {
                    return Ok(());
                }
                self.new_local_state_from_scratch(known_length)?
            }
            InitSource::FromPrefiller(mut pipe) => {
                match pipe.wait()? {
                    Some((_, bytes)) => {
                        let local = LocalState::new(
                            &self.local_path,
                            // TODO: if we want partial prefill, we should still create local state with
                            // entire length of remote file, not the partial data.
                            bytes.len() as u64,
                            self.open_options,
                        )?;
                        let blocks_range = to_block_range(0..bytes.len() as u64);
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
                        self.new_local_state_from_scratch(known_length)?
                    }
                }
            }
        };

        self.local
            .set(local)
            .expect("OnceLock::set must succeed while holding init_lock");

        Ok(())
    }

    fn new_local_state_from_scratch(&self, known_length: Option<u64>) -> Result<LocalState> {
        let len = match known_length {
            Some(len) => len,
            None => self.remote()?.len::<u8>()?,
        };
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
    R: DiskCacheRemote,
{
    type Fs = DiskCacheFs<R>;

    type BorrowedReadPipeline<'a, U>
        = DiskCachePipeline<'a, R, U>
    where
        R: 'a,
        Self: 'a,
        U: UserData;

    type OwnedReadPipeline<U>
        = OwnedDiskCachePipeline<R, U>
    where
        U: UserData;

    fn reopen(&mut self) -> Result<()> {
        // Wait for InitSource::Prefill, if set.
        let mut init_guard = self.init_lock.lock();
        self.init_local_state(&mut init_guard, false, None)?;

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

    fn read_bytes<P: AccessPattern>(&self, range: Range<u64>, align: usize) -> Result<ACow<'_>> {
        let mut pipeline = DiskCachePipeline::<R, ()>::new()?;
        pipeline.schedule::<P>((), self, range, align)?;
        let (_, bytes) = pipeline.wait()?.expect("there's exactly one read");
        Ok(bytes)
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

    fn populate_auto() -> bool {
        false
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
