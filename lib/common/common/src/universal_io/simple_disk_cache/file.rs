use std::assert_matches;
use std::fmt::Debug;
use std::io::{self, ErrorKind};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use parking_lot::Mutex;

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
    /// Open options for when the local mmap is initialized.
    pub(super) open_options: OpenOptions,
    /// Path to the local mmap file.
    pub(super) local_path: PathBuf,
    /// Lazily-initialized mirror.
    // We could switch to LazyLock, but there is no fallible initialization
    pub(super) state: OnceLock<State<R>>,
    /// Guards initialization of `local` and carries the source of init.
    pub(super) init_lock: Arc<Mutex<InitSource<R>>>,
}

#[derive(Debug)]
pub(super) struct State<R> {
    pub remote: R,
    pub local: LocalState,
}

impl<R> Debug for DiskCache<R>
where
    R: UniversalRead,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskCache")
            .field("remote_fs", &self.remote_fs)
            .field("remote_path", &self.remote_path)
            .field("open_options", &self.open_options)
            .field("local_path", &self.local_path)
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

/// Where the [`LocalState`] comes from on first init.
pub(super) enum InitSource<R: UniversalRead> {
    /// Build an empty local mmap and let reads fill blocks on demand.
    FromScratch,
    /// Wait for the prefill pipeline.
    Prefiller(R::OwnedReadPipeline<()>),
    /// Wait for the prefill pipeline, but from reopen
    PartialPrefiller {
        prefiller: R::OwnedReadPipeline<u64>,
        local_state: LocalState,
    },
}

impl<R> Debug for InitSource<R>
where
    R: UniversalRead,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InitSource::FromScratch => write!(f, "FromScratch"),
            InitSource::Prefiller(_) => write!(f, "Prefiller"),
            InitSource::PartialPrefiller { .. } => write!(f, "PartialPrefiller"),
        }
    }
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
            open_options: options,
            local_path,
            state: OnceLock::new(),
            init_lock: Arc::new(Mutex::new(init_source)),
        }
    }

    pub(super) fn open_remote(&self) -> Result<R> {
        let remote_options = OpenOptions {
            writeable: false,
            populate: Populate::No,
            need_sequential: false,
            advice: AdviceSetting::Global,
        };

        self.remote_fs
            .open(&self.remote_path, remote_options, self.remote_extra.clone())
    }

    /// Return the cached [`State`], initializing it on first call.
    pub(super) fn state(&self) -> Result<&State<R>> {
        if let Some(state) = self.state.get() {
            return Ok(state);
        }

        let mut init_guard = self.init_lock.lock();

        // Try again now that we have the lock, in case another thread initialized it first.
        if self.state.get().is_none() {
            self.init_state(&mut init_guard, true, None)?;
        }

        Ok(self.state.get().expect("just initialized"))
    }

    /// Initialize the local state depending on `InitSource`
    ///
    /// If `allow_from_scratch` is false, this method will avoid initializing if `InitSource::FromScratch` is set.
    /// This is helpful for [`Self::reopen`] scenario where we can avoid work if no reads have taken place.
    pub(super) fn init_state(
        &self,
        init_guard: &mut InitSource<R>,
        allow_from_scratch: bool,
        known_length: Option<u64>,
    ) -> Result<()> {
        let state = match std::mem::replace(init_guard, InitSource::FromScratch) {
            InitSource::FromScratch => {
                if !allow_from_scratch {
                    return Ok(());
                }
                self.new_state_from_scratch(self.open_remote()?, known_length)?
            }
            InitSource::Prefiller(mut prefiller) => {
                match prefiller.wait()? {
                    Some((_, bytes)) => {
                        let local = LocalState::new(
                            &self.local_path,
                            // bytes length is the length of the remote file
                            bytes.len() as u64,
                            self.open_options,
                        )?;
                        let blocks_range = to_block_range(0..bytes.len() as u64);
                        unsafe { local.write_mmap_bytes(&bytes, blocks_range) };
                        State {
                            local,
                            remote: prefiller.into_inner(),
                        }
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
                        self.new_state_from_scratch(prefiller.into_inner(), known_length)?
                    }
                }
            }
            InitSource::PartialPrefiller {
                mut prefiller,
                mut local_state,
            } => {
                match prefiller.wait()? {
                    Some((start, bytes)) => {
                        let end = start + bytes.len() as u64;

                        local_state.resize(&self.local_path, end)?;

                        let blocks_range = to_block_range(start..end);
                        unsafe { local_state.write_mmap_bytes(&bytes, blocks_range) }
                    }
                    None => {
                        // The remote file didn't grow
                        // TODO: double check that the remote didn't shrink?
                    }
                };

                let remote = prefiller.into_inner();

                State {
                    remote,
                    local: local_state,
                }
            }
        };

        self.state
            .set(state)
            .expect("OnceLock::set must succeed while holding init_lock");

        Ok(())
    }

    fn new_state_from_scratch(&self, remote: R, known_length: Option<u64>) -> Result<State<R>> {
        let len = if let Some(len) = known_length {
            len
        } else {
            remote.len::<u8>()?
        };
        let local = LocalState::new(&self.local_path, len, self.open_options)?;
        Ok(State { remote, local })
    }

    /// Make sure every byte in the range `byte_start..remote_len` is present on the local file
    fn populate_from(&self, byte_start: u64) -> std::result::Result<(), UniversalIoError> {
        if crate::low_memory::low_memory_mode().skip_populate() {
            return Ok(());
        }

        let remote_len = self.state()?.remote.len::<u8>()?;
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
        self.init_state(&mut init_guard, false, None)?;

        let Some(State {
            mut remote,
            mut local,
        }) = self.state.take()
        else {
            // If `self.state` didn't initialize after `init_state`, we are not populating
            // and we haven't made any reads.
            //
            // The first read will take care of initializing to the remote length.
            return Ok(());
        };

        // Reopen remote so it reflects current length
        remote.reopen()?;

        let local_len = local.mmap().len::<u8>()?;

        match self.open_options.populate {
            Populate::Auto | Populate::No => {
                let remote_len = remote.len::<u8>()?;

                // The remote is assumed to be append-only; a smaller file is unexpected.
                if local_len > remote_len {
                    return Err(UniversalIoError::Io(io::Error::new(
                        ErrorKind::UnexpectedEof,
                        format!(
                            "Reopen encountered a smaller file than expected; old_len: {local_len}, new_len: {remote_len}"
                        ),
                    )));
                }
                // Make the new length visible; new blocks will be filled lazily on read.
                local.resize(&self.local_path, remote_len)?;

                // return the updated local state and remote
                self.state
                    .set(State { remote, local })
                    .expect("we just take()'d it");
            }
            Populate::Blocking | Populate::PreferBackground => {
                // Re-fetch from the start of the (possibly partial) tail block so
                // we still make an page-aligned read.
                let from = local_len.saturating_sub(local_len % BLOCK_SIZE as u64);

                let mut remote_pipeline = R::OwnedReadPipeline::new(remote)?;

                // FIXME: check can_schedule in a loop?
                remote_pipeline.schedule_whole(from, from)?;

                assert_matches!(
                    *init_guard,
                    InitSource::FromScratch,
                    "by this point, InitSource must be FromScratch"
                );
                *init_guard = InitSource::PartialPrefiller {
                    prefiller: remote_pipeline,
                    local_state: local,
                };

                // For blocking, resolve the prefill now instead of on first read.
                if matches!(self.open_options.populate, Populate::Blocking) {
                    self.init_state(&mut init_guard, false, None)?;
                }
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
        self.state()?.local.mmap().len::<T>()
    }

    fn populate(&self) -> Result<()> {
        self.populate_from(0)
    }

    fn populate_auto() -> bool {
        false
    }

    fn clear_ram_cache(&self) -> Result<()> {
        if let Some(state) = self.state.get() {
            state.local.mmap().clear_ram_cache()?;
        }
        Ok(())
    }

    fn kind() -> UniversalKind {
        UniversalKind::SimpleDiskCache
    }
}
