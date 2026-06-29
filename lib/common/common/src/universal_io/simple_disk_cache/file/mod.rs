//! [`DiskCache`]: a lazily-populated local mirror of an immutable remote file.
//!
//! The implementation is split across this module's submodules:
//!
//! - this file — the [`DiskCache`] type, its backing [`State`], and the cheap
//!   constructors ([`DiskCache::new`], [`DiskCache::open_remote`]).
//! - [`init`] — how the mirror is brought to life on first use: the
//!   [`InitSource`] state machine and [`DiskCache::init_state`]. **Start here**
//!   to understand cold-start vs. eager-prefill behavior.
//! - [`reopen`] — refreshing the mirror after the (append-only) remote grew.
//! - [`read`] — the [`UniversalRead`] implementation (the public read surface).

use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use parking_lot::Mutex;

use super::DiskCacheRemote;
use super::local_state::LocalState;
use crate::mmap::AdviceSetting;
use crate::universal_io::{OpenOptions, Populate, Result, UniversalRead, UniversalReadFs};

mod init;
mod read;
mod reopen;

pub(crate) use init::InitSource;

/// A lazily-populated local mirror of an immutable remote file.
///
/// The remote is assumed to be immutable for the lifetime of the file;
/// this type implements [`UniversalRead`] only, but not [`UniversalWrite`].
///
/// The local mirror can either be initialized lazily on first read (filling
/// blocks on demand from the remote) or eagerly if populate is set. See
/// [`init`] for the precise lifecycle.
///
/// WARN: There should be only a single instance of DiskCache per path.
/// Initializing multiple instances will try to re-read from remote.
pub struct DiskCache<R>
where
    R: UniversalRead + 'static,
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

/// The materialized mirror: the opened `remote` handle paired with its local
/// mmap mirror. Created exactly once, lazily, by [`DiskCache::init_state`].
#[derive(Debug)]
pub(crate) struct State<R> {
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
}
