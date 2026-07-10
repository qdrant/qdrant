//! [`DiskCache`]: a lazily-populated local mirror of an immutable remote file.

use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use parking_lot::Mutex;

use super::DiskCacheRemote;
use super::local_state::LocalState;
use crate::mmap::AdviceSetting;
use crate::universal_io::{
    OpenOptions, OwnedPipeline, Populate, Result, UniversalRead, UniversalReadFs,
};

mod init;
mod read;
mod reopen;

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
#[derive(Debug)]
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
    /// The cache's lifecycle state, initialized lazily on first use.
    pub(super) state: Mutex<State<R>>,
    /// Fast-path gate: `true` when `state` is [`State::Ready`].
    pub(super) is_ready: AtomicBool,
}

/// The lifecycle of a [`DiskCache`]'s local mirror, from "not yet materialized"
/// through to the live [`Ready`](Self::Ready) mirror.
#[derive(Debug)]
pub(crate) enum State<R: UniversalRead + 'static> {
    /// Uninitialized start. Chosen for `Populate::No` / `Auto`.
    Uninit,
    /// The live mirror: the opened `remote` handle paired with its local mmap.
    Ready { remote: R, local: LocalState },
    /// Eager open-time prefill: an in-flight whole-object read scheduled at open;
    /// init waits on it and writes the whole mirror. For `Populate::Blocking` /
    /// `PreferBackground`.
    OpenPrefill { pipeline: OwnedPipeline<R, ()> },
    /// The reopen-time counterpart of [`OpenPrefill`](Self::OpenPrefill): an
    /// in-flight read of just the appended tail (block-aligned old length → new
    /// EOF). Init resizes the mirror and writes only that suffix. See [`reopen`].
    ReopenPrefill {
        pipeline: OwnedPipeline<R, u64>,
        local: LocalState,
    },
    /// Open-time partial prefill
    PartialPrefill {
        pipeline: OwnedPipeline<R, Range<u32>>,
        len: u64,
    },
}

impl<R: UniversalRead + 'static> State<R> {
    #[inline]
    pub fn is_ready(&self) -> bool {
        match self {
            State::Ready { .. } => true,
            State::Uninit
            | State::OpenPrefill { .. }
            | State::ReopenPrefill { .. }
            | State::PartialPrefill { .. } => false,
        }
    }

    #[inline]
    pub fn is_uninit(&self) -> bool {
        match self {
            State::Uninit => true,
            State::Ready { .. }
            | State::OpenPrefill { .. }
            | State::ReopenPrefill { .. }
            | State::PartialPrefill { .. } => false,
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
        state: State<R>,
    ) -> Self {
        let is_ready = state.is_ready();
        Self {
            remote_fs,
            remote_extra,
            remote_path: remote_path.as_ref().to_owned(),
            open_options: options,
            local_path,
            state: Mutex::new(state),
            is_ready: AtomicBool::new(is_ready),
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
