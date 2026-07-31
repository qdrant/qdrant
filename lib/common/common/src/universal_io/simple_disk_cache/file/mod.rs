//! [`DiskCache`]: a lazily-populated local mirror of an append-only remote
//! file.

use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use parking_lot::Mutex;

use super::DiskCacheRemote;
use super::local_state::LocalState;
use crate::universal_io::simple_disk_cache::REMOTE_OPEN_OPTIONS;
use crate::universal_io::{OpenOptions, OwnedPipeline, UioResult, UniversalRead, UniversalReadFs};

mod init;
mod read;
mod reopen;

/// A lazily-populated local mirror of an append-only remote file.
///
/// The remote's existing bytes are assumed to be immutable for the lifetime
/// of the file: it may grow externally (picked up by [`reopen`]), but never
/// shrink or change in place. This type implements [`UniversalRead`] only —
/// appends are deliberately not supported through the cache (append
/// directly to the backing storage instead), and random-offset writes stay
/// unsupported.
///
/// The local mirror can either be initialized lazily on first read (filling
/// blocks on demand from the remote) or eagerly if populate is set. See
/// [`init`] for the precise lifecycle.
///
/// Every instance mirrors into its own uniquely-named local file (see
/// [`unique_local_path`](super::fs)), so multiple instances per remote path
/// are safe — e.g. a live-reload can open a fresh handle while the old one is
/// still alive. Each instance fetches from the remote independently though,
/// so per-path handles should not be multiplied without reason. The mirror
/// file is removed on drop.
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
    Ready {
        remote: R,
        local: LocalState,
        /// What the next [`reopen`] must do. Staged by [`schedule_reopen`],
        /// consumed (reset to [`ScheduledReopen::No`]) by [`reopen`]. Only
        /// touched under `&mut self`, and `ReadyRef` borrows just
        /// `remote`/`local`, so staging never disturbs the served state.
        ///
        /// [`reopen`]: UniversalRead::reopen
        /// [`schedule_reopen`]: UniversalRead::schedule_reopen
        scheduled_reopen: ScheduledReopen<R>,
    },
    /// Eager open-time prefill: an in-flight whole-object read scheduled at open;
    /// init waits on it and writes the whole mirror. For `Populate::Blocking` /
    /// `PreferBackground`.
    OpenPrefill { pipeline: OwnedPipeline<R, ()> },
    /// Open-time partial prefill
    PartialPrefill {
        pipeline: OwnedPipeline<R, Range<u32>>,
        len: u64,
    },
}

/// The obligation of the next [`reopen`](UniversalRead::reopen), staged ahead
/// of time by [`schedule_reopen`](UniversalRead::schedule_reopen).
///
/// Staging deliberately leaves the mirror alone: its length keeps matching its
/// persisted content until the apply, so readers observe nothing in between
/// and components keep `len()` as their growth signal. Nothing else remembers
/// the staged targets, hence the `target_len` on every variant.
#[derive(Debug)]
pub(crate) enum ScheduledReopen<R: UniversalRead + 'static> {
    /// Nothing staged — the default at every [`State::Ready`] construction
    /// site, and what a no-growth schedule leaves behind. `reopen` then
    /// schedules and waits inline.
    No,
    /// Scheduling detected that the file size has not changed, so no need to
    /// reopen.
    Unchanged,
    /// Lazy populate (`No` / `Auto` / `Partial`): apply resizes the mirror to
    /// `target_len` and lets the new blocks fault in on demand.
    Resize { target_len: u64 },
    /// Populated (`Blocking` / `PreferBackground`): the appended tail is
    /// already in flight on a clone of the remote; apply resizes, drains it
    /// and writes it. Holds exactly one read, whose user data is the block
    /// range it covers, as in [`State::PartialPrefill`].
    Tail {
        pipeline: OwnedPipeline<R, Range<u32>>,
        target_len: u64,
    },
}

impl<R: UniversalRead + 'static> ScheduledReopen<R> {
    /// Length the scheduled reopen would bring the mirror to, if anything is
    /// staged.
    pub(super) fn target_len(&self) -> Option<u64> {
        match self {
            ScheduledReopen::No |
            ScheduledReopen::Unchanged => None,
            ScheduledReopen::Resize { target_len }
            | ScheduledReopen::Tail {
                target_len,
                pipeline: _,
            } => Some(*target_len),
        }
    }
}

impl<R: UniversalRead + 'static> State<R> {
    /// A live mirror with nothing staged — the default at every construction
    /// site.
    pub fn ready(remote: R, local: LocalState) -> Self {
        State::Ready {
            remote,
            local,
            scheduled_reopen: ScheduledReopen::No,
        }
    }

    #[inline]
    pub fn is_ready(&self) -> bool {
        match self {
            State::Ready { .. } => true,
            State::Uninit | State::OpenPrefill { .. } | State::PartialPrefill { .. } => false,
        }
    }

    #[inline]
    pub fn is_uninit(&self) -> bool {
        match self {
            State::Uninit => true,
            State::Ready { .. } | State::OpenPrefill { .. } | State::PartialPrefill { .. } => false,
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

    pub(super) fn open_remote(&self) -> UioResult<R> {
        self.remote_fs.open(
            &self.remote_path,
            REMOTE_OPEN_OPTIONS,
            self.remote_extra.clone(),
        )
    }
}

impl<R> Drop for DiskCache<R>
where
    R: UniversalRead + 'static,
{
    fn drop(&mut self) {
        // Best-effort mirror cleanup: mirror names are unique per open, so a
        // file left behind would never be reused. Absent (`Err`) when the
        // state never materialized a local mirror.
        let _ = fs_err::remove_file(&self.local_path);
    }
}
