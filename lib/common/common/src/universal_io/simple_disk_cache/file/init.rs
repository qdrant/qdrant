//! Lazy first-use initialization of a [`DiskCache`]'s [`State`].

use std::ops::Range;
use std::sync::atomic::Ordering;

use super::{DiskCache, State};
use crate::universal_io::simple_disk_cache::local_state::LocalState;
use crate::universal_io::simple_disk_cache::{DiskCacheRemote, to_block_range};
use crate::universal_io::{OwnedPipeline, UioResult};

/// A borrowed view of a materialized [`State::Ready`]: the live `remote` handle
/// paired with its local mmap mirror.
///
/// Both references are valid for as long as the borrow of the `DiskCache` that
/// produced them — see [`DiskCache::state`] for why that holds even though the
/// backing enum lives behind a lock.
pub(crate) struct ReadyRef<'a, R> {
    pub remote: &'a R,
    pub local: &'a LocalState,
}

impl<R> DiskCache<R>
where
    R: DiskCacheRemote,
{
    /// Return the materialized [`State::Ready`], initializing it on first call.
    pub(crate) fn state(&self) -> UioResult<ReadyRef<'_, R>> {
        if !self.is_ready() {
            self.init_state()?;
        }

        // SAFETY: self.ready tracks whether `state` is `State::Ready`, to make reads "lock-free".
        let State::Ready { remote, local } = (unsafe { &*self.state.data_ptr() }) else {
            unreachable!("the `ready` flag guarantees the `Ready` variant")
        };
        Ok(ReadyRef { remote, local })
    }

    /// Whether the local mirror has been materialized. Cheap, lock-free; lets
    /// callers act on an already-live mirror without forcing initialization.
    pub(crate) fn is_ready(&self) -> bool {
        self.is_ready.load(Ordering::Acquire)
    }

    /// Drive `state` to [`State::Ready`] under the lock, consuming whichever
    /// pre-init variant is staged. Idempotent: a no-op once `ready` is set.
    pub(super) fn init_state(&self) -> UioResult<()> {
        let mut state = self.state.lock();

        // Another thread may have materialized while we waited for the lock.
        // Prevent using `mem::replace` if it is already initialized.
        if self.is_ready() {
            return Ok(());
        }

        let (remote, local) = match std::mem::replace(&mut *state, State::Uninit) {
            State::Uninit => self.init_from_scratch(self.open_remote()?)?,
            State::OpenPrefill { pipeline } => self.init_from_open_prefill(pipeline)?,
            State::ReopenPrefill { pipeline, local } => {
                self.init_from_reopen_prefill(pipeline, local)?
            }
            State::PartialPrefill { pipeline, len } => {
                self.init_from_partial_prefill(pipeline, len)?
            }
            State::Ready { .. } => {
                unreachable!("We just observed `!ready` while holding the mutex lock")
            }
        };

        *state = State::Ready { remote, local };
        self.is_ready.store(true, Ordering::Release);

        Ok(())
    }

    /// Build an empty local mmap sized from the remote length; reads fault blocks
    /// in on demand. The lazy cold-start path ([`State::Uninit`]).
    fn init_from_scratch(&self, remote: R) -> UioResult<(R, LocalState)> {
        let len = remote.len::<u8>()?;
        let local = LocalState::new(&self.local_path, len, self.open_options)?;
        Ok((remote, local))
    }

    /// Resolve an [`State::OpenPrefill`] whole-object read into a fully-written
    /// local mirror. Falls back to a cold start if no bytes were scheduled.
    pub(super) fn init_from_open_prefill(
        &self,
        mut pipeline: OwnedPipeline<R, ()>,
    ) -> UioResult<(R, LocalState)> {
        match pipeline.wait()? {
            Some((_, bytes)) => {
                // `bytes` covers the whole file, so its length is the remote length.
                let local =
                    LocalState::new(&self.local_path, bytes.len() as u64, self.open_options)?;
                let blocks_range = to_block_range(0..bytes.len() as u64);
                // SAFETY: `bytes` covers `blocks_range` exactly, and the remote
                // is immutable, so the mmap is filled once with correct data.
                unsafe { local.write_mmap_bytes(&bytes, blocks_range) };
                Ok((pipeline.into_inner(), local))
            }
            // `None` means the whole-object read scheduled nothing — the remote
            // is empty (`schedule_whole` from offset 0 against EOF 0). Fall back
            // to a from-scratch, zero-length mirror.
            None => self.init_from_scratch(pipeline.into_inner()),
        }
    }

    /// Resolve a [`State::ReopenPrefill`] tail read: resize the mirror and write
    /// only the appended suffix. See [`super::reopen`].
    pub(super) fn init_from_reopen_prefill(
        &self,
        mut pipeline: OwnedPipeline<R, u64>,
        mut local: LocalState,
    ) -> UioResult<(R, LocalState)> {
        match pipeline.wait()? {
            Some((start, bytes)) if !bytes.is_empty() => {
                let end = start + bytes.len() as u64;
                local.resize(&self.local_path, end)?;
                let blocks_range = to_block_range(start..end);
                // SAFETY: `bytes` covers `blocks_range` exactly, and the remote
                // is immutable, so the mmap suffix is filled once with correct data.
                unsafe { local.write_mmap_bytes(&bytes, blocks_range) }
            }
            // There is nothing to apply.
            // TODO: double check that the remote didn't shrink?
            Some(_) | None => {}
        }

        Ok((pipeline.into_inner(), local))
    }

    /// Resolve a [`State::PartialPrefill`] read: size the mirror from `len` and
    /// write only the fetched (block-aligned) range; the remainder faults in
    /// lazily on later reads. See [`Populate::Partial`].
    ///
    /// [`Populate::Partial`]: crate::universal_io::Populate::Partial
    pub(super) fn init_from_partial_prefill(
        &self,
        mut pipeline: OwnedPipeline<R, Range<u32>>,
        len: u64,
    ) -> UioResult<(R, LocalState)> {
        let local = LocalState::new(&self.local_path, len, self.open_options)?;

        match pipeline.wait()? {
            Some((blocks_range, bytes)) if !bytes.is_empty() => {
                // SAFETY: `start` is block-aligned and `bytes` covers
                // `blocks_range` exactly (up to EOF), and the remote is
                // immutable, so the mmap range is filled once with correct data.
                unsafe { local.write_mmap_bytes(&bytes, blocks_range) };
            }
            // Nothing was scheduled (empty range) or the read came back empty:
            // leave the mirror empty and fault blocks in lazily.
            Some(_) | None => {}
        }

        Ok((pipeline.into_inner(), local))
    }

    /// Stage a [`State::OpenPrefill`] when the cache is still cold ([`State::Uninit`]),
    /// so that a subsequent [`read_whole`] reads the whole object in one access
    /// instead of a separate `len` call followed by lazy faulting.
    ///
    /// [`read_whole`]: crate::universal_io::UniversalRead::read_whole
    pub(super) fn prefill_if_uninit(&self) -> UioResult<()> {
        if self.is_ready() {
            return Ok(());
        }

        let mut state = self.state.lock();
        if state.is_uninit() {
            // Use the remote's `schedule_whole` to avoid an extra `len` call.
            let mut pipeline = OwnedPipeline::new(self.open_remote()?)?;
            pipeline.schedule_whole((), 0)?;
            *state = State::OpenPrefill { pipeline };
        }

        Ok(())
    }
}
