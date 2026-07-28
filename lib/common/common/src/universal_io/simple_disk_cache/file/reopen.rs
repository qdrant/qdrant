//! [`Reopen`] the local mirror after the (append-only) remote has grown,
//! either in one blocking step or split into a schedule and an apply phase
//! ([`reopen_schedule`]).
//!
//! [`reopen`]: crate::universal_io::UniversalRead::reopen
//! [`reopen_schedule`]: crate::universal_io::UniversalRead::reopen_schedule

use std::io::{self, ErrorKind};
use std::path::Path;

use super::{DiskCache, PendingReopen, State};
use crate::generic_consts::Sequential;
use crate::universal_io::simple_disk_cache::local_state::LocalState;
use crate::universal_io::simple_disk_cache::pipeline::REMOTE_READ_ALIGNMENT;
use crate::universal_io::simple_disk_cache::{
    BLOCK_SIZE, DiskCacheRemote, block_aligned_fetch, to_block_range,
};
use crate::universal_io::{OwnedPipeline, Populate, UioResult, UniversalIoError, UniversalRead};

impl<R> DiskCache<R>
where
    R: DiskCacheRemote,
{
    /// Body of [`UniversalRead::reopen`](crate::universal_io::UniversalRead::reopen).
    pub(super) fn reopen_impl(&mut self) -> UioResult<()> {
        // Apply whatever `reopen_schedule` staged, if anything. This is the
        // only place the mirror grows: `resize` remaps the mmap, which is
        // sound only under `&mut self`, with no reader holding a slice.
        if let State::Ready {
            remote: _,
            local,
            pending_reopen,
        } = self.state.get_mut()
        {
            match std::mem::replace(pending_reopen, PendingReopen::Stale) {
                // Nothing staged: fall through to the blocking path below.
                PendingReopen::Stale => {}
                PendingReopen::Resize { target_len } => {
                    return local.resize(&self.local_path, target_len);
                }
                PendingReopen::Tail {
                    pipeline,
                    target_len,
                } => return Self::apply_tail(&self.local_path, local, pipeline, target_len),
            }
        }

        // `&mut self` gives exclusive access, so we can transition `state`
        // directly without locking or touching the `ready` gate concurrently.
        //
        // Resolve any in-flight prefill so we hold a concrete mirror.
        let (mut remote, mut local) = match std::mem::replace(self.state.get_mut(), State::Uninit) {
            // If it is still `Uninit`, we can let the first read initialize it later.
            State::Uninit => return Ok(()),
            State::Ready {
                remote,
                local,
                pending_reopen: _, // just observed as `Stale`
            } => (remote, local),
            State::OpenPrefill { pipeline } => self.init_from_open_prefill(pipeline)?,
            State::ReopenPrefill { pipeline, local } => {
                self.init_from_reopen_prefill(pipeline, local)?
            }
            State::PartialPrefill { pipeline, len } => {
                self.init_from_partial_prefill(pipeline, len)?
            }
        };
        *self.is_ready.get_mut() = false;

        // Reopen remote so it reflects current length
        remote.reopen()?;

        let local_len = local.mmap().len::<u8>()?;

        match self.open_options.populate {
            Populate::Auto | Populate::No | Populate::Partial(_) => {
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

                *self.state.get_mut() = State::Ready {
                    remote,
                    local,
                    pending_reopen: PendingReopen::Stale,
                };
                *self.is_ready.get_mut() = true;
            }
            Populate::Blocking | Populate::PreferBackground => {
                // Re-fetch from the start of the (possibly partial) tail block so
                // we still make an page-aligned read.
                let from = local_len.saturating_sub(local_len % BLOCK_SIZE as u64);

                let mut pipeline = OwnedPipeline::new(remote)?;

                // FIXME: check can_schedule in a loop?
                pipeline.schedule_whole(from, from)?;

                *self.state.get_mut() = State::ReopenPrefill { pipeline, local };

                // For blocking, resolve the prefill now instead of on first read.
                if matches!(self.open_options.populate, Populate::Blocking) {
                    self.init_state()?;
                }
            }
        }

        Ok(())
    }

    /// Body of [`UniversalRead::reopen_schedule`].
    ///
    /// Records what the next [`reopen_impl`](Self::reopen_impl) must do and,
    /// for populated files, puts the tail fetch in flight — without waiting on
    /// it and without touching the mirror, so readers see no change until the
    /// apply.
    ///
    /// [`UniversalRead::reopen_schedule`]: crate::universal_io::UniversalRead::reopen_schedule
    pub(super) fn reopen_schedule_impl(&mut self, known_len: Option<u64>) -> UioResult<()> {
        // Without a length there is nothing to stage: leave `Stale` so
        // `reopen` asks the remote itself.
        let Some(known_len) = known_len else {
            return Ok(());
        };

        // A prefill can only be resolved by waiting on it — bounded to the
        // first schedule after open, which may race the open-time fetch.
        if !self.state.get_mut().is_ready() && !self.state.get_mut().is_uninit() {
            self.init_state()?;
        }

        // Cold start: materializing here is IO-free (the open is construction
        // only) and saves the `len()` round-trip the first read would pay.
        if self.state.get_mut().is_uninit() {
            let remote = self.open_remote()?;
            let local = LocalState::new(&self.local_path, known_len, self.open_options)?;

            *self.state.get_mut() = State::Ready {
                remote,
                local,
                // The mirror already sits at `known_len`; without this marker
                // the next `reopen` would go down the blocking path.
                pending_reopen: PendingReopen::Resize {
                    target_len: known_len,
                },
            };
            *self.is_ready.get_mut() = true;
            return Ok(());
        }

        let populate = self.open_options.populate;
        let State::Ready {
            remote,
            local,
            pending_reopen,
        } = self.state.get_mut()
        else {
            unreachable!("prefills were resolved and `Uninit` handled above")
        };

        // A staged tail holds a clone of `remote`, and clones share the
        // mapping on local backends — reopening the held handle now would
        // dangle it. A staged tail is therefore never superseded: growth past
        // its target is picked up by the next schedule, once `reopen` consumed
        // this one.
        if matches!(pending_reopen, PendingReopen::Tail { .. }) {
            return Ok(());
        }

        // Make the remote reflect the current length before staging: a no-op
        // for the async bridge, but load-bearing for local remotes, where a
        // stale handle would fetch an empty tail and later lazy faults would
        // read past the mapping. Local stat/remap only, no IO wait.
        remote.reopen()?;

        let mirror_len = local.mmap().len::<u8>()?;
        let staged_len = match pending_reopen {
            PendingReopen::Stale => mirror_len,
            PendingReopen::Resize { target_len } => *target_len,
            PendingReopen::Tail { .. } => unreachable!("returned above"),
        };

        // Growth past what is already staged is the only thing worth
        // scheduling; a shorter `known_len` never downgrades a staged target.
        let grew = known_len > staged_len;
        if !grew && !matches!(pending_reopen, PendingReopen::Stale) {
            return Ok(());
        }

        let populated = matches!(populate, Populate::Blocking | Populate::PreferBackground);
        if !grew || !populated {
            // Lazy populate never fetches ahead — that would pull bytes that
            // may never be read. Same variant marks the no-growth case, where
            // the resize is a no-op.
            *pending_reopen = PendingReopen::Resize {
                target_len: known_len,
            };
            return Ok(());
        }

        // Populated and grown: fetch the appended tail on a clone of the
        // remote (a refcount bump for the async bridge). The read is bounded
        // to `known_len` — the file's cut for this refresh — and starts at the
        // block-aligned floor of the mirror, as in `reopen_impl`.
        let (_, byte_range) = block_aligned_fetch(mirror_len..known_len, known_len)
            .expect("growth leaves a non-empty range to fetch");

        let mut pipeline = OwnedPipeline::new(remote.clone())?;
        // FIXME: check can_schedule in a loop?
        pipeline.schedule::<Sequential>(byte_range.start, byte_range, REMOTE_READ_ALIGNMENT)?;

        *pending_reopen = PendingReopen::Tail {
            pipeline,
            target_len: known_len,
        };

        Ok(())
    }

    /// Drain a staged tail fetch into the mirror: resize to the read's end,
    /// write the (block-aligned) suffix, then settle at `target_len`.
    ///
    /// Takes the mirror by argument rather than through `&mut self` so the
    /// caller can keep its disjoint borrow of `state`.
    fn apply_tail(
        local_path: &Path,
        local: &mut LocalState,
        mut pipeline: OwnedPipeline<R, u64>,
        target_len: u64,
    ) -> UioResult<()> {
        // Exactly one read is scheduled per staged tail, so one `wait` drains
        // it. Normally it has already landed by the time we get here.
        match pipeline.wait()? {
            Some((from, bytes)) if !bytes.is_empty() => {
                let end = from + bytes.len() as u64;
                local.resize(local_path, end)?;
                let blocks_range = to_block_range(from..end);
                // SAFETY: `bytes` covers `blocks_range` exactly, and the remote
                // is immutable, so the mmap suffix is filled once with correct data.
                unsafe { local.write_mmap_bytes(&bytes, blocks_range) }
            }
            // Nothing landed: the resize below still makes the length visible,
            // and the new blocks fault in on demand.
            Some(_) | None => {}
        }

        // Normally a no-op — the read was bounded to `target_len`.
        local.resize(local_path, target_len)
    }
}
