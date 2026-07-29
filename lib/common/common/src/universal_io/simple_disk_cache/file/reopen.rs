//! [`Reopen`] the local mirror after the (append-only) remote has grown,
//! either in one blocking step or split into a schedule and an apply phase
//! ([`schedule_reopen`]).
//!
//! [`reopen`]: crate::universal_io::UniversalRead::reopen
//! [`schedule_reopen`]: crate::universal_io::UniversalRead::schedule_reopen

use std::io::{self, ErrorKind};

use super::{DiskCache, ScheduledReopen, State};
use crate::generic_consts::Sequential;
use crate::universal_io::simple_disk_cache::pipeline::REMOTE_READ_ALIGNMENT;
use crate::universal_io::simple_disk_cache::{
    DiskCacheRemote, block_aligned_fetch, to_block_range,
};
use crate::universal_io::{OwnedPipeline, Populate, UioResult, UniversalIoError, UniversalRead};

impl<R> DiskCache<R>
where
    R: DiskCacheRemote,
{
    /// Body of [`UniversalRead::reopen`](crate::universal_io::UniversalRead::reopen).
    pub(super) fn reopen_impl(&mut self) -> UioResult<()> {
        if self.resolve_pending_reopen()? {
            return Ok(());
        }

        // If not previously done, schedule and wait blockingly
        self.schedule_reopen_impl(None)?;
        self.resolve_pending_reopen()?;

        Ok(())
    }

    // Apply whatever `schedule_reopen` staged, if anything.
    //
    // Returns `true` if a pending reopen was resolved, `false` otherwise.
    fn resolve_pending_reopen(&mut self) -> UioResult<bool> {
        let State::Ready {
            remote: _,
            local,
            scheduled_reopen,
        } = self.state.get_mut()
        else {
            return Ok(false);
        };

        match std::mem::replace(scheduled_reopen, ScheduledReopen::No) {
            // Nothing staged
            ScheduledReopen::No => Ok(false),
            ScheduledReopen::Resize { target_len } => {
                local.resize(&self.local_path, target_len)?;
                Ok(true)
            }
            ScheduledReopen::Tail {
                mut pipeline,
                target_len,
            } => {
                local.resize(&self.local_path, target_len)?;

                match pipeline.wait()? {
                    Some((from, bytes)) if !bytes.is_empty() => {
                        let end = from + bytes.len() as u64;
                        assert_eq!(
                            end, target_len,
                            "Expected the bytes and the scheduled read to be the same length"
                        );

                        let blocks_range = to_block_range(from..end);
                        // SAFETY: `bytes` covers `blocks_range` exactly, and the remote
                        // is immutable, so the mmap suffix is filled once with correct data.
                        unsafe { local.write_mmap_bytes(&bytes, blocks_range) }
                    }
                    // Nothing landed: the resize still makes the length visible,
                    // and the new blocks fault in on demand.
                    Some(_) | None => {}
                }

                Ok(true)
            }
        }
    }

    /// Body of [`UniversalRead::schedule_reopen`].
    ///
    /// Records what the next [`reopen_impl`](Self::reopen_impl) must do and,
    /// for populated files, puts the tail fetch in flight — without waiting on
    /// it and without touching the mirror, so readers see no change until the
    /// apply.
    ///
    /// [`UniversalRead::schedule_reopen`]: crate::universal_io::UniversalRead::schedule_reopen
    pub(super) fn schedule_reopen_impl(&mut self, known_len: Option<u64>) -> UioResult<()> {
        // Wait for scheduled prefill, if any.
        if !self.is_ready() {
            // warn: this will do a length request if uninit, but when using a
            // cached fs to create the file it should never be uninit.
            self.init_state()?;
        }

        let State::Ready {
            remote,
            local,
            scheduled_reopen,
        } = self.state.get_mut()
        else {
            unreachable!("init_state drives state to Ready");
        };

        // Make the remote reflect the current length, even if it is already
        // larger what we'll request.
        remote.reopen()?;

        let remote_len = if let Some(known_len) = known_len {
            known_len
        } else {
            remote.len::<u8>()?
        };

        // Reject operation if we find a smaller remote.
        let local_len = local.mmap().len::<u8>()?;
        if remote_len < local_len {
            return Err(UniversalIoError::Io(io::Error::new(
                ErrorKind::UnexpectedEof,
                format!(
                    "Reopen encountered a smaller file than expected; old_len: {local_len}, new_len: {remote_len}"
                ),
            )));
        }

        // Check if length has grown
        if scheduled_reopen.target_len() == Some(remote_len) || remote_len == local_len {
            return Ok(());
        }

        *scheduled_reopen = match self.open_options.populate {
            Populate::Blocking | Populate::PreferBackground => {
                // Populated and grown: fetch the appended tail on a clone of the
                // remote (a refcount bump for the async bridge). The read is bounded
                // to `remote` — the file's cut for this refresh — and starts at the
                // block-aligned floor of the mirror, as in `reopen_impl`.
                let (_, byte_range) = block_aligned_fetch(local_len..remote_len, remote_len)
                    .expect("the byte range is non-empty");

                let mut pipeline = OwnedPipeline::new(remote.clone())?;
                // FIXME: check can_schedule in a loop?
                pipeline.schedule::<Sequential>(
                    byte_range.start,
                    byte_range,
                    REMOTE_READ_ALIGNMENT,
                )?;

                ScheduledReopen::Tail {
                    pipeline,
                    target_len: remote_len,
                }
            }
            // No prefetch for lazy population
            Populate::Auto | Populate::No | Populate::Partial(_) => ScheduledReopen::Resize {
                target_len: remote_len,
            },
        };

        Ok(())
    }
}
