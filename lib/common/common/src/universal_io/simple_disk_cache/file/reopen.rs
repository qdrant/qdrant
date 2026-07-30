//! [`Reopen`] the local mirror after the (append-only) remote has grown,
//! either in one blocking step or split into a schedule and an apply phase
//! ([`schedule_reopen`]).
//!
//! [`reopen`]: crate::universal_io::UniversalRead::reopen
//! [`schedule_reopen`]: crate::universal_io::UniversalRead::schedule_reopen

use std::io::{self, ErrorKind};
use std::path::Path;

use super::{DiskCache, ScheduledReopen, State};
use crate::generic_consts::Sequential;
use crate::universal_io::cached_fs::FileInfo;
use crate::universal_io::simple_disk_cache::pipeline::REMOTE_READ_ALIGNMENT;
use crate::universal_io::simple_disk_cache::{DiskCacheRemote, block_aligned_fetch};
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
        self.schedule_reopen_with_len(None)?;
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
                    Some((blocks_range, bytes)) if !bytes.is_empty() => {
                        // SAFETY: `bytes` covers `blocks_range` exactly
                        // (clamped to EOF)
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

    pub(super) fn schedule_reopen_impl<F: FnOnce(&Path) -> Option<FileInfo>>(
        &mut self,
        get_file_info: F,
    ) -> UioResult<()> {
        let Some(file_info) = get_file_info(&self.remote_path) else {
            return Err(UniversalIoError::NotFound {
                path: self.remote_path.clone(),
            });
        };

        self.schedule_reopen_with_len(Some(file_info.size))
    }

    /// Body of [`UniversalRead::schedule_reopen`].
    ///
    /// Records what the next [`reopen_impl`](Self::reopen_impl) must do and,
    /// for populated files, puts the tail fetch in flight — without waiting on
    /// it and without touching the mirror, so readers see no change until the
    /// apply.
    ///
    /// [`UniversalRead::schedule_reopen`]: crate::universal_io::UniversalRead::schedule_reopen
    pub(super) fn schedule_reopen_with_len(&mut self, known_len: Option<u64>) -> UioResult<()> {
        // Wait for scheduled prefill, if any.
        //
        // warn: this will do a length request if uninit, but when using a
        // cached fs to create the file it should never be uninit.
        self.init_state()?;

        let State::Ready {
            remote,
            local,
            scheduled_reopen,
        } = self.state.get_mut()
        else {
            unreachable!("init_state drives state to Ready");
        };

        let local_len = local.mmap().len::<u8>()?;

        // With a known length, decide before touching the remote: the common
        // no-growth sweep then costs no round-trip, and a kept staged tail
        // never has its remote reopened out from under its pipeline clone
        // (`MmapFile` clones share the mapping). Without one, ask the remote
        // — the blocking `reopen()` fallback.
        let remote_len = match known_len {
            Some(known_len) => known_len,
            None => {
                remote.reopen()?;
                remote.len::<u8>()?
            }
        };

        // Reject operation if we find a smaller remote.
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

        // Make the held remote reflect the current length. If length wasn't
        // provided, this is already done above.
        if known_len.is_some() {
            remote.reopen()?;
        }

        *scheduled_reopen = match self.open_options.populate {
            Populate::Blocking | Populate::PreferBackground => {
                // Schedule the read of the new tail blocks.
                let (blocks_range, byte_range) =
                    block_aligned_fetch(local_len..remote_len, remote_len)
                        .expect("the byte range is non-empty");

                let mut pipeline = OwnedPipeline::new(remote.clone())?;
                // FIXME: check can_schedule in a loop?
                pipeline.schedule::<Sequential>(blocks_range, byte_range, REMOTE_READ_ALIGNMENT)?;

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
