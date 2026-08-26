//! [`live_reload`] the local mirror after the (append-only) remote has grown,
//! either in one blocking step or split into a schedule and an apply phase
//! ([`live_preload`]).
//!
//! [`live_reload`]: crate::universal_io::UniversalRead::live_reload
//! [`live_preload`]: crate::universal_io::UniversalRead::live_preload

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
    /// Body of [`UniversalRead::live_reload`](crate::universal_io::UniversalRead::live_reload).
    pub(super) fn reopen_impl(&mut self) -> UioResult<()> {
        if self.resolve_pending_reload()? {
            return Ok(());
        }

        // If not previously done, schedule and wait blockingly
        self.live_preload_with_len(None)?;
        self.resolve_pending_reload()?;

        Ok(())
    }

    // Apply whatever `live_preload` staged, if anything.
    //
    // Returns `true` if a pending reopen was resolved, `false` otherwise.
    fn resolve_pending_reload(&mut self) -> UioResult<bool> {
        let State::Ready {
            remote,
            local,
            scheduled_reopen,
        } = self.state.get_mut()
        else {
            return Ok(false);
        };

        let Some(scheduled_reopen) = scheduled_reopen.take() else {
            // There isn't anything scheduled.
            return Ok(false);
        };

        // Handle the scheduled reopen.
        match scheduled_reopen {
            // It was staged without changes.
            ScheduledReopen::Unchanged => {}
            ScheduledReopen::Resize { target_len } => {
                // reopen remote, so we can read up to the new length.
                remote.live_reload()?;

                local.resize(&self.local_path, target_len)?;
            }
            ScheduledReopen::Tail {
                mut pipeline,
                target_len,
            } => {
                let fetched = pipeline.wait()?;

                // resize only after pipeline.wait() returns Ok
                local.resize(&self.local_path, target_len)?;

                match fetched {
                    Some((blocks_range, bytes)) if !bytes.is_empty() => {
                        // SAFETY: `bytes` covers `blocks_range` exactly
                        // (clamped to EOF)
                        unsafe { local.write_mmap_bytes(&bytes, blocks_range) }
                    }
                    // Nothing landed: the resize still makes the length visible,
                    // and the new blocks fault in on demand.
                    Some(_) | None => {}
                }

                // replace remote with the one from the owned pipeline
                *remote = pipeline.into_inner();
            }
        }

        Ok(true)
    }

    pub(super) fn live_preload_impl<F: FnOnce(&Path) -> Option<FileInfo>>(
        &self,
        get_file_info: F,
    ) -> UioResult<()> {
        let Some(file_info) = get_file_info(&self.remote_path) else {
            return Err(UniversalIoError::NotFound {
                path: self.remote_path.clone(),
            });
        };

        self.live_preload_with_len(Some(file_info.size))?;
        self.set_etag(file_info.etag);
        Ok(())
    }

    /// Body of [`UniversalRead::live_preload`].
    ///
    /// Records what the next [`reopen_impl`](Self::reopen_impl) must do and,
    /// for populated files, puts the tail fetch in flight — without waiting on
    /// it and without touching the mirror, so readers see no change until the
    /// apply.
    ///
    /// [`UniversalRead::live_preload`]: crate::universal_io::UniversalRead::live_preload
    pub(super) fn live_preload_with_len(&self, known_len: Option<u64>) -> UioResult<()> {
        // Wait for scheduled prefill, if any.
        //
        // warn: this will do a length request if uninit, but when using a
        // cached fs to create the file it should never be uninit.
        self.init_state()?;

        let mut state = self.state.lock();
        let State::Ready {
            remote,
            local,
            scheduled_reopen,
        } = &mut *state
        else {
            unreachable!("init_state drives state to Ready");
        };

        let local_len = local.mmap().len::<u8>()?;

        // If we don't have a known length, reopen the remote to tell the new length.
        let remote_len = match known_len {
            Some(known_len) => known_len,
            None => {
                remote.live_reload()?;
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

        // Check if staged length has grown
        if scheduled_reopen
            .as_ref()
            .is_some_and(|r| r.target_len() == Some(remote_len))
        {
            return Ok(());
        }

        let new_scheduled_reopen = if remote_len == local_len {
            ScheduledReopen::Unchanged
        } else {
            match self.open_options.populate {
                Populate::Blocking | Populate::PreferBackground => {
                    // Schedule the read of the new tail blocks.
                    let (blocks_range, byte_range) =
                        block_aligned_fetch(local_len..remote_len, remote_len)
                            .expect("the byte range is non-empty");

                    // Fresh remote handle
                    let new_remote = self.open_remote()?;
                    let mut pipeline = OwnedPipeline::new(new_remote)?;
                    // FIXME: check can_schedule in a loop?
                    pipeline.schedule::<Sequential>(
                        blocks_range,
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
            }
        };

        *scheduled_reopen = Some(new_scheduled_reopen);

        Ok(())
    }
}
