//! [`live_reload`] the local mirror after the (append-only) remote has grown,
//! either in one blocking step or split into a schedule and an apply phase
//! ([`live_preload`]).
//!
//! [`live_reload`]: crate::universal_io::UniversalRead::live_reload
//! [`live_preload`]: crate::universal_io::UniversalRead::live_preload

use std::io::{self, ErrorKind};
use std::path::Path;

use futures::FutureExt;
use futures::future::{BoxFuture, Shared};

use super::{DiskCache, ScheduledReopen, State};
use crate::generic_consts::Sequential;
use crate::universal_io::cached_fs::FileInfo;
use crate::universal_io::simple_disk_cache::pipeline::REMOTE_READ_ALIGNMENT;
use crate::universal_io::simple_disk_cache::{DiskCacheRemote, block_aligned_fetch};
use crate::universal_io::{Populate, UioResult, UniversalIoError, UniversalRead};

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
        futures::executor::block_on(self.live_preload_with_len(None)?);
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
                future,
                mut data,
                blocks_range,
                target_len,
            } => {
                futures::executor::block_on(future);
                let (new_remote, fetched) = data
                    .try_recv()
                    .expect("sender is never dropped before sending")
                    .expect("data should be available, and no other consumer exists")?;

                // resize only after the fetch succeeded
                local.resize(&self.local_path, target_len)?;

                if !fetched.is_empty() {
                    // SAFETY: `fetched` covers `blocks_range` exactly
                    // (clamped to EOF)
                    unsafe { local.write_mmap_bytes(&fetched, blocks_range) }
                }

                // replace remote with the one that fetched the tail
                *remote = new_remote;
            }
        }

        Ok(true)
    }

    pub(super) fn live_preload_impl<F: FnOnce(&Path) -> Option<FileInfo>>(
        &self,
        get_file_info: F,
    ) -> UioResult<Shared<BoxFuture<'static, ()>>> {
        let Some(file_info) = get_file_info(&self.remote_path) else {
            return Err(UniversalIoError::NotFound {
                path: self.remote_path.clone(),
            });
        };
        let fut = self.live_preload_with_len(Some(file_info.size))?;
        self.set_etag(file_info.etag);
        Ok(fut)
    }

    /// Body of [`UniversalRead::live_preload`].
    ///
    /// Records what the next [`reopen_impl`](Self::reopen_impl) must do and,
    /// for populated files, puts the tail fetch in flight — without waiting on
    /// it and without touching the mirror, so readers see no change until the
    /// apply.
    ///
    /// [`UniversalRead::live_preload`]: crate::universal_io::UniversalRead::live_preload
    pub(super) fn live_preload_with_len(
        &self,
        known_len: Option<u64>,
    ) -> UioResult<Shared<BoxFuture<'static, ()>>> {
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

        // If we don't have a known length, reload the remote to tell the new length.
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

        // Already staged at this length: reuse the staged fetch's signal so
        // the caller still observes its completion.
        if let Some(scheduled) = scheduled_reopen.as_ref()
            && scheduled.target_len() == Some(remote_len)
        {
            let future = match scheduled {
                ScheduledReopen::Tail {
                    future,
                    data: _,
                    blocks_range: _,
                    target_len: _,
                } => future.clone(),
                ScheduledReopen::Unchanged | ScheduledReopen::Resize { target_len: _ } => {
                    async {}.boxed().shared()
                }
            };
            return Ok(future);
        }

        if remote_len == local_len {
            *scheduled_reopen = Some(ScheduledReopen::Unchanged);
        } else {
            match self.open_options.populate {
                Populate::Blocking | Populate::PreferBackground => {
                    // Schedule the read of the new tail blocks.
                    let (blocks_range, byte_range) =
                        block_aligned_fetch(local_len..remote_len, remote_len)
                            .expect("the byte range is non-empty");

                    // Fresh remote handle
                    let new_remote = self.open_remote()?;
                    let (tx, rx) = futures::channel::oneshot::channel();
                    let future = {
                        async move {
                            let fetch = || async {
                                Ok(new_remote
                                    .read_bytes_async(byte_range, Sequential, REMOTE_READ_ALIGNMENT)
                                    .await?
                                    .try_cast_bytemuck::<u8>()?
                                    .into_owned())
                            };
                            let result = fetch().await.map(|fetched| (new_remote, fetched));

                            tx.send(result).ok();
                        }
                        .boxed()
                        .shared()
                    };

                    *scheduled_reopen = Some(ScheduledReopen::Tail {
                        target_len: remote_len,
                        future: future.clone(),
                        data: rx,
                        blocks_range,
                    });

                    return Ok(future);
                }
                // No prefetch for lazy population
                Populate::Auto | Populate::No | Populate::Partial(_) => {
                    *scheduled_reopen = Some(ScheduledReopen::Resize {
                        target_len: remote_len,
                    });
                }
            }
        };

        Ok(async {}.boxed().shared())
    }
}
