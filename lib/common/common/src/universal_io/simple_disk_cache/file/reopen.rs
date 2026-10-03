//! [`live_reload`] the local mirror after the (append-only) remote has grown,
//! either in one blocking step or split into a schedule and an apply phase
//! ([`live_preload`]).
//!
//! [`live_reload`]: crate::universal_io::UniversalRead::live_reload
//! [`live_preload`]: crate::universal_io::UniversalRead::live_preload

use std::io::{self, ErrorKind};
use std::path::Path;

use aligned_vec::{AVec, RuntimeAlign};
use futures::FutureExt;
use futures::future::{BoxFuture, Shared};

use super::{DiskCache, ScheduledReopen, State};
use crate::generic_consts::Sequential;
use crate::universal_io::cached_fs::FileInfo;
use crate::universal_io::simple_disk_cache::pipeline::REMOTE_READ_ALIGNMENT;
use crate::universal_io::simple_disk_cache::{
    BLOCK_SIZE, DiskCacheRemote, block_aligned_fetch, to_block_range,
};
use crate::universal_io::{ChunkSink, Populate, UioResult, UniversalIoError, UniversalRead};

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
            ScheduledReopen::UnboundedTail {
                future,
                mut data,
                from,
            } => {
                futures::executor::block_on(future);
                let (new_remote, total_len, fetched) = data
                    .try_recv()
                    .expect("sender is never dropped before sending")
                    .expect("data should be available, and no other consumer exists")?;

                let local_len = local.mmap().len::<u8>()?;
                check_not_shrunk(local_len, total_len)?;

                // Resize only after the fetch succeeded
                local.resize(&self.local_path, total_len)?;

                if !fetched.is_empty() {
                    let blocks_range = to_block_range(from..total_len);
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
        let file_info = get_file_info(&self.remote_path);
        let fut = self.live_preload_with_len(file_info.as_ref().map(|info| info.size))?;
        if let Some(file_info) = file_info {
            self.set_etag(file_info.etag);
        }
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

        // Already staged for this request: reuse the staged signal so callers
        // still observe its completion.
        if let Some(scheduled) = scheduled_reopen.as_ref() {
            let already_staged = match (scheduled, known_len) {
                (_, Some(len)) => scheduled.target_len() == Some(len),
                (ScheduledReopen::UnboundedTail { .. }, None) => true,
                _ => false,
            };
            if already_staged {
                return Ok(scheduled.future());
            }
        }

        match self.open_options.populate {
            Populate::Blocking | Populate::PreferBackground => match known_len {
                Some(remote_len) => {
                    check_not_shrunk(local_len, remote_len)?;
                    if remote_len == local_len {
                        *scheduled_reopen = Some(ScheduledReopen::Unchanged);
                        Ok(async {}.boxed().shared())
                    } else {
                        self.schedule_bounded_tail(scheduled_reopen, local_len, remote_len)
                    }
                }
                None => self.schedule_unbounded_tail(scheduled_reopen, local_len),
            },
            Populate::Auto | Populate::No | Populate::Partial(_) => {
                let remote_len = match known_len {
                    Some(known_len) => known_len,
                    None => {
                        remote.live_reload()?;
                        remote.len::<u8>()?
                    }
                };

                check_not_shrunk(local_len, remote_len)?;

                *scheduled_reopen = Some(if remote_len == local_len {
                    ScheduledReopen::Unchanged
                } else {
                    ScheduledReopen::Resize {
                        target_len: remote_len,
                    }
                });

                Ok(async {}.boxed().shared())
            }
        }
    }

    fn schedule_bounded_tail(
        &self,
        scheduled_reopen: &mut Option<ScheduledReopen<R>>,
        local_len: u64,
        remote_len: u64,
    ) -> UioResult<Shared<BoxFuture<'static, ()>>> {
        let (blocks_range, byte_range) = block_aligned_fetch(local_len..remote_len, remote_len)
            .expect("the byte range is non-empty");

        let new_remote = self.open_remote()?;
        let (tx, rx) = futures::channel::oneshot::channel();
        let future = {
            async move {
                let fetch = || async {
                    Ok(new_remote
                        .read_bytes_async(byte_range, Sequential, REMOTE_READ_ALIGNMENT)
                        .await?
                        .into_owned(REMOTE_READ_ALIGNMENT))
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

        Ok(future)
    }

    fn schedule_unbounded_tail(
        &self,
        scheduled_reopen: &mut Option<ScheduledReopen<R>>,
        local_len: u64,
    ) -> UioResult<Shared<BoxFuture<'static, ()>>> {
        let from = (local_len / BLOCK_SIZE as u64) * BLOCK_SIZE as u64;

        let new_remote = self.open_remote()?;
        let (tx, rx) = futures::channel::oneshot::channel();
        let future = {
            async move {
                let fetch = || async {
                    let collector = new_remote
                        .read_from_into_async(from, move |total_len| {
                            let tail_len = (total_len.saturating_sub(from)) as usize;
                            let mut buffer = AVec::new(REMOTE_READ_ALIGNMENT);
                            buffer.resize(tail_len, 0);
                            Ok(TailCollector {
                                from,
                                total_len,
                                buffer,
                            })
                        })
                        .await?;
                    Ok((new_remote, collector.total_len, collector.buffer))
                };
                let result = fetch().await;
                tx.send(result).ok();
            }
            .boxed()
            .shared()
        };

        *scheduled_reopen = Some(ScheduledReopen::UnboundedTail {
            future: future.clone(),
            data: rx,
            from,
        });

        Ok(future)
    }
}

struct TailCollector {
    from: u64,
    total_len: u64,
    buffer: AVec<u8, RuntimeAlign>,
}

impl ChunkSink for TailCollector {
    fn write_chunk(&mut self, offset: u64, bytes: &[u8]) -> UioResult<()> {
        let Some(dst_start) = offset.checked_sub(self.from) else {
            return Ok(());
        };
        let dst_start = dst_start as usize;
        let dst_end = dst_start + bytes.len();

        if dst_end <= self.buffer.len() {
            self.buffer[dst_start..dst_end].copy_from_slice(bytes);
        }

        Ok(())
    }
}

fn check_not_shrunk(local_len: u64, new_len: u64) -> UioResult<()> {
    if new_len < local_len {
        return Err(UniversalIoError::Io(io::Error::new(
            ErrorKind::UnexpectedEof,
            format!(
                "Reopen encountered a smaller file than expected; old_len: {local_len}, new_len: {new_len}"
            ),
        )));
    }
    Ok(())
}
