//! Refreshing the local mirror after the (append-only) remote has grown.
//!
//! The remote is immutable except that it may be *appended to*. [`reopen`] picks
//! up that growth: for the lazy populate modes it just resizes the mirror and
//! lets later reads fault the new blocks in; for the eager modes it schedules a
//! tail read of the appended suffix and stages it as an
//! [`InitSource::PartialPrefiller`](super::InitSource::PartialPrefiller).
//!
//! [`reopen`]: crate::universal_io::UniversalRead::reopen

use std::assert_matches;
use std::io::{self, ErrorKind};

use super::{DiskCache, InitSource, State};
use crate::universal_io::simple_disk_cache::{BLOCK_SIZE, DiskCacheRemote};
use crate::universal_io::{OwnedPipeline, Populate, Result, UniversalIoError, UniversalRead};

impl<R> DiskCache<R>
where
    R: DiskCacheRemote,
{
    /// Body of [`UniversalRead::reopen`](crate::universal_io::UniversalRead::reopen).
    pub(super) fn reopen_impl(&mut self) -> Result<()> {
        // Wait for InitSource::Prefill, if set.
        let mut init_guard = self.init_lock.lock();
        self.init_state(&mut init_guard, false)?;

        let Some(state) = self.state.take() else {
            // If `self.state` didn't initialize after `init_state`, we are not populating
            // and we haven't made any reads.
            //
            // The first read will take care of initializing to the remote length.
            return Ok(());
        };

        let State {
            mut remote,
            mut local,
        } = state;

        // Reopen remote so it reflects current length
        remote.reopen()?;

        let local_len = local.mmap().len::<u8>()?;

        match self.open_options.populate {
            Populate::Auto | Populate::No => {
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

                // return the updated local state and remote
                self.state
                    .set(State { remote, local })
                    .expect("we just take()'d it");
            }
            Populate::Blocking | Populate::PreferBackground => {
                // Re-fetch from the start of the (possibly partial) tail block so
                // we still make an page-aligned read.
                let from = local_len.saturating_sub(local_len % BLOCK_SIZE as u64);

                let mut remote_pipeline = OwnedPipeline::new(remote)?;

                // FIXME: check can_schedule in a loop?
                remote_pipeline.schedule_whole(from, from)?;

                assert_matches!(
                    *init_guard,
                    InitSource::FromScratch,
                    "by this point, InitSource must be FromScratch"
                );
                *init_guard = InitSource::PartialPrefiller {
                    prefiller: remote_pipeline,
                    local_state: local,
                };

                // For blocking, resolve the prefill now instead of on first read.
                if matches!(self.open_options.populate, Populate::Blocking) {
                    self.init_state(&mut init_guard, false)?;
                }
            }
        }

        Ok(())
    }
}
