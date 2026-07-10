//! [`Reopen`] the local mirror after the (append-only) remote has grown.
//!
//! [`reopen`]: crate::universal_io::UniversalRead::reopen

use std::io::{self, ErrorKind};

use super::{DiskCache, State};
use crate::universal_io::simple_disk_cache::{BLOCK_SIZE, DiskCacheRemote};
use crate::universal_io::{OwnedPipeline, Populate, Result, UniversalIoError, UniversalRead};

impl<R> DiskCache<R>
where
    R: DiskCacheRemote,
{
    /// Body of [`UniversalRead::reopen`](crate::universal_io::UniversalRead::reopen).
    pub(super) fn reopen_impl(&mut self) -> Result<()> {
        // `&mut self` gives exclusive access, so we can transition `state`
        // directly without locking or touching the `ready` gate concurrently.
        //
        // Resolve any in-flight prefill so we hold a concrete mirror.
        let (mut remote, mut local) = match std::mem::replace(self.state.get_mut(), State::Uninit) {
            // If it is still `Uninit`, we can let the first read initialize it later.
            State::Uninit => return Ok(()),
            State::Ready { remote, local } => (remote, local),
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

                *self.state.get_mut() = State::Ready { remote, local };
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
}
