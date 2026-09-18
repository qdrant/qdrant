use std::cell::OnceCell;
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;
use std::time::Instant;

use slab::Slab;

use super::placeholder::{Placeholder, PlaceholderGuard, PlaceholderResult, WaitResult};
use super::stats::FetchStats;
use crate::ext::aligned_vec::ACow;
use crate::generic_consts::{AccessPattern, Random, Sequential};
use crate::universal_io::simple_disk_cache::local_state::LocalState;
use crate::universal_io::simple_disk_cache::{
    DiskCache, DiskCacheRemote, block_aligned_fetch, to_block_range,
};
use crate::universal_io::{ReadPipeline, UioResult, UniversalIoError, UniversalRead, UserData};

#[cfg(target_os = "linux")]
/// Required alignment when using io_uring with `O_DIRECT` on Linux
pub(super) const REMOTE_READ_ALIGNMENT: usize = crate::universal_io::io_uring::KERNEL_PAGE_SIZE;

#[cfg(not(target_os = "linux"))]
/// Default alignment on non-Linux platforms
pub(super) const REMOTE_READ_ALIGNMENT: usize = 1;

/// A remote fetch in flight on `remote_pipeline`.
struct RemoteFetch<'file, R>
where
    R: UniversalRead + 'static,
{
    file: &'file DiskCache<R>,
    guard: PlaceholderGuard,
    fetch: FetchStats,
}

impl<'file, R: UniversalRead> RemoteFetch<'file, R> {
    #[inline]
    fn covers(&self, file: &DiskCache<R>, range: &Range<u32>) -> bool {
        std::ptr::eq(self.file, file) && self.guard.placeholder().covers(range)
    }
}

/// A read scheduled on this pipeline waiting for blocks to be fetched into local mirror.
struct ScheduledRead<'file, R, U>
where
    R: UniversalRead + 'static,
{
    user_data: U,
    file: &'file DiskCache<R>,
    range: Range<u64>,
    is_sequential: bool,
    placeholder: Arc<Placeholder>,
}

/// Outcome of [`pick_source`]: either the requested range is already available
/// locally (or is empty) and needs no remote work, or a remote read must be
/// scheduled for `blocks_byte_range` covering `blocks_range`.
pub(super) enum Source {
    Local {
        range: Range<u64>,
        is_sequential: bool,
    },
    Remote {
        blocks_range: Range<u32>,
        blocks_byte_range: Range<u64>,
    },
}

/// Decide whether `range` can be answered from local mmap or needs a remote fetch.
///
/// Avoids materializing the local file for empty reads.
pub(super) fn pick_source<P>(local: &LocalState, range: Range<u64>) -> UioResult<Source>
where
    P: AccessPattern,
{
    if range.is_empty() {
        return Ok(Source::Local {
            range,
            is_sequential: P::IS_SEQUENTIAL,
        });
    }

    if range.end > local.mmap().len::<u8>()? {
        // If remote file has grown, and `reopen` hasn't been called, it is OOB
        return Err(UniversalIoError::OutOfBounds {
            start: range.start,
            end: range.end,
            elements: (range.end - range.start) as usize,
        });
    }

    let blocks_range = to_block_range(range.clone());

    // Fast path skips the bitmap mutex once the file is fully populated.
    if local.contains(blocks_range.clone()) {
        return Ok(Source::Local {
            range,
            is_sequential: P::IS_SEQUENTIAL,
        });
    }

    // BLOCK_SIZE aligned, clamped to EOF. `range` is non-empty here, so the
    // block range is non-empty and `block_aligned_fetch` yields `Some`.
    let (blocks_range, blocks_byte_range) = block_aligned_fetch(range, local.mmap().len::<u8>()?)
        .expect("non-empty range has a non-empty block range");

    Ok(Source::Remote {
        blocks_range,
        blocks_byte_range,
    })
}

/// Read a locally-cached `byte_range` from `file`. Returns an empty slice without
/// touching the local mmap when `byte_range` is empty.
///
/// # Safety
/// `byte_range` must correspond to blocks already known to be local (typically
/// because [`pick_source`] returned [`Source::Local`] for it, or
/// [`commit_and_complete`] just fetched them).
pub(super) unsafe fn read_local<R>(
    file: &DiskCache<R>,
    range: Range<u64>,
    is_sequential: bool,
) -> UioResult<&[u8]>
where
    R: DiskCacheRemote,
{
    if range.is_empty() {
        return Ok(&[]);
    }
    let local = file.state()?.local;
    if is_sequential {
        unsafe { local.read_mmap_bytes::<Sequential>(range) }
    } else {
        unsafe { local.read_mmap_bytes::<Random>(range) }
    }
}

/// Commit remote-fetched `bytes` into local mmap and unblock all waiting reads.
///
/// # Safety
/// `bytes` must be the remote content of `fetch.guard.placeholder().blocks_range` (clamped to EOF).
unsafe fn commit_and_complete<'file, R>(fetch: RemoteFetch<'file, R>, bytes: &[u8]) -> UioResult<()>
where
    R: DiskCacheRemote,
{
    let RemoteFetch {
        file,
        guard,
        fetch: timing,
    } = fetch;
    timing.complete(bytes.len());

    let local = file.state()?.local;

    unsafe {
        local.write_mmap_bytes(bytes, guard.placeholder().blocks_range.clone());
    }

    guard.complete();
    Ok(())
}

type RemotePipeline<'file, R> = <R as UniversalRead>::ReadPipeline<'file, u64>;

pub struct DiskCachePipeline<'file, R, U>
where
    R: UniversalRead + 'static,
    U: UserData,
{
    /// Pipeline for queuing remote reads. Each read's user data is its
    /// `in_flight` slot key.
    remote_pipeline: OnceCell<RemotePipeline<'file, R>>,
    /// One entry per remote read scheduled and not yet completed, keyed by
    /// the id passed to the remote pipeline as user data.
    in_flight: Slab<RemoteFetch<'file, R>>,
    /// Reads waiting on an in-flight fetch (their own or piggybacked).
    pending: VecDeque<ScheduledRead<'file, R, U>>,
    /// Resolved reads, ready to be returned by `wait`.
    results: VecDeque<(U, &'file [u8])>,
}

impl<'file, R, U> DiskCachePipeline<'file, R, U>
where
    R: UniversalRead + 'file,
    U: UserData,
{
    /// Takes the field instead of `&mut self` so callers can keep disjoint
    /// borrows of other fields (see the `vacant_entry` in `schedule`).
    fn get_or_init_remote_pipeline<'a>(
        remote_pipeline: &'a mut OnceCell<RemotePipeline<'file, R>>,
    ) -> UioResult<&'a mut RemotePipeline<'file, R>> {
        if remote_pipeline.get().is_none() {
            let remote = R::ReadPipeline::new()?;
            // We just observed the cell as empty and hold `&mut`, so set cannot fail.
            let _ = remote_pipeline.set(remote);
        }
        Ok(remote_pipeline.get_mut().expect("just initialized"))
    }

    /// Number of remote fetches currently in flight.
    #[cfg(test)]
    pub(super) fn in_flight_fetches(&self) -> usize {
        self.in_flight.len()
    }

    fn schedule_remote_fetch(
        &mut self,
        file: &'file DiskCache<R>,
        guard: PlaceholderGuard,
        is_sequential: bool,
    ) -> UioResult<Arc<Placeholder>>
    where
        R: DiskCacheRemote,
    {
        let remote_pipeline = Self::get_or_init_remote_pipeline(&mut self.remote_pipeline)?;
        let entry = self.in_flight.vacant_entry();
        let started = Instant::now();
        let placeholder = guard.placeholder().clone();
        let state = file.state()?;

        if is_sequential {
            remote_pipeline.schedule::<Sequential>(
                entry.key() as u64,
                state.remote,
                placeholder.blocks_byte_range.clone(),
                REMOTE_READ_ALIGNMENT,
            )?;
        } else {
            remote_pipeline.schedule::<Random>(
                entry.key() as u64,
                state.remote,
                placeholder.blocks_byte_range.clone(),
                REMOTE_READ_ALIGNMENT,
            )?;
        }

        entry.insert(RemoteFetch {
            file,
            guard,
            fetch: file.stats.fetch(started),
        });

        Ok(placeholder)
    }
}

impl<'file, R, U> ReadPipeline<'file, U> for DiskCachePipeline<'file, R, U>
where
    R: DiskCacheRemote + 'file,
    U: UserData,
{
    type File = DiskCache<R>;

    fn new() -> UioResult<Self> {
        Ok(Self {
            remote_pipeline: OnceCell::new(),
            in_flight: Slab::new(),
            pending: VecDeque::new(),
            results: VecDeque::new(),
        })
    }

    fn can_schedule(&mut self) -> bool {
        self.results.is_empty()
            && self
                .remote_pipeline
                .get_mut()
                .is_none_or(|remote| remote.can_schedule())
    }

    fn schedule<P: AccessPattern>(
        &mut self,
        user_data: U,
        file: &'file DiskCache<R>,
        range: Range<u64>,
        _align: usize,
    ) -> UioResult<()> {
        let state = file.state()?;
        match pick_source::<P>(state.local, range.clone())? {
            Source::Local {
                range,
                is_sequential,
            } => {
                let bytes = unsafe { read_local::<R>(file, range, is_sequential)? };
                self.results.push_back((user_data, bytes));
            }
            Source::Remote {
                blocks_range,
                blocks_byte_range,
            } => {
                let self_placeholder = self
                    .in_flight
                    .iter()
                    .find(|(_, inflight)| inflight.covers(file, &blocks_range))
                    .map(|(_, inflight)| inflight.guard.placeholder().clone());

                let placeholder = if let Some(placeholder) = self_placeholder {
                    placeholder
                } else {
                    match state.local.placeholders.get_or_register(
                        state.local,
                        blocks_range,
                        blocks_byte_range,
                    ) {
                        PlaceholderResult::AlreadyLocal => {
                            let bytes = unsafe { read_local::<R>(file, range, P::IS_SEQUENTIAL)? };
                            self.results.push_back((user_data, bytes));
                            return Ok(());
                        }
                        PlaceholderResult::Piggyback(placeholder) => placeholder,
                        PlaceholderResult::Leader(guard) => {
                            self.schedule_remote_fetch(file, guard, P::IS_SEQUENTIAL)?
                        }
                    }
                };

                self.pending.push_back(ScheduledRead {
                    user_data,
                    file,
                    range,
                    is_sequential: P::IS_SEQUENTIAL,
                    placeholder,
                });
            }
        }
        Ok(())
    }

    fn schedule_whole(
        &mut self,
        user_data: U,
        file: &'file DiskCache<R>,
        from: u64,
    ) -> UioResult<()>
    where
        Self::File: UniversalRead,
    {
        let state = file.state()?;
        let eof = state.local.mmap().len::<u8>()?;

        if from >= eof {
            return Ok(());
        }

        self.schedule::<Sequential>(user_data, file, from..eof, 1)
    }

    fn wait(&mut self) -> UioResult<Option<(U, ACow<'file>)>> {
        loop {
            if let Some((user_data, slice)) = self.results.pop_front() {
                return Ok(Some((user_data, ACow::Borrowed(slice))));
            }

            // Check if a piggybacking or completed request is done
            if let Some(idx) = self
                .pending
                .iter()
                .position(|read| read.placeholder.is_completed())
            {
                let read = self.pending.remove(idx).expect("idx exists");
                let slice = unsafe { read_local::<R>(read.file, read.range, read.is_sequential)? };
                return Ok(Some((read.user_data, ACow::Borrowed(slice))));
            }

            // Drive remote fetches this pipeline is leading
            if !self.in_flight.is_empty() {
                let Some(remote_pipeline) = self.remote_pipeline.get_mut() else {
                    return Ok(None);
                };
                let completion = match remote_pipeline.wait() {
                    Ok(completion) => completion,
                    Err(err) => {
                        self.in_flight.clear();
                        self.pending.clear();
                        self.remote_pipeline.take();
                        return Err(err);
                    }
                };
                let Some((fetch_id, bytes)) = completion else {
                    return Ok(None);
                };

                let fetch = self
                    .in_flight
                    .try_remove(fetch_id as usize)
                    .expect("completed fetch has an in-flight entry");

                // SAFETY: `bytes` is the content of `fetch.guard.placeholder().blocks_range` as scheduled.
                unsafe { commit_and_complete(fetch, &bytes)? };
                continue;
            }

            // Wait for a piggybacking read to complete or promote to leader
            if let Some(read) = self.pending.pop_front() {
                match read.placeholder.wait() {
                    WaitResult::Completed => {
                        let slice =
                            unsafe { read_local::<R>(read.file, read.range, read.is_sequential)? };
                        return Ok(Some((read.user_data, ACow::Borrowed(slice))));
                    }
                    WaitResult::Promoted(guard) => {
                        self.schedule_remote_fetch(read.file, guard, read.is_sequential)?;
                        self.pending.push_front(read);
                        continue;
                    }
                }
            }

            return Ok(None);
        }
    }
}
