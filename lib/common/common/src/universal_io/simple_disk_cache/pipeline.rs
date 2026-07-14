use std::cell::OnceCell;
use std::collections::VecDeque;
use std::ops::Range;

use slab::Slab;

use crate::ext::aligned_vec::ACow;
use crate::generic_consts::{AccessPattern, Random, Sequential};
use crate::universal_io::simple_disk_cache::local_state::LocalState;
use crate::universal_io::simple_disk_cache::{
    DiskCache, DiskCacheRemote, block_aligned_fetch, to_block_range,
};
use crate::universal_io::{self, ReadPipeline, Result, UniversalIoError, UniversalRead, UserData};

#[cfg(target_os = "linux")]
/// Required alignment when using io_uring with `O_DIRECT` on Linux
pub(super) const REMOTE_READ_ALIGNMENT: usize = universal_io::io_uring::KERNEL_PAGE_SIZE;

#[cfg(not(target_os = "linux"))]
/// Default alignment on non-Linux platforms
pub(super) const REMOTE_READ_ALIGNMENT: usize = 1;

/// A remote fetch in flight: which file and blocks it covers, and every
/// scheduled read waiting on it — the read that triggered the fetch, plus any
/// later reads whose blocks it fully covers. See [`DiskCachePipeline::schedule`].
///
/// Lives in `DiskCachePipeline::in_flight`, keyed by its slab slot, which
/// also serves as the fetch's user data on the remote pipeline.
struct InFlightFetch<'file, R, U>
where
    R: UniversalRead + 'static,
{
    /// Identifies which file this fetch belongs to. Matched by reference
    /// identity (see `schedule`'s piggyback check): sound because `DiskCache`
    /// isn't `Clone`, and `'file` pins the borrow for the pipeline's lifetime,
    /// so two live `&'file DiskCache<R>` referring to the same logical file
    /// are guaranteed to be the same reference.
    file: &'file DiskCache<R>,
    /// Blocks the fetch covers; committed to the local mirror on completion.
    blocks_range: Range<u32>,
    /// `(user_data, byte range)` of every read resolved by this fetch; each is
    /// re-read from the local mirror once the fetch commits. Never empty.
    reads: Vec<(U, Range<u64>)>,
}

/// Outcome of [`pick_source`]: either the requested range is already available
/// locally (or is empty) and needs no remote work, or a remote read must be
/// scheduled for `blocks_byte_range` covering `blocks_range`.
enum Source {
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
fn pick_source<P>(local: &LocalState, range: Range<u64>) -> Result<Source>
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
/// [`commit_and_read`] just fetched them).
unsafe fn read_local<R>(
    file: &DiskCache<R>,
    range: Range<u64>,
    is_sequential: bool,
) -> universal_io::Result<&[u8]>
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

/// Commit remote-fetched `bytes` into local mmap and resolve every read waiting
/// on `fetch`, pushing the resulting slices to `results`.
///
/// # Safety
/// `bytes` must be the remote content of `fetch.blocks_range` (clamped to EOF),
/// and every range in `fetch.reads` must be covered by those blocks.
unsafe fn commit_and_read<'file, R, U>(
    fetch: InFlightFetch<'file, R, U>,
    bytes: &[u8],
    results: &mut VecDeque<(U, &'file [u8])>,
) -> universal_io::Result<()>
where
    R: DiskCacheRemote,
    U: UserData,
{
    let InFlightFetch {
        file,
        blocks_range,
        reads,
    } = fetch;

    // The mirror is already materialized: scheduling this remote read went
    // through `file.state()` (see `schedule`), which forces initialization.
    let local = file.state()?.local;

    unsafe {
        local.write_mmap_bytes(bytes, blocks_range);
        for (user_data, read_range) in reads {
            let slice = local.read_mmap_bytes::<Random>(read_range)?;
            results.push_back((user_data, slice));
        }
    }

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
    in_flight: Slab<InFlightFetch<'file, R, U>>,
    /// Resolved reads, ready to be returned by `wait`.
    results: VecDeque<(U, &'file [u8])>,
}

impl<'file, R, U> DiskCachePipeline<'file, R, U>
where
    R: UniversalRead + 'file,
    U: UserData,
{
    fn get_or_init_remote_pipeline(
        &mut self,
    ) -> universal_io::Result<&mut RemotePipeline<'file, R>> {
        if self.remote_pipeline.get().is_none() {
            let remote = R::ReadPipeline::new()?;
            // We just observed the cell as empty and hold `&mut self`, so set cannot fail.
            let _ = self.remote_pipeline.set(remote);
        }
        Ok(self.remote_pipeline.get_mut().expect("just initialized"))
    }

    /// Number of remote fetches currently in flight.
    #[cfg(test)]
    pub(super) fn in_flight_fetches(&self) -> usize {
        self.in_flight.len()
    }
}

impl<'file, R, U> ReadPipeline<'file, U> for DiskCachePipeline<'file, R, U>
where
    R: DiskCacheRemote + 'file,
    U: UserData,
{
    type File = DiskCache<R>;

    fn new() -> universal_io::Result<Self> {
        Ok(Self {
            remote_pipeline: OnceCell::new(),
            in_flight: Slab::new(),
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
    ) -> universal_io::Result<()> {
        let state = file.state()?;
        match pick_source::<P>(state.local, range.clone())? {
            Source::Local {
                range,
                is_sequential,
            } => {
                // SAFETY: Source::Local confirms the range is local (or empty).
                let bytes = unsafe { read_local::<R>(file, range, is_sequential)? };
                self.results.push_back((user_data, bytes));
            }
            Source::Remote {
                blocks_range,
                blocks_byte_range,
            } => {
                // An in-flight fetch for the same file covering all needed
                // blocks resolves this read too: piggyback on it instead of
                // fetching the same blocks twice.
                if let Some((_, fetch)) = self.in_flight.iter_mut().find(|(_, inflight)| {
                    std::ptr::eq(inflight.file, file)
                        && inflight.blocks_range.start <= blocks_range.start
                        && blocks_range.end <= inflight.blocks_range.end
                }) {
                    fetch.reads.push((user_data, range));
                    return Ok(());
                }

                // Peek the slot `in_flight` would assign next, without
                // consuming it: a failed remote schedule must leave no trace,
                // and only the `insert` below actually commits a slot. Since
                // nothing else touches `in_flight` in between, `insert` is
                // guaranteed to land on this same key.
                let id = self.in_flight.vacant_entry().key() as u64;
                let remote_pipeline = self.get_or_init_remote_pipeline()?;
                remote_pipeline.schedule::<P>(
                    id,
                    state.remote,
                    blocks_byte_range,
                    REMOTE_READ_ALIGNMENT,
                )?;
                self.in_flight.insert(InFlightFetch {
                    file,
                    blocks_range,
                    reads: vec![(user_data, range)],
                });
            }
        }
        Ok(())
    }

    fn schedule_whole(&mut self, user_data: U, file: &'file DiskCache<R>, from: u64) -> Result<()>
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

    fn wait(&mut self) -> universal_io::Result<Option<(U, ACow<'file>)>> {
        if let Some((user_data, slice)) = self.results.pop_front() {
            return Ok(Some((user_data, ACow::Borrowed(slice))));
        }

        let Some(remote_pipeline) = self.remote_pipeline.get_mut() else {
            return Ok(None);
        };
        let completion = match remote_pipeline.wait() {
            Ok(completion) => completion,
            Err(err) => {
                // Note: `wait()` interface is blind to which request belongs
                // to which response, so, we can't remove the requests waiting for
                // this particular failed response.
                //
                // Let's just drop all in-flight requests.
                self.in_flight.clear();
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

        // SAFETY: `bytes` is the content of `fetch.blocks_range` as scheduled,
        // and piggybacked reads were accepted only when covered by those blocks.
        unsafe { commit_and_read(fetch, &bytes, &mut self.results)? };

        let (user_data, slice) = self
            .results
            .pop_front()
            .expect("a completed fetch resolves at least one read");
        Ok(Some((user_data, ACow::Borrowed(slice))))
    }
}
