use std::cell::OnceCell;
use std::fmt;
use std::ops::Range;

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

struct RemoteMeta<File, U> {
    file: File,
    scheduled_read: ScheduledRead,
    user_data: U,
}

impl<File, U: fmt::Debug> fmt::Debug for RemoteMeta<File, U> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            scheduled_read,
            user_data,
            file: _,
        } = self;
        f.debug_struct("RemoteMeta")
            .field("scheduled_read", scheduled_read)
            .field("user_data", user_data)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct ScheduledRead {
    blocks_range: Range<u32>,
    read_range: Range<u64>,
}

/// Outcome of [`plan_schedule`]: either the requested range is already available
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
/// because [`plan_schedule`] returned [`SchedulePlan::Local`] for it, or
/// [`complete_remote_read`] just fetched them).
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

/// Commit remote-fetched `bytes` into local mmap and re-read the user's slice.
///
/// # Safety
/// `blocks_range` and `read_range` must correspond to the `bytes` section of the mmap.
unsafe fn commit_and_read<'a, R>(
    file: &'a DiskCache<R>,
    bytes: &[u8],
    scheduled_read: ScheduledRead,
) -> universal_io::Result<&'a [u8]>
where
    R: DiskCacheRemote,
{
    let ScheduledRead {
        blocks_range,
        read_range,
    } = scheduled_read;

    // The mirror is already materialized: scheduling this remote read went
    // through `file.state()` (see `schedule`), which forces initialization.
    let local = file.state()?.local;

    unsafe {
        local.write_mmap_bytes(bytes, blocks_range);
        local.read_mmap_bytes::<Random>(read_range)
    }
}

type RemotePipeline<'file, R, U> =
    <R as UniversalRead>::ReadPipeline<'file, RemoteMeta<&'file DiskCache<R>, U>>;

pub struct DiskCachePipeline<'file, R, U>
where
    R: UniversalRead + 'static,
    U: UserData,
{
    /// Pipeline for queuing remote reads.
    remote_pipeline: OnceCell<RemotePipeline<'file, R, U>>,
    /// A result of (user_data, bytes)
    result: Option<(U, &'file [u8])>,
}

impl<'file, R, U> DiskCachePipeline<'file, R, U>
where
    R: UniversalRead + 'file,
    U: UserData,
{
    fn get_or_init_remote_pipeline(
        &mut self,
    ) -> universal_io::Result<&mut RemotePipeline<'file, R, U>> {
        if self.remote_pipeline.get().is_none() {
            let remote = R::ReadPipeline::new()?;
            // We just observed the cell as empty and hold `&mut self`, so set cannot fail.
            let _ = self.remote_pipeline.set(remote);
        }
        Ok(self.remote_pipeline.get_mut().expect("just initialized"))
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
            result: None,
        })
    }

    fn can_schedule(&mut self) -> bool {
        self.result.is_none()
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
                self.result = Some((user_data, bytes));
            }
            Source::Remote {
                blocks_range,
                blocks_byte_range,
            } => {
                let remote_meta = RemoteMeta {
                    file,
                    scheduled_read: ScheduledRead {
                        blocks_range,
                        read_range: range,
                    },
                    user_data,
                };
                let remote_pipeline = self.get_or_init_remote_pipeline()?;
                remote_pipeline.schedule::<P>(
                    remote_meta,
                    state.remote,
                    blocks_byte_range,
                    REMOTE_READ_ALIGNMENT,
                )?;
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
        if let Some((user_data, slice)) = self.result.take() {
            return Ok(Some((user_data, ACow::Borrowed(slice))));
        }

        let Some(remote_pipeline) = self.remote_pipeline.get_mut() else {
            return Ok(None);
        };
        let Some((remote_meta, bytes)) = remote_pipeline.wait()? else {
            return Ok(None);
        };

        let RemoteMeta {
            file,
            scheduled_read,
            user_data,
        } = remote_meta;

        // SAFETY: `blocks_range` and `read_range` match what was scheduled.
        let items = unsafe { commit_and_read::<R>(file, &bytes, scheduled_read)? };
        Ok(Some((user_data, ACow::Borrowed(items))))
    }
}
