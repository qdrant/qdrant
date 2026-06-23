use std::cell::OnceCell;
use std::ops::Range;

use crate::ext::aligned_vec::ACow;
use crate::generic_consts::{AccessPattern, Random, Sequential};
use crate::universal_io::simple_disk_cache::local_state::LocalState;
use crate::universal_io::simple_disk_cache::{
    BLOCK_SIZE, DiskCache, DiskCacheRemote, to_block_range,
};
use crate::universal_io::traits::BorrowedReadPipeline;
use crate::universal_io::{
    self, OwnedReadPipeline, Result, UniversalIoError, UniversalRead, UserData,
};

struct RemoteMeta<File, U> {
    file: File,
    scheduled_read: ScheduledRead,
    user_data: U,
}

enum ScheduledRead {
    Range {
        blocks_range: Range<u32>,
        read_range: Range<u64>,
    },
    Whole {
        from: u64,
    },
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

    // BLOCK_SIZE aligned, clamped to EOF.
    let byte_offset = u64::from(blocks_range.start) * BLOCK_SIZE as u64;
    let fetch_length = blocks_range.len() as u64 * BLOCK_SIZE as u64;
    let max_length = local.mmap().len::<u8>()?.saturating_sub(byte_offset);
    let blocks_byte_range = byte_offset..byte_offset + max_length.min(fetch_length);

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
    let local = &file.state()?.local;
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
    let mut known_len = None;
    let (blocks_range, byte_range) = match scheduled_read {
        ScheduledRead::Range {
            blocks_range,
            read_range,
        } => (blocks_range, read_range),
        ScheduledRead::Whole { from } => {
            // derive whole ranges from the actual bytes returned.
            let byte_len = bytes.len() as u64;
            let eof = from + byte_len;
            let byte_range = from..eof;
            known_len = Some(eof);
            let blocks_range = to_block_range(byte_range.clone());
            (blocks_range, byte_range)
        }
    };

    let state = if let Some(state) = file.state.get() {
        state
    } else {
        let mut init_guard = file.init_lock.lock();
        if file.state.get().is_none() {
            file.init_state(&mut init_guard, true, known_len)?;
        }
        file.state.get().expect("just initialized")
    };

    unsafe {
        state.local.write_mmap_bytes(bytes, blocks_range);
        state.local.read_mmap_bytes::<Random>(byte_range)
    }
}

type BorrowedRemotePipeline<'file, R, U> =
    <R as UniversalRead>::BorrowedReadPipeline<'file, RemoteMeta<&'file DiskCache<R>, U>>;

pub struct DiskCachePipeline<'file, R, U>
where
    R: UniversalRead,
    U: UserData,
{
    /// Pipeline for queuing remote reads.
    remote_pipeline: OnceCell<BorrowedRemotePipeline<'file, R, U>>,
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
    ) -> universal_io::Result<&mut BorrowedRemotePipeline<'file, R, U>> {
        if self.remote_pipeline.get().is_none() {
            let remote = R::BorrowedReadPipeline::new()?;
            // We just observed the cell as empty and hold `&mut self`, so set cannot fail.
            let _ = self.remote_pipeline.set(remote);
        }
        Ok(self.remote_pipeline.get_mut().expect("just initialized"))
    }
}

impl<'file, R, U> BorrowedReadPipeline<'file, U> for DiskCachePipeline<'file, R, U>
where
    R: DiskCacheRemote + 'file,
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
        align: usize,
    ) -> universal_io::Result<()> {
        let state = file.state()?;
        match pick_source::<P>(&state.local, range.clone())? {
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
                    scheduled_read: ScheduledRead::Range {
                        blocks_range,
                        read_range: range,
                    },
                    user_data,
                };
                let remote_pipeline = self.get_or_init_remote_pipeline()?;
                remote_pipeline.schedule::<P>(
                    remote_meta,
                    &state.remote,
                    blocks_byte_range,
                    align,
                )?;
            }
        }
        Ok(())
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

pub struct OwnedDiskCachePipeline<R, U>
where
    R: UniversalRead,
    U: UserData,
{
    /// The file being cached.
    file: DiskCache<R>,
    /// Pipeline for queuing remote reads.
    remote_pipeline: Option<R::OwnedReadPipeline<RemoteMeta<(), U>>>,
    /// A result ready to be read, contains (user_data, byte_range).
    ready: Option<(U, Range<u64>, bool)>,
}

impl<R, U> OwnedDiskCachePipeline<R, U>
where
    R: DiskCacheRemote,
    U: UserData,
{
    fn get_or_init_remote_pipeline(
        &mut self,
    ) -> universal_io::Result<&mut R::OwnedReadPipeline<RemoteMeta<(), U>>> {
        if self.remote_pipeline.is_none() {
            let remote = if let Some(state) = self.file.state.get() {
                state.remote.clone()
            } else {
                self.file.open_remote()?
            };
            let pipeline = R::OwnedReadPipeline::new(remote)?;
            self.remote_pipeline = Some(pipeline);
        }
        Ok(self.remote_pipeline.as_mut().expect("just initialized"))
    }
}

impl<R, U> OwnedReadPipeline<U> for OwnedDiskCachePipeline<R, U>
where
    R: DiskCacheRemote,
{
    type File = DiskCache<R>;

    fn new(file: Self::File) -> universal_io::Result<Self> {
        Ok(Self {
            file,
            remote_pipeline: None,
            ready: None,
        })
    }

    fn can_schedule(&mut self) -> bool {
        self.ready.is_none()
            && self
                .remote_pipeline
                .as_mut()
                .is_none_or(|pipeline| pipeline.can_schedule())
    }

    fn schedule<P: AccessPattern>(
        &mut self,
        user_data: U,
        range: Range<u64>,
        align: usize,
    ) -> universal_io::Result<()> {
        match pick_source::<P>(&self.file.state()?.local, range.clone())? {
            Source::Local {
                range,
                is_sequential,
            } => {
                self.ready = Some((user_data, range, is_sequential));
            }
            Source::Remote {
                blocks_range,
                blocks_byte_range,
            } => {
                let remote_meta = RemoteMeta {
                    file: (),
                    scheduled_read: ScheduledRead::Range {
                        blocks_range,
                        read_range: range,
                    },
                    user_data,
                };
                let remote_pipeline = self.get_or_init_remote_pipeline()?;
                remote_pipeline.schedule::<P>(remote_meta, blocks_byte_range, align)?;
            }
        }
        Ok(())
    }

    fn schedule_whole(&mut self, user_data: U, from: u64) -> Result<()> {
        // If local has already been initialized, use the mmap length
        if let Some(state) = &self.file.state.get() {
            let eof = state.local.mmap().len::<u8>()?;
            if from >= eof {
                return Ok(());
            }
            return self.schedule::<Sequential>(user_data, from..eof, 1);
        }

        // Use schedule_whole on the remote pipeline directly
        let remote_meta = RemoteMeta {
            file: (),
            scheduled_read: ScheduledRead::Whole { from },
            user_data,
        };
        let remote_pipeline = self.get_or_init_remote_pipeline()?;
        remote_pipeline.schedule_whole(remote_meta, from)
    }

    fn wait(&mut self) -> universal_io::Result<Option<(U, ACow<'_>)>> {
        if let Some((user_data, range, is_sequential)) = self.ready.take() {
            // SAFETY: being in `pending` confirms the range is local (or empty).
            let bytes = unsafe { read_local::<R>(&self.file, range, is_sequential)? };
            return Ok(Some((user_data, ACow::Borrowed(bytes))));
        }

        let Some(remote_pipeline) = self.remote_pipeline.as_mut() else {
            return Ok(None);
        };
        let Some((remote_meta, bytes)) = remote_pipeline.wait()? else {
            return Ok(None);
        };

        let RemoteMeta {
            file: _,
            scheduled_read,
            user_data,
        } = remote_meta;

        let items =
            // TODO: if schedule_whole is used other than during `open`, `commit_and_read` will call `remote.len()` regardless.
            unsafe { commit_and_read::<R>(&self.file, &bytes, scheduled_read)? };
        Ok(Some((user_data, ACow::Borrowed(items))))
    }

    fn into_inner(self) -> DiskCache<R> {
        self.file
    }
}
