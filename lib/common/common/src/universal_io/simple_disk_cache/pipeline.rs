use std::borrow::Cow;
use std::cell::OnceCell;
use std::ops::Range;

use crate::generic_consts::AccessPattern;
use crate::universal_io::simple_disk_cache::local_state::LocalState;
use crate::universal_io::simple_disk_cache::{BLOCK_SIZE, DiskCache, to_block_range};
use crate::universal_io::traits::BorrowedReadPipeline;
use crate::universal_io::{
    self, OwnedReadPipeline, ReadRange, Result, UniversalIoError, UniversalRead, UserData,
};

struct RemoteMeta<File, U> {
    file: File,
    blocks_range: Range<u32>,
    read_range: ReadRange,
    user_data: U,
}

/// Outcome of [`plan_schedule`]: either the requested range is already available
/// locally (or is empty) and needs no remote work, or a remote read must be
/// scheduled for `blocks_byte_range` covering `blocks_range`.
enum Source {
    Local {
        byte_range: Range<u64>,
    },
    Remote {
        blocks_range: Range<u32>,
        blocks_byte_range: ReadRange,
    },
}

/// Decide whether `range` can be answered from local mmap or needs a remote fetch.
///
/// Avoids materializing the local file for empty reads.
fn pick_source<'a, T>(local_state: &'a LocalState, range: ReadRange) -> Result<Source>
where
    T: bytemuck::Pod,
{
    let byte_range = range.into_byte_range::<T>();
    if range.length == 0 {
        return Ok(Source::Local { byte_range });
    }

    if byte_range.end > local_state.mmap.len() as u64 {
        // TODO: Grow local file if remote file has grown, or OOB.
        //       When growing, we need to remember to set `fully_populated` to false
        return Err(UniversalIoError::OutOfBounds {
            start: byte_range.start,
            end: byte_range.end,
            elements: range.length as usize,
        });
    }

    let blocks_range = to_block_range(byte_range.clone());

    // Fast path skips the bitmap mutex once the file is fully populated.
    if local_state.contains(blocks_range.clone()) {
        return Ok(Source::Local { byte_range });
    }

    // BLOCK_SIZE aligned, clamped to EOF.
    let byte_offset = blocks_range.start as usize * BLOCK_SIZE;
    let fetch_length = blocks_range.len() * BLOCK_SIZE;
    let max_length = local_state.mmap.len().saturating_sub(byte_offset);
    let blocks_byte_range = ReadRange {
        byte_offset: byte_offset as u64,
        length: fetch_length.min(max_length) as u64,
    };

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
unsafe fn read_local<R, T>(
    file: &DiskCache<R>,
    byte_range: Range<u64>,
) -> universal_io::Result<&[T]>
where
    R: UniversalRead,
    T: bytemuck::Pod,
{
    if byte_range.is_empty() {
        return Ok(&[]);
    }
    let local = file.local_state()?;
    let bytes = unsafe { local.read_mmap_bytes(byte_range) };
    Ok(bytemuck::cast_slice(bytes))
}

/// Commit remote-fetched `bytes` into local mmap and re-read the user's slice.
///
/// # Safety
/// `blocks_range` and `read_range` must correspond to the `bytes` section of the mmap.
unsafe fn commit_and_read<'a, R, T>(
    file: &'a DiskCache<R>,
    bytes: &[u8],
    blocks_range: Range<u32>,
    read_range: ReadRange,
) -> universal_io::Result<&'a [T]>
where
    R: UniversalRead,
    T: bytemuck::Pod,
{
    let local = file.local_state()?;
    let mmap_bytes = unsafe {
        local.write_mmap_bytes(bytes, blocks_range);
        local.read_mmap_bytes(read_range.into_byte_range::<T>())
    };
    Ok(bytemuck::cast_slice(mmap_bytes))
}

type BorrowedRemotePipeline<'file, R, U> =
    <R as UniversalRead>::BorrowedReadPipeline<'file, u8, RemoteMeta<&'file DiskCache<R>, U>>;

pub struct DiskCachePipeline<'file, R, T, U>
where
    R: UniversalRead,
    T: bytemuck::Pod,
    U: UserData,
{
    /// Pipeline for queuing remote reads.
    remote_pipeline: OnceCell<BorrowedRemotePipeline<'file, R, U>>,
    /// A result of (user_data, bytes)
    result: Option<(U, &'file [T])>,
}

impl<'file, R, T, U> DiskCachePipeline<'file, R, T, U>
where
    R: UniversalRead + 'file,
    T: bytemuck::Pod,
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

impl<'file, R, T, U> BorrowedReadPipeline<'file, T, U> for DiskCachePipeline<'file, R, T, U>
where
    R: UniversalRead + 'file,
    T: bytemuck::Pod,
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
        range: ReadRange,
    ) -> universal_io::Result<()> {
        match pick_source::<T>(file.local_state()?, range)? {
            Source::Local { byte_range } => {
                // SAFETY: Source::Local confirms the range is local (or empty).
                let bytes = unsafe { read_local::<R, T>(file, byte_range)? };
                self.result = Some((user_data, bytes));
            }
            Source::Remote {
                blocks_range,
                blocks_byte_range,
            } => {
                let remote_meta = RemoteMeta {
                    file,
                    blocks_range,
                    read_range: range,
                    user_data,
                };
                let remote_pipeline = self.get_or_init_remote_pipeline()?;
                remote_pipeline.schedule::<P>(remote_meta, file.remote()?, blocks_byte_range)?;
            }
        }
        Ok(())
    }

    fn wait(&mut self) -> universal_io::Result<Option<(U, Cow<'file, [T]>)>> {
        if let Some((user_data, slice)) = self.result.take() {
            return Ok(Some((user_data, Cow::Borrowed(slice))));
        }

        let Some(remote_pipeline) = self.remote_pipeline.get_mut() else {
            return Ok(None);
        };
        let Some((remote_meta, bytes)) = remote_pipeline.wait()? else {
            return Ok(None);
        };

        let RemoteMeta {
            file,
            blocks_range,
            read_range,
            user_data,
        } = remote_meta;

        // SAFETY: `blocks_range` and `read_range` match what was scheduled.
        let items = unsafe { commit_and_read::<R, T>(file, &bytes, blocks_range, read_range)? };
        Ok(Some((user_data, Cow::Borrowed(items))))
    }
}

pub struct OwnedDiskCachePipeline<R, T, U>
where
    R: UniversalRead,
    T: bytemuck::Pod,
    U: UserData,
{
    /// The file being cached.
    file: DiskCache<R>,
    /// Pipeline for queuing remote reads.
    remote_pipeline: OnceCell<R::OwnedReadPipeline<u8, RemoteMeta<(), U>>>,
    /// A result ready to be read, contains (user_data, byte_range).
    ready: Option<(U, Range<u64>)>,
    _phantom: std::marker::PhantomData<T>,
}

impl<R, T, U> OwnedDiskCachePipeline<R, T, U>
where
    R: UniversalRead + Clone,
    T: bytemuck::Pod,
    U: UserData,
{
    fn get_or_init_remote_pipeline(
        &mut self,
    ) -> universal_io::Result<&mut R::OwnedReadPipeline<u8, RemoteMeta<(), U>>> {
        if self.remote_pipeline.get().is_none() {
            let remote = R::OwnedReadPipeline::new(self.file.remote()?.clone())?;
            // We just observed the cell as empty and hold `&mut self`, so set cannot fail.
            let _ = self.remote_pipeline.set(remote);
        }
        Ok(self.remote_pipeline.get_mut().expect("just initialized"))
    }
}

impl<R, T, U> OwnedReadPipeline<T, U> for OwnedDiskCachePipeline<R, T, U>
where
    R: UniversalRead + Clone,
    T: bytemuck::Pod,
{
    type File = DiskCache<R>;

    fn new(file: Self::File) -> universal_io::Result<Self> {
        Ok(Self {
            file,
            remote_pipeline: OnceCell::new(),
            ready: None,
            _phantom: std::marker::PhantomData,
        })
    }

    fn can_schedule(&mut self) -> bool {
        self.ready.is_none()
            && self
                .remote_pipeline
                .get_mut()
                .is_none_or(|remote| remote.can_schedule())
    }

    fn schedule<P>(&mut self, user_data: U, range: ReadRange) -> universal_io::Result<()>
    where
        P: AccessPattern,
    {
        match pick_source::<T>(self.file.local_state()?, range)? {
            Source::Local { byte_range } => {
                self.ready = Some((user_data, byte_range));
            }
            Source::Remote {
                blocks_range,
                blocks_byte_range,
            } => {
                let remote_meta = RemoteMeta {
                    file: (),
                    blocks_range,
                    read_range: range,
                    user_data,
                };
                let remote_pipeline = self.get_or_init_remote_pipeline()?;
                remote_pipeline.schedule::<P>(remote_meta, blocks_byte_range)?;
            }
        }
        Ok(())
    }

    fn wait(&mut self) -> universal_io::Result<Option<(U, Cow<'_, [T]>)>> {
        if let Some((user_data, byte_range)) = self.ready.take() {
            // SAFETY: being in `pending` confirms the range is local (or empty).
            let items = unsafe { read_local::<R, T>(&self.file, byte_range)? };
            return Ok(Some((user_data, Cow::Borrowed(items))));
        }

        let Some(remote_pipeline) = self.remote_pipeline.get_mut() else {
            return Ok(None);
        };
        let Some((remote_meta, bytes)) = remote_pipeline.wait()? else {
            return Ok(None);
        };

        let RemoteMeta {
            file: _,
            blocks_range,
            read_range,
            user_data,
        } = remote_meta;

        let items =
            unsafe { commit_and_read::<R, T>(&self.file, &bytes, blocks_range, read_range)? };
        Ok(Some((user_data, Cow::Borrowed(items))))
    }
}
