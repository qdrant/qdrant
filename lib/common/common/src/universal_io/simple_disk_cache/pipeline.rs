use std::borrow::Cow;
use std::cell::OnceCell;
use std::mem::ManuallyDrop;
use std::ops::Range;

use crate::generic_consts::AccessPattern;
use crate::universal_io::simple_disk_cache::file::to_block_range;
use crate::universal_io::simple_disk_cache::{BLOCK_SIZE, DiskCache};
use crate::universal_io::traits::BorrowedReadPipeline;
use crate::universal_io::{
    self, OwnedReadPipeline, ReadRange, UniversalIoError, UniversalRead, UserData,
};

struct RemoteMeta<File, U> {
    file: File,
    blocks_range: Range<u32>,
    read_range: ReadRange,
    meta: U,
}

type BorrowedRemotePipeline<'file, R, U> = <R as UniversalRead>::BorrowedReadPipeline<'file, u8, RemoteMeta<&'file DiskCache<R>, U>> ;

pub struct DiskCachePipeline<'file, R, T, U>
where
    R: UniversalRead,
    T: bytemuck::Pod,
    U: UserData,
{
    remote_pipeline:
        OnceCell<BorrowedRemotePipeline<'file, R, U>>,
    result: Option<(U, &'file [T])>,
}

impl<'file, T, U, R> DiskCachePipeline<'file, R, T, U>
where
    T: bytemuck::Pod,
    R: UniversalRead + 'file,
    U: UserData,
{
    fn get_or_init_remote_pipeline(
        &mut self,
    ) -> universal_io::Result<
        &mut BorrowedRemotePipeline<'file, R, U>,
    > {
        if self.remote_pipeline.get().is_none() {
            let remote = R::BorrowedReadPipeline::new()?;
            // We just observed the cell as empty and hold `&mut self`, so set cannot fail.
            let _ = self.remote_pipeline.set(remote);
        }
        Ok(self.remote_pipeline.get_mut().expect("just initialized"))
    }

    fn wait_for_remote(&mut self) -> universal_io::Result<Option<(U, &'file [T])>> {
        let Some(remote_pipeline) = self.remote_pipeline.get_mut() else {
            return Ok(None);
        };

        let Some((meta, bytes)) = remote_pipeline.wait()? else {
            return Ok(None);
        };

        let RemoteMeta {
            file,
            blocks_range,
            read_range,
            meta,
        } = meta;

        let local = file.local_state()?;

        let mmap_bytes = unsafe {
            local.write_mmap_bytes(&bytes, blocks_range);

            local.read_mmap_bytes(read_range.into_byte_range::<T>())
        };

        Ok(Some((meta, bytemuck::cast_slice(mmap_bytes))))
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
        meta: U,
        file: &'file DiskCache<R>,
        range: crate::universal_io::ReadRange,
    ) -> crate::universal_io::Result<()> {
        // Short-circuit if range is empty
        if range.length == 0 {
            self.result = Some((meta, &[]));
            return Ok(());
        }

        let local = file.local_state()?;

        let byte_range = range.into_byte_range::<T>();

        // Validate byte range
        if byte_range.end > local.mmap.len() as u64 {
            // TODO: Grow local file if remote file has grown, or OOB.
            //       When growing, we need to remember to set `fully_populated` to false
            return Err(UniversalIoError::OutOfBounds {
                start: byte_range.start,
                end: byte_range.end,
                elements: range.length as usize,
            });
        }

        let blocks_range = to_block_range(byte_range.clone());

        // Check if blocks are already fetched (fast path skips the bitmap
        // mutex once the file is fully populated).
        if local.contains(blocks_range.clone()) {
            // The range is already local, put into result.
            let bytes = unsafe { local.read_mmap_bytes(byte_range) };
            self.result = Some((meta, bytemuck::cast_slice(bytes)));
            return Ok(());
        }

        // Schedule BLOCK_SIZE aligned read from remote. Making sure to not try to read past EOF.
        let byte_offset = blocks_range.start as usize * BLOCK_SIZE;
        let fetch_length = blocks_range.len() * BLOCK_SIZE;
        let max_length = local.mmap.len().saturating_sub(byte_offset);
        let blocks_byte_range = ReadRange {
            byte_offset: byte_offset as u64,
            length: fetch_length.min(max_length) as u64,
        };

        let pipelined_meta = RemoteMeta {
            file,
            blocks_range,
            read_range: range,
            meta,
        };

        let remote_pipeline = self.get_or_init_remote_pipeline()?;
        remote_pipeline.schedule::<P>(pipelined_meta, file.remote()?, blocks_byte_range)?;

        Ok(())
    }

    fn wait(&mut self) -> crate::universal_io::Result<Option<(U, std::borrow::Cow<'file, [T]>)>> {
        if let Some((meta, slice)) = self.result.take() {
            return Ok(Some((meta, Cow::Borrowed(slice))));
        }

        Ok(self
            .wait_for_remote()?
            .map(|(meta, items)| (meta, Cow::Borrowed(items))))
    }
}

impl<'file, R, T, U> BorrowedReadPipeline<'file, T, U> for DiskCachePipeline<'file, R, T, U>
where
    T: bytemuck::Pod,
    R: UniversalRead + 'file,
{
    type File = DiskCache<R>;

    fn new() -> crate::universal_io::Result<Self> {
        Ok(Self {
            remote_pipeline: OnceCell::new(),
            result: None,
        })
    }

    fn can_schedule(&mut self) -> bool {
        self.can_schedule()
    }

    fn schedule<P: AccessPattern>(
        &mut self,
        user_data: U,
        file: &'file DiskCache<R>,
        range: crate::universal_io::ReadRange,
    ) -> crate::universal_io::Result<()> {
        self.schedule::<P>(user_data, file, range)
    }

    fn wait(&mut self) -> crate::universal_io::Result<Option<(U, std::borrow::Cow<'file, [T]>)>> {
        self.wait()
    }
}

pub struct OwnedDiskCachePipeline<R, T, U>
where
    R: UniversalRead,
    T: bytemuck::Pod,
    U: UserData,
{
    file: ManuallyDrop<DiskCache<R>>,
    remote_pipeline: OnceCell<R::OwnedReadPipeline<u8, RemoteMeta<(), U>>>,
    pending: Option<(U, Range<u64>, bool)>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T, U, R> OwnedReadPipeline<T, U> for OwnedDiskCachePipeline<R, T, U>
where
    R: UniversalRead + Clone,
    T: bytemuck::Pod,
    U: UserData,
{
    type File = DiskCache<R>;

    fn new(file: Self::File) -> universal_io::Result<Self> {
        Ok(Self {
            file: ManuallyDrop::new(file),
            remote_pipeline: OnceCell::new(),
            pending: None,
            _phantom: std::marker::PhantomData,
        })
    }

    fn can_schedule(&mut self) -> bool {
        self.pending.is_none()
            && self
                .remote_pipeline
                .get_mut()
                .is_none_or(|remote| remote.can_schedule())
    }

    fn schedule<P>(&mut self, user_data: U, range: ReadRange) -> universal_io::Result<()>
    where
        P: AccessPattern,
    {
        let byte_range = range.into_byte_range::<T>();

        // Short-circuit if range is empty
        if range.length == 0 {
            self.pending = Some((user_data, byte_range, P::IS_SEQUENTIAL));
            return Ok(());
        }

        let local = self.file.local_state()?;

        // Validate byte range
        if byte_range.end > local.mmap.len() as u64 {
            // TODO: Grow local file if remote file has grown, or OOB.
            //       When growing, we need to remember to set `fully_populated` to false
            return Err(UniversalIoError::OutOfBounds {
                start: byte_range.start,
                end: byte_range.end,
                elements: range.length as usize,
            });
        }

        let blocks_range = to_block_range(byte_range.clone());

        // Check if blocks are already fetched (fast path skips the bitmap
        // mutex once the file is fully populated).
        if local.contains(blocks_range.clone()) {
            // The range is already local, put into result.
            self.pending = Some((user_data, byte_range, P::IS_SEQUENTIAL));
            return Ok(());
        }

        // Schedule BLOCK_SIZE aligned read from remote. Making sure to not try to read past EOF.
        let byte_offset = blocks_range.start as usize * BLOCK_SIZE;
        let fetch_length = blocks_range.len() * BLOCK_SIZE;
        let max_length = local.mmap.len().saturating_sub(byte_offset);
        let blocks_byte_range = ReadRange {
            byte_offset: byte_offset as u64,
            length: fetch_length.min(max_length) as u64,
        };

        let pipelined_meta = RemoteMeta {
            file: (),
            blocks_range,
            read_range: range,
            meta: user_data,
        };

        let remote_pipeline = {
            if self.remote_pipeline.get().is_none() {
                let remote = R::OwnedReadPipeline::new(self.file.remote()?.clone())?;
                // We just observed the cell as empty and hold `&mut self`, so set cannot fail.
                let _ = self.remote_pipeline.set(remote);
            }
            self.remote_pipeline.get_mut().expect("just initialized")
        };
        remote_pipeline.schedule::<P>(pipelined_meta, blocks_byte_range)?;

        Ok(())
    }

    fn wait(&mut self) -> universal_io::Result<Option<(U, Cow<'_, [T]>)>> {
        if let Some((user_data, byte_range, _is_sequential)) = self.pending.take() {
            let local = self.file.local_state()?;
            let bytes = unsafe { local.read_mmap_bytes(byte_range) };
            let slice = bytemuck::cast_slice(bytes);
            return Ok(Some((user_data, Cow::Borrowed(slice))));
        }

        let Some(remote_pipeline) = self.remote_pipeline.get_mut() else {
            return Ok(None);
        };

        let Some((meta, bytes)) = remote_pipeline.wait()? else {
            return Ok(None);
        };

        let RemoteMeta {
            file: _,
            blocks_range,
            read_range,
            meta,
        } = meta;

        let local = self.file.local_state()?;

        let mmap_bytes = unsafe {
            local.write_mmap_bytes(&bytes, blocks_range);

            local.read_mmap_bytes(read_range.into_byte_range::<T>())
        };

        let items = bytemuck::cast_slice(mmap_bytes);

        Ok(Some((meta, Cow::Borrowed(items))))
    }
}
