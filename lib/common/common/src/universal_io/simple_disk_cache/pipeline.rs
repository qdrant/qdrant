use std::borrow::Cow;
use std::cell::OnceCell;
use std::ops::Range;

use crate::generic_consts::AccessPattern;
use crate::universal_io::read::UniversalReadPipeline;
use crate::universal_io::simple_disk_cache::BLOCK_SIZE;
use crate::universal_io::simple_disk_cache::file::to_block_range;
use crate::universal_io::{self, DiskCache, ReadRange, UniversalIoError, UniversalRead};

struct RemoteMeta<'file, Meta, R> {
    file: &'file DiskCache<R>,
    blocks_range: Range<u32>,
    read_range: ReadRange,
    meta: Meta,
}

pub struct DiskCachePipeline<'file, T, Meta, R>
where
    R: UniversalRead + 'file,
    T: bytemuck::Pod,
{
    remote_pipeline: OnceCell<R::ReadPipeline<'file, u8, RemoteMeta<'file, Meta, R>>>,
    result: Option<(Meta, &'file [T])>,
}

impl<'file, T, Meta, R> DiskCachePipeline<'file, T, Meta, R>
where
    T: bytemuck::Pod,
    R: UniversalRead + 'file,
{
    fn get_or_init_remote_pipeline(
        &mut self,
    ) -> universal_io::Result<&mut R::ReadPipeline<'file, u8, RemoteMeta<'file, Meta, R>>> {
        if self.remote_pipeline.get().is_none() {
            let remote = R::ReadPipeline::new()?;
            // We just observed the cell as empty and hold `&mut self`, so set cannot fail.
            let _ = self.remote_pipeline.set(remote);
        }
        Ok(self.remote_pipeline.get_mut().expect("just initialized"))
    }

    fn wait_for_remote(&mut self) -> universal_io::Result<Option<(Meta, &'file [T])>> {
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
}

impl<'file, T, Meta, Remote> UniversalReadPipeline<'file, T, Meta>
    for DiskCachePipeline<'file, T, Meta, Remote>
where
    T: bytemuck::Pod + Copy + 'static,
    Remote: UniversalRead,
{
    type File = DiskCache<Remote>;

    fn new() -> crate::universal_io::Result<Self> {
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
        meta: Meta,
        file: &'file DiskCache<Remote>,
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
            // TODO: Grow local file if remote file has grown, or OOB
            return Err(UniversalIoError::OutOfBounds {
                start: byte_range.start,
                end: byte_range.end,
                elements: range.length as usize,
            });
        }

        let blocks_range = to_block_range(byte_range.clone());

        // Check if blocks are already fetched
        if local.fetched.lock().contains_range(blocks_range.clone()) {
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
        remote_pipeline.schedule::<P>(pipelined_meta, &file.remote, blocks_byte_range)?;

        Ok(())
    }

    fn wait(
        &mut self,
    ) -> crate::universal_io::Result<Option<(Meta, std::borrow::Cow<'file, [T]>)>> {
        if let Some((meta, slice)) = self.result.take() {
            return Ok(Some((meta, Cow::Borrowed(slice))));
        }

        Ok(self
            .wait_for_remote()?
            .map(|(meta, items)| (meta, Cow::Borrowed(items))))
    }
}
