//! The [`UniversalRead`] implementation for [`DiskCache`] — the public read
//! surface. The heavy lifting lives elsewhere: first-use init in [`super::init`],
//! growth handling in [`super::reopen`].
use std::borrow::Cow;
use std::ops::Range;

use super::DiskCache;
use crate::ext::aligned_vec::ACow;
use crate::generic_consts::{AccessPattern, Sequential};
use crate::universal_io::simple_disk_cache::fs::DiskCacheFs;
use crate::universal_io::simple_disk_cache::pipeline::DiskCachePipeline;
use crate::universal_io::simple_disk_cache::{BLOCK_SIZE, DiskCacheRemote};
use crate::universal_io::{
    Item, ReadPipeline, ReadRange, UioResult, UniversalKind, UniversalRead, UserData,
};

impl<R> DiskCache<R>
where
    R: DiskCacheRemote,
{
    /// Make sure every byte in the range `byte_start..remote_len` is present on the local file
    fn populate_from(&self, byte_start: u64) -> UioResult<()> {
        if crate::low_memory::low_memory_mode().skip_populate() {
            return Ok(());
        }

        let remote_len = self.state()?.remote.len::<u8>()?;
        if remote_len == 0 {
            return Ok(());
        }

        let one_byte_per_block = (byte_start..remote_len)
            .step_by(BLOCK_SIZE)
            .map(|byte_offset| ((), ReadRange::one(byte_offset)));

        // Read one byte per block purely to fault each block into the local
        // cache; the bytes themselves are discarded.
        self.read_batch::<Sequential, u8, (), _>(one_byte_per_block, |(), _bytes| {
            UioResult::Ok(())
        })?;

        Ok(())
    }
}

impl<R> UniversalRead for DiskCache<R>
where
    R: DiskCacheRemote,
{
    type Fs = DiskCacheFs<R>;

    type ReadPipeline<'a, U>
        = DiskCachePipeline<'a, R, U>
    where
        Self: 'a,
        R: 'a,
        U: UserData;

    fn reopen(&mut self) -> UioResult<()> {
        self.reopen_impl()
    }

    fn read_bytes<P: AccessPattern>(&self, range: Range<u64>, align: usize) -> UioResult<ACow<'_>> {
        let mut pipeline = DiskCachePipeline::<R, ()>::new()?;
        pipeline.schedule::<P>((), self, range, align)?;
        let (_, bytes) = pipeline.wait()?.expect("there's exactly one read");
        Ok(bytes)
    }

    fn read_whole<T: Item>(&self) -> UioResult<Cow<'_, [T]>> {
        self.prefill_if_uninit()?;
        let length = self.len::<T>()?;
        self.read::<Sequential, T>(ReadRange {
            byte_offset: 0,
            length,
        })
    }

    fn len<T>(&self) -> UioResult<u64> {
        self.state()?.local.mmap().len::<T>()
    }

    fn populate(&self) -> UioResult<()> {
        self.populate_from(0)
    }

    fn populate_auto() -> bool {
        false
    }

    fn clear_ram_cache(&self) -> UioResult<()> {
        // Only touch an already-live mirror; don't force initialization just to
        // clear a cache that may not exist yet.
        if self.is_ready() {
            self.state()?.local.mmap().clear_ram_cache()?;
        }
        Ok(())
    }

    fn kind() -> UniversalKind {
        UniversalKind::SimpleDiskCache
    }
}
