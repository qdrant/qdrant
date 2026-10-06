//! The [`UniversalRead`] implementation for [`DiskCache`] — the public read
//! surface. The heavy lifting lives elsewhere: first-use init in [`super::init`],
//! growth handling in [`super::reopen`].
use std::borrow::Cow;
use std::future::ready;
use std::ops::Range;
use std::path::Path;

use futures::FutureExt as _;

use super::DiskCache;
use crate::counter::hardware_counter::HardwareCounterCell;
use crate::ext::aligned_vec::ACow;
use crate::generic_consts::{AccessPattern, Sequential};
use crate::universal_io::cached_fs::FileInfo;
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
    /// Make sure every byte from `byte_start` to the end of the file is present on the local file
    fn populate_from(&self, byte_start: u64, hw_counter: &HardwareCounterCell) -> UioResult<()> {
        if crate::low_memory::low_memory_mode().skip_populate() {
            return Ok(());
        }

        // The mirror is sized to the remote file on init: no remote round-trip needed
        let len = self.len::<u8>(hw_counter)?;
        if len == 0 {
            return Ok(());
        }

        let one_byte_per_block = (byte_start..len)
            .step_by(BLOCK_SIZE)
            .map(|byte_offset| ((), ReadRange::one(byte_offset)));

        // Read one byte per block purely to fault each block into the local
        // cache; the bytes themselves are discarded.
        self.read_batch(
            one_byte_per_block,
            Sequential,
            |(), _bytes: &[u8]| UioResult::Ok(()),
            hw_counter,
        )?;

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

    fn live_reload(&mut self, hw_counter: &HardwareCounterCell) -> UioResult<()> {
        self.reopen_impl(hw_counter)
    }

    fn live_preload<F: FnOnce(&Path) -> Option<FileInfo>>(
        &self,
        get_file_info: F,
        hw_counter: &HardwareCounterCell,
    ) -> UioResult<impl Future<Output = ()> + Send + 'static> {
        let mut future = self.live_preload_impl(get_file_info, hw_counter)?;

        // Poll once so that actual async work begins right away
        let future = match (&mut future).now_or_never() {
            Some(()) => ready(()).left_future(),
            None => future.right_future(),
        };

        Ok(future)
    }

    fn read_bytes<P: AccessPattern>(
        &self,
        range: Range<u64>,
        _access_pattern: P,
        align: usize,
        hw_counter: &HardwareCounterCell,
    ) -> UioResult<ACow<'_>> {
        let mut pipeline = DiskCachePipeline::<R, ()>::new()?;
        pipeline.schedule::<P>((), self, range, align, hw_counter)?;
        let (_, bytes) = pipeline
            .wait(hw_counter)?
            .expect("there's exactly one read");
        Ok(bytes)
    }

    fn read_whole<T: Item>(&self, hw_counter: &HardwareCounterCell) -> UioResult<Cow<'_, [T]>> {
        self.prefill_if_uninit(hw_counter)?;
        let length = self.len::<T>(hw_counter)?;
        self.read(ReadRange::new(0, length), Sequential, hw_counter)
    }

    fn len<T>(&self, hw_counter: &HardwareCounterCell) -> UioResult<u64> {
        self.state(hw_counter)?.local.mmap().len::<T>(hw_counter)
    }

    fn populate(&self, hw_counter: &HardwareCounterCell) -> UioResult<()> {
        self.populate_from(0, hw_counter)
    }

    fn populate_auto() -> bool {
        false
    }

    fn clear_ram_cache(&self) -> UioResult<()> {
        // Only touch an already-live mirror; don't force initialization just to
        // clear a cache that may not exist yet.
        if self.is_ready() {
            // 🤖 A ready state makes no remote requests.
            self.state(&HardwareCounterCell::disposable())?
                .local
                .mmap()
                .clear_ram_cache()?;
        }
        Ok(())
    }

    fn kind() -> UniversalKind {
        UniversalKind::SimpleDiskCache
    }
}
