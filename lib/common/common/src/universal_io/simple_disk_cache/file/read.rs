//! The [`UniversalRead`] implementation for [`DiskCache`] — the public read
//! surface. The heavy lifting lives elsewhere: first-use init in [`super::init`],
//! growth handling in [`super::reopen`].
use std::borrow::Cow;
use std::ops::Range;
use std::path::Path;

use super::DiskCache;
use crate::ext::aligned_vec::ACow;
use crate::generic_consts::{AccessPattern, Random, Sequential};
use crate::universal_io::cached_fs::FileInfo;
use crate::universal_io::simple_disk_cache::fs::DiskCacheFs;
use crate::universal_io::simple_disk_cache::pipeline::{
    DiskCachePipeline, REMOTE_READ_ALIGNMENT, Source, pick_source, read_local,
};
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
        self.read_batch(one_byte_per_block, Sequential, |(), _bytes: &[u8]| {
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

    fn live_reload(&mut self) -> UioResult<()> {
        self.reopen_impl()
    }

    fn live_preload<F: FnOnce(&Path) -> Option<FileInfo>>(
        &self,
        get_file_info: F,
    ) -> UioResult<()> {
        self.live_preload_impl(get_file_info)
    }

    fn read_bytes<P: AccessPattern>(
        &self,
        range: Range<u64>,
        _access_pattern: P,
        align: usize,
    ) -> UioResult<ACow<'_>> {
        let mut pipeline = DiskCachePipeline::<R, ()>::new()?;
        pipeline.schedule::<P>((), self, range, align)?;
        let (_, bytes) = pipeline.wait()?.expect("there's exactly one read");
        Ok(bytes)
    }

    async fn read_bytes_async<P: AccessPattern>(
        &self,
        range: Range<u64>,
        access_pattern: P,
        _align: usize,
    ) -> UioResult<ACow<'_>> {
        // warn: first-touch init (`state()`) still does blocking I/O; only the
        // block fetch itself is async.
        //
        // TODO(uio): This is a targeted use of async, but maybe later we'd want
        // a proper async pipeline
        let state = self.state()?;
        match pick_source::<P>(state.local, range.clone())? {
            Source::Local {
                range,
                is_sequential,
            } => {
                // SAFETY: Source::Local confirms the range is local (or empty).
                let bytes = unsafe { read_local::<R>(self, range, is_sequential)? };
                Ok(ACow::Borrowed(bytes))
            }
            Source::Remote {
                blocks_range,
                blocks_byte_range,
            } => {
                let bytes = state
                    .remote
                    .read_bytes_async(blocks_byte_range, access_pattern, REMOTE_READ_ALIGNMENT)
                    .await?;
                // SAFETY: `bytes` is the remote content of `blocks_range`
                // (clamped to EOF), which covers `range`.
                unsafe {
                    state.local.write_mmap_bytes(&bytes, blocks_range);
                    Ok(ACow::Borrowed(
                        state.local.read_mmap_bytes::<Random>(range)?,
                    ))
                }
            }
        }
    }

    fn read_whole<T: Item>(&self) -> UioResult<Cow<'_, [T]>> {
        self.prefill_if_uninit()?;
        let length = self.len::<T>()?;
        self.read(ReadRange::new(0, length), Sequential)
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
