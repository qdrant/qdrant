//! Read pipeline for [`CachedBlobFile`]: a thin wrapper delegating to the
//! disk-cache pipeline, which serves cached blocks locally and fetches
//! missing ones from the remote concurrently.

use std::ops::Range;

use common::counter::hardware_counter::HardwareCounterCell;
use common::ext::aligned_vec::ACow;
use common::generic_consts::AccessPattern;
use common::universal_io::{DiskCache, ReadPipeline, UioResult, UniversalRead, UserData};

use super::CachedBlobFile;
use crate::file::BlobFile;
use crate::read::AsyncRead;

type Inner<'file, A, U> = <DiskCache<BlobFile<A>> as UniversalRead>::ReadPipeline<'file, U>;

pub struct CachedBlobReadPipeline<'file, A: AsyncRead + Clone, U: UserData> {
    inner: Inner<'file, A, U>,
}

impl<'file, A: AsyncRead + Clone, U: UserData> ReadPipeline<'file, U>
    for CachedBlobReadPipeline<'file, A, U>
{
    type File = CachedBlobFile<A>;

    fn new() -> UioResult<Self> {
        Ok(Self {
            inner: <Inner<'file, A, U> as ReadPipeline<'file, U>>::new()?,
        })
    }

    fn can_schedule(&mut self) -> bool {
        self.inner.can_schedule()
    }

    fn schedule<P: AccessPattern>(
        &mut self,
        user_data: U,
        file: &'file Self::File,
        range: Range<u64>,
        align: usize,
        hw_counter: &HardwareCounterCell,
    ) -> UioResult<()> {
        self.inner
            .schedule::<P>(user_data, &file.cache, range, align, hw_counter)
    }

    fn schedule_whole(
        &mut self,
        user_data: U,
        file: &'file Self::File,
        from: u64,
        hw_counter: &HardwareCounterCell,
    ) -> UioResult<()> {
        self.inner
            .schedule_whole(user_data, &file.cache, from, hw_counter)
    }

    fn wait(&mut self, hw_counter: &HardwareCounterCell) -> UioResult<Option<(U, ACow<'file>)>> {
        self.inner.wait(hw_counter)
    }
}
