use std::ops::Range;

use common::ext::aligned_vec::ACow;
use common::generic_consts::{AccessPattern, Sequential};
use common::universal_io::{OwnedReadPipeline, Result, UniversalRead, UserData};

use super::buffer::{read_into_byte_buffer, read_whole_into_byte_buffer};
use super::inner::PipelineInner;
use crate::file::BlobFile;
use crate::read::AsyncRead;

/// `OwnedReadPipeline` impl that takes ownership of a [`BlobFile`] and routes
/// every `schedule` call through the file's [`BridgeRuntime`](crate::BridgeRuntime).
/// Allocates its channel up-front in `new`, unlike [`BorrowedBlobPipeline`]
/// which is lazy.
///
/// [`BorrowedBlobPipeline`]: super::BorrowedBlobPipeline
pub struct OwnedBlobPipeline<A: AsyncRead, U> {
    file: BlobFile<A>,
    inner: PipelineInner<U>,
}

impl<A, U> OwnedReadPipeline<U> for OwnedBlobPipeline<A, U>
where
    A: AsyncRead + Clone,
    U: UserData,
{
    type File = BlobFile<A>;

    fn new(file: BlobFile<A>) -> Result<Self> {
        let (tx, rx) = PipelineInner::<U>::default_channel();
        Ok(Self {
            file,
            inner: PipelineInner::new(tx, rx),
        })
    }

    fn can_schedule(&mut self) -> bool {
        self.inner.can_schedule()
    }

    fn schedule<P: AccessPattern>(
        &mut self,
        user_data: U,
        range: Range<u64>,
        align: usize,
    ) -> Result<()> {
        let future = read_into_byte_buffer::<A>(&self.file, range, align);
        self.inner.schedule(&self.file.runtime, user_data, future)
    }

    fn schedule_whole(&mut self, user_data: U, from: u64) -> Result<()> {
        if from == 0 {
            let future = read_whole_into_byte_buffer::<A>(&self.file, 1);
            return self.inner.schedule(&self.file.runtime, user_data, future);
        }
        // A tail read from a known offset still needs the current length.
        let eof = self.file.len::<u8>()?;
        if from >= eof {
            return Ok(());
        }
        self.schedule::<Sequential>(user_data, from..eof, 1)
    }

    fn wait(&mut self) -> Result<Option<(U, ACow<'_>)>> {
        Ok(self.inner.wait()?.map(|(u, v)| (u, ACow::Owned(v))))
    }

    fn into_inner(self) -> BlobFile<A> {
        self.file
    }
}
