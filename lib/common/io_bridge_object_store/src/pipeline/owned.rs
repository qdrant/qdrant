use std::ops::Range;

use common::ext::aligned_vec::ACow;
use common::generic_consts::AccessPattern;
use common::universal_io::{OwnedReadPipeline, Result, UserData};

use super::buffer::{read_from_into_byte_buffer, read_into_byte_buffer};
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
        // One open-ended GET from `from` to EOF, byte-aligned, sized from the
        // response — no separate `len`/HEAD round-trip. `from == 0` reads the
        // whole object; an offset at or past EOF resolves to an empty read
        // inside the future (see `read_from_into_byte_buffer`).
        let future = read_from_into_byte_buffer::<A>(&self.file, from, 1);
        self.inner.schedule(&self.file.runtime, user_data, future)
    }

    fn wait(&mut self) -> Result<Option<(U, ACow<'_>)>> {
        Ok(self.inner.wait()?.map(|(u, v)| (u, ACow::Owned(v))))
    }

    fn into_inner(self) -> BlobFile<A> {
        self.file
    }
}
