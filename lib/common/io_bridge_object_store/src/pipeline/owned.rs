use std::borrow::Cow;

use common::generic_consts::AccessPattern;
use common::universal_io::{OwnedReadPipeline, ReadRange, Result, UserData};

use super::buffer::read_into_buffer;
use super::inner::PipelineInner;
use crate::file::BlobFile;
use crate::read::AsyncRead;

/// `OwnedReadPipeline` impl that takes ownership of a [`BlobFile`] and routes
/// every `schedule` call through the file's [`BridgeRuntime`](crate::BridgeRuntime).
/// Allocates its channel up-front in `new`, unlike [`BorrowedBlobPipeline`]
/// which is lazy.
///
/// [`BorrowedBlobPipeline`]: super::BorrowedBlobPipeline
pub struct OwnedBlobPipeline<A: AsyncRead, T, U> {
    file: BlobFile<A>,
    inner: PipelineInner<T, U>,
}

impl<A, T, U> OwnedReadPipeline<T, U> for OwnedBlobPipeline<A, T, U>
where
    A: AsyncRead,
    T: bytemuck::Pod,
    U: UserData,
{
    type File = BlobFile<A>;

    fn new(file: BlobFile<A>) -> Result<Self> {
        let (tx, rx) = PipelineInner::<T, U>::default_channel();
        Ok(Self {
            file,
            inner: PipelineInner::new(tx, rx),
        })
    }

    fn can_schedule(&mut self) -> bool {
        self.inner.can_schedule()
    }

    fn schedule<P: AccessPattern>(&mut self, user_data: U, range: ReadRange) -> Result<()> {
        let future = read_into_buffer::<A, T>(&self.file, range);
        self.inner.schedule(&self.file.runtime, user_data, future)
    }

    fn wait(&mut self) -> Result<Option<(U, Cow<'_, [T]>)>> {
        Ok(self.inner.wait()?.map(|(u, v)| (u, Cow::Owned(v))))
    }
}
