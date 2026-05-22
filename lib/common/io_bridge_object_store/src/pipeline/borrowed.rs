use std::borrow::Cow;
use std::marker::PhantomData;

use common::generic_consts::AccessPattern;
use common::universal_io::{BorrowedReadPipeline, ReadRange, Result, UserData};

use super::buffer::read_into_buffer;
use super::inner::PipelineInner;
use crate::file::BlobFile;
use crate::read::AsyncRead;

/// `BorrowedReadPipeline` impl over a [`BlobFile`]. Lazy: no channel / map is
/// allocated until the first `schedule` call, so creating one is cheap even
/// if the caller ends up not issuing any reads.
pub struct BorrowedBlobPipeline<'file, A: AsyncRead, T, U> {
    inner: Option<PipelineInner<T, U>>,
    _phantom: PhantomData<&'file BlobFile<A>>,
}

impl<'file, A, T, U> BorrowedReadPipeline<'file, T, U> for BorrowedBlobPipeline<'file, A, T, U>
where
    A: AsyncRead,
    T: bytemuck::Pod,
    U: UserData,
{
    type File = BlobFile<A>;

    fn new() -> Result<Self> {
        Ok(Self {
            inner: None,
            _phantom: PhantomData,
        })
    }

    fn can_schedule(&mut self) -> bool {
        self.inner.as_ref().is_none_or(|i| i.can_schedule())
    }

    fn schedule<P: AccessPattern>(
        &mut self,
        user_data: U,
        file: &'file BlobFile<A>,
        range: ReadRange,
    ) -> Result<()> {
        let inner = self.inner.get_or_insert_with(|| {
            let (tx, rx) = PipelineInner::<T, U>::default_channel();
            PipelineInner::new(tx, rx)
        });
        let future = read_into_buffer::<A, T>(file, range);
        inner.schedule(&file.runtime, user_data, future)
    }

    fn wait(&mut self) -> Result<Option<(U, Cow<'file, [T]>)>> {
        let Some(inner) = self.inner.as_mut() else {
            return Ok(None);
        };
        Ok(inner.wait()?.map(|(u, v)| (u, Cow::Owned(v))))
    }
}
