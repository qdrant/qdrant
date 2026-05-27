use std::marker::PhantomData;
use std::ops::Range;

use common::ext::aligned_vec::ACow;
use common::generic_consts::AccessPattern;
use common::universal_io::{BorrowedReadPipeline, Result, UserData};

use super::buffer::read_into_byte_buffer;
use super::inner::PipelineInner;
use crate::file::BlobFile;
use crate::read::AsyncRead;

/// `BorrowedReadPipeline` impl over a [`BlobFile`]. Lazy: no channel / map is
/// allocated until the first `schedule` call, so creating one is cheap even
/// if the caller ends up not issuing any reads.
pub struct BorrowedBlobPipeline<'file, A: AsyncRead, U> {
    inner: Option<PipelineInner<U>>,
    _phantom: PhantomData<&'file BlobFile<A>>,
}

impl<'file, A, U> BorrowedReadPipeline<'file, U> for BorrowedBlobPipeline<'file, A, U>
where
    A: AsyncRead,
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
        range: Range<u64>,
        align: usize,
    ) -> Result<()> {
        let inner = self.inner.get_or_insert_with(|| {
            let (tx, rx) = PipelineInner::<U>::default_channel();
            PipelineInner::new(tx, rx)
        });
        let future = read_into_byte_buffer::<A>(file, range, align);
        inner.schedule(&file.runtime, user_data, future)
    }

    fn wait(&mut self) -> Result<Option<(U, ACow<'file>)>> {
        let Some(inner) = self.inner.as_mut() else {
            return Ok(None);
        };
        Ok(inner.wait()?.map(|(u, v)| (u, ACow::Owned(v))))
    }
}
