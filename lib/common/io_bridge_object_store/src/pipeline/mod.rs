//! Read pipelines bridging the sync universal-IO pipeline traits to the async
//! object-store backends.
//!
//! [`BlobReadPipeline`] is [`ReadPipeline`] impl over a [`BlobFile`](crate::BlobFile).
//!
//! - [`buffer`]: the read-future builder that streams a backend read straight
//!   into a freshly allocated, aligned destination `AVec<u8>`.
//! - [`inner`]: the schedule/wait engine ([`PipelineInner`](inner::PipelineInner))
//!   backing the pipeline.

mod buffer;
mod inner;

use std::marker::PhantomData;
use std::ops::Range;

use common::ext::aligned_vec::ACow;
use common::generic_consts::AccessPattern;
use common::universal_io::{ReadPipeline, Result, UserData};

pub(crate) use self::buffer::{
    read_from_into_byte_buffer, read_into_byte_buffer, read_whole_into_byte_buffer,
};
use self::inner::PipelineInner;
use crate::file::BlobFile;
use crate::read::AsyncRead;

pub(crate) const BLOB_PIPELINE_CAPACITY: usize = 256;

/// [`ReadPipeline`] over a [`BlobFile`].
///
/// Reads always produce *owned* buffers (the async worker streams into a fresh `AVec`),
/// so the `'file` lifetime is purely a file-safety guard. Lazy: no channel / slab is
/// allocated until the first `schedule` call, so creating one is cheap even if caller
/// ends up not issuing any reads.
pub struct BlobReadPipeline<'file, A: AsyncRead, U> {
    inner: Option<PipelineInner<U>>,
    _phantom: PhantomData<&'file BlobFile<A>>,
}

impl<'file, A, U> ReadPipeline<'file, U> for BlobReadPipeline<'file, A, U>
where
    A: AsyncRead + Clone,
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

    fn schedule_whole(&mut self, user_data: U, file: &'file BlobFile<A>, from: u64) -> Result<()> {
        // One open-ended GET from `from` to EOF, byte-aligned, sized from the
        // response — no separate `len`/HEAD round-trip. `from == 0` reads the
        // whole object; an offset at or past EOF resolves to an empty read
        // inside the future (see `read_from_into_byte_buffer`).

        let inner = self.inner.get_or_insert_with(|| {
            let (tx, rx) = PipelineInner::<U>::default_channel();
            PipelineInner::new(tx, rx)
        });

        let future = read_from_into_byte_buffer::<A>(file, from, 1);
        inner.schedule(&file.runtime, user_data, future)
    }

    fn wait(&mut self) -> Result<Option<(U, ACow<'file>)>> {
        let Some(inner) = self.inner.as_mut() else {
            return Ok(None);
        };
        Ok(inner.wait()?.map(|(u, v)| (u, ACow::Owned(v))))
    }
}
