use std::borrow::Cow;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use common::generic_consts::AccessPattern;
use common::universal_io::{ReadRange, Result, UniversalIoError, UniversalReadPipeline, UserData};

use crate::backend::AsyncReadBackend;
use crate::dispatcher::{AsyncDispatcher, DispatchResult, ReadRequest};

/// Default in-flight cap — effectively unbounded. The bridge does not impose
/// an artificial scheduler limit; callers cap explicitly via
/// [`IoBridgeReadPipeline::with_max_concurrency`] when they want one
/// (e.g. tests that exercise the cap, or backends with a known parallelism
/// budget like S3 connection limits).
pub(crate) const DEFAULT_MAX_CONCURRENCY: usize = usize::MAX;

/// File-side glue that an async-backed [`UniversalRead`] impl provides so the
/// IoBridge pipeline can find its dispatcher and the per-file `Location` for
/// each scheduled read.
pub trait IoBridgeFile {
    type Backend: AsyncReadBackend;
    fn dispatcher(&self) -> &Arc<AsyncDispatcher<Self::Backend>>;
    fn location(&self) -> <Self::Backend as AsyncReadBackend>::Location;
}

/// Per-batch pipeline that turns sync `schedule` / `wait` calls into async
/// backend reads via the [`AsyncDispatcher`] exposed by each scheduled file.
/// `new()` keeps its no-arg signature, while each `schedule()` uses that file's
/// dispatcher so one batch can span multiple backend clients.
pub struct IoBridgeReadPipeline<'file, F, T, U>
where
    F: IoBridgeFile,
    T: bytemuck::Pod,
    U: UserData,
{
    result_tx: tokio::sync::mpsc::UnboundedSender<DispatchResult>,
    result_rx: tokio::sync::mpsc::UnboundedReceiver<DispatchResult>,
    in_flight: usize,
    max_concurrency: usize,
    user_data: HashMap<u64, U>,
    next_tag: u64,
    _phantom: PhantomData<(&'file F, T)>,
}

impl<'file, F, T, U> IoBridgeReadPipeline<'file, F, T, U>
where
    F: IoBridgeFile,
    T: bytemuck::Pod,
    U: UserData,
{
    /// Build a pipeline with an explicit in-flight cap. The trait-required
    /// no-arg [`UniversalReadPipeline::new`] uses [`DEFAULT_MAX_CONCURRENCY`].
    pub fn with_max_concurrency(max_concurrency: usize) -> Result<Self> {
        let (result_tx, result_rx) = tokio::sync::mpsc::unbounded_channel();
        Ok(Self {
            result_tx,
            result_rx,
            in_flight: 0,
            max_concurrency,
            user_data: HashMap::new(),
            next_tag: 0,
            _phantom: PhantomData,
        })
    }
}

impl<'file, F, T, U> UniversalReadPipeline<'file, T, U> for IoBridgeReadPipeline<'file, F, T, U>
where
    F: IoBridgeFile + 'file,
    T: bytemuck::Pod,
    U: UserData,
{
    type File = F;

    fn new() -> Result<Self> {
        Self::with_max_concurrency(DEFAULT_MAX_CONCURRENCY)
    }

    fn can_schedule(&mut self) -> bool {
        self.in_flight < self.max_concurrency
    }

    fn schedule<P>(&mut self, user_data: U, file: &'file Self::File, range: ReadRange) -> Result<()>
    where
        P: AccessPattern,
    {
        if !self.can_schedule() {
            return Err(UniversalIoError::QueueIsFull);
        }
        let dispatcher = Arc::clone(file.dispatcher());
        let tag = self.next_tag;
        self.next_tag += 1;
        self.user_data.insert(tag, user_data);
        let byte_length = range.length.saturating_mul(size_of::<T>() as u64);
        dispatcher.submit(ReadRequest {
            location: file.location(),
            byte_offset: range.byte_offset,
            byte_length,
            user_data_tag: tag,
            result_tx: self.result_tx.clone(),
        })?;
        self.in_flight += 1;
        Ok(())
    }

    fn wait(&mut self) -> Result<Option<(U, Cow<'file, [T]>)>> {
        if self.in_flight == 0 {
            return Ok(None);
        }
        let res = self.result_rx.blocking_recv().ok_or_else(|| {
            UniversalIoError::uninitialized("dispatcher closed before result received")
        })?;
        self.in_flight -= 1;
        let user = self
            .user_data
            .remove(&res.user_data_tag)
            .expect("user_data tag tracked by pipeline");
        let bytes = res.bytes?;
        Ok(Some((user, Cow::Owned(bytes_to_typed_vec::<T>(&bytes)))))
    }
}

/// Copy `bytes` into a fresh `Vec<T>`. Allocating the vec first guarantees
/// `T`-alignment, sidestepping the bytemuck slice-cast alignment requirement
/// when the backend returns a buffer that happens to be byte-aligned only.
fn bytes_to_typed_vec<T: bytemuck::Pod>(bytes: &[u8]) -> Vec<T> {
    let elem_count = bytes.len() / size_of::<T>();
    let mut out = vec![T::zeroed(); elem_count];
    let out_bytes = bytemuck::cast_slice_mut::<T, u8>(&mut out);
    out_bytes.copy_from_slice(&bytes[..out_bytes.len()]);
    out
}
