use std::borrow::Cow;
use std::future::Future;
use std::marker::PhantomData;

use ahash::AHashMap;
use bytes::Bytes;
use common::generic_consts::AccessPattern;
use common::universal_io::{
    BorrowedReadPipeline, OwnedReadPipeline, ReadRange, Result, UniversalIoError, UserData,
};
use tokio::sync::mpsc;

use crate::file::BlobFile;
use crate::read::AsyncRead;
use crate::runtime::{BridgeRequest, BridgeResponse, BridgeRuntime};

pub(crate) const BLOB_PIPELINE_CAPACITY: usize = 256;

/// Pipeline-side bookkeeping.
///
/// Owns only the reply channel and per-request slot tracking. The
/// [`BridgeRuntime`] is intentionally not stored here — callers supply it on
/// every `schedule` call. This keeps the pipeline cheap (one channel + a map)
/// and lets the same pipeline collect replies from multiple runtimes if
/// desired.
pub(crate) struct PipelineInner<U> {
    /// Sender for the reply channel. Cloned into every outgoing
    /// [`BridgeRequest`] as the worker's return address.
    tx: mpsc::Sender<BridgeResponse>,
    /// Receiver for the reply channel. `wait` blocks on this.
    rx: mpsc::Receiver<BridgeResponse>,
    /// `slot -> user_data` map. Keyed by the slot assigned at `schedule` time
    /// so out-of-order completion still reunites bytes with the right caller
    /// context. `AHashMap` is used over `std::collections::HashMap` for its
    /// faster hashing on small integer keys.
    pub(crate) pending: AHashMap<u64, U>,
    /// Monotonic counter that assigns a fresh slot id to every scheduled
    /// request. Wraps around on overflow — collisions only occur if more than
    /// `u64::MAX` requests are simultaneously pending, which is structurally
    /// impossible given the channel capacity.
    next_slot: u64,
}

impl<U> PipelineInner<U>
where
    U: UserData,
{
    /// Build a pipeline over an existing reply channel pair. The caller owns
    /// channel construction so that, if needed, the same channel can be shared
    /// across pipelines or sized independently of [`BLOB_PIPELINE_CAPACITY`].
    /// Use [`PipelineInner::default_channel`] to get the standard pair.
    pub(crate) fn new(
        tx: mpsc::Sender<BridgeResponse>,
        rx: mpsc::Receiver<BridgeResponse>,
    ) -> Self {
        Self {
            tx,
            rx,
            pending: AHashMap::new(),
            next_slot: 0,
        }
    }

    /// Standard `(tx, rx)` pair sized to [`BLOB_PIPELINE_CAPACITY`]. Most
    /// pipelines should use this and feed it directly into [`Self::new`].
    pub(crate) fn default_channel() -> (mpsc::Sender<BridgeResponse>, mpsc::Receiver<BridgeResponse>)
    {
        mpsc::channel(BLOB_PIPELINE_CAPACITY)
    }

    pub(crate) fn can_schedule(&self) -> bool {
        self.pending.len() < BLOB_PIPELINE_CAPACITY
    }

    /// Enqueue an async byte-producing operation on `runtime` and tag the
    /// result with `user_data` so the eventual reply can be paired back with
    /// its caller context.
    ///
    /// # Parameters
    /// - `runtime`: the [`BridgeRuntime`] that will drive the future. The
    ///   pipeline is runtime-agnostic, so different `schedule` calls on the
    ///   same pipeline can target different runtimes.
    /// - `user_data`: opaque caller context (e.g. a request id, point id,
    ///   destination buffer key) stored in `pending` under the freshly
    ///   assigned slot. Returned alongside the bytes from [`Self::wait`].
    /// - `future`: the async work to perform. Must resolve to `Result<Bytes>`,
    ///   be `Send + 'static`, and own all of its captured state — it will be
    ///   moved to the worker thread and `.await`-ed there. In practice this
    ///   is almost always the future returned by [`AsyncRead::read_range`],
    ///   but any byte-producing future is accepted.
    ///
    /// The future is boxed at the channel boundary (see
    /// [`BridgeRequest::future`]); callers pass an unboxed `impl Future` and
    /// the pipeline handles the type-erasure.
    ///
    /// # Errors
    /// - [`UniversalIoError::QueueIsFull`] if the pipeline already has
    ///   [`BLOB_PIPELINE_CAPACITY`] pending requests.
    /// - [`UniversalIoError::S3RuntimeShutDown`] if the runtime's request
    ///   channel has been closed.
    ///
    /// # Example
    /// ```ignore
    /// use bytes::Bytes;
    /// use io_bridge_object_store::{BridgeRuntime, pipeline::PipelineInner};
    ///
    /// let runtime = BridgeRuntime::global();
    /// let (tx, rx) = PipelineInner::<u32>::default_channel();
    /// let mut pipeline: PipelineInner<u32> = PipelineInner::new(tx, rx);
    ///
    /// // Schedule a trivial future tagged with user_data = 42.
    /// pipeline.schedule(&runtime, 42, async {
    ///     Ok(Bytes::from_static(b"hello"))
    /// })?;
    ///
    /// // Real usage: hand off the future produced by an AsyncRead backend.
    /// // let fut = source.read_range(0..1024);
    /// // pipeline.schedule(&runtime, point_id, fut)?;
    ///
    /// let (user_data, bytes) = pipeline.wait::<u8>()?.unwrap();
    /// assert_eq!(user_data, 42);
    /// assert_eq!(&bytes[..], b"hello");
    /// ```
    pub(crate) fn schedule<F>(
        &mut self,
        runtime: &BridgeRuntime,
        user_data: U,
        future: F,
    ) -> Result<()>
    where
        F: Future<Output = Result<Bytes>> + Send + 'static,
    {
        if !self.can_schedule() {
            return Err(UniversalIoError::QueueIsFull);
        }
        let slot = self.next_slot;
        self.next_slot = self.next_slot.wrapping_add(1);
        self.pending.insert(slot, user_data);

        let req = BridgeRequest::new(future, self.tx.clone(), slot);
        runtime
            .tx()
            .try_send(req)
            .map_err(|_| UniversalIoError::S3RuntimeShutDown)
    }

    pub(crate) fn wait<T: bytemuck::Pod>(&mut self) -> Result<Option<(U, Vec<T>)>> {
        if self.pending.is_empty() {
            return Ok(None);
        }
        let response = self
            .rx
            .blocking_recv()
            .expect("tx held by self; cannot disconnect");
        let user_data = self
            .pending
            .remove(&response.slot)
            .expect("response slot must be in pending");
        let bytes = response.bytes?;
        let items = bytemuck::pod_collect_to_vec(&bytes);
        Ok(Some((user_data, items)))
    }
}

/// `BorrowedReadPipeline` impl over a [`BlobFile`]. Lazy: no channel / map is
/// allocated until the first `schedule` call, so creating one is cheap even
/// if the caller ends up not issuing any reads.
pub struct BorrowedBlobPipeline<'file, A: AsyncRead, T, U> {
    inner: Option<PipelineInner<U>>,
    _phantom: PhantomData<(&'file BlobFile<A>, T)>,
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
            let (tx, rx) = PipelineInner::<U>::default_channel();
            PipelineInner::new(tx, rx)
        });
        let item_size = size_of::<T>() as u64;
        let start = range.byte_offset;
        let end = start + range.length * item_size;
        let total = end - start;
        let stream = file.inner.read_range(&file.path, start..end);
        let future = async move {
            let stream = stream.await?;
            object_store::collect_bytes(stream, Some(total)).await
        };
        inner.schedule(&file.runtime, user_data, future)
    }

    fn wait(&mut self) -> Result<Option<(U, Cow<'file, [T]>)>> {
        let Some(inner) = self.inner.as_mut() else {
            return Ok(None);
        };
        Ok(inner.wait::<T>()?.map(|(u, v)| (u, Cow::Owned(v))))
    }
}

/// `OwnedReadPipeline` impl that takes ownership of a [`BlobFile`] and routes
/// every `schedule` call through the file's [`BridgeRuntime`]. Allocates its
/// channel up-front in `new`, unlike [`BorrowedBlobPipeline`] which is lazy.
pub struct OwnedBlobPipeline<A: AsyncRead, T, U> {
    file: BlobFile<A>,
    inner: PipelineInner<U>,
    _phantom: PhantomData<T>,
}

impl<A, T, U> OwnedReadPipeline<T, U> for OwnedBlobPipeline<A, T, U>
where
    A: AsyncRead,
    T: bytemuck::Pod,
    U: UserData,
{
    type File = BlobFile<A>;

    fn new(file: BlobFile<A>) -> Result<Self> {
        let (tx, rx) = PipelineInner::<U>::default_channel();
        Ok(Self {
            file,
            inner: PipelineInner::new(tx, rx),
            _phantom: PhantomData,
        })
    }

    fn can_schedule(&mut self) -> bool {
        self.inner.can_schedule()
    }

    fn schedule<P: AccessPattern>(&mut self, user_data: U, range: ReadRange) -> Result<()> {
        let item_size = size_of::<T>() as u64;
        let start = range.byte_offset;
        let end = start + range.length * item_size;
        let total = end - start;
        let stream_fut = self.file.inner.read_range(&self.file.path, start..end);
        let future = async move {
            let stream = stream_fut.await?;
            object_store::collect_bytes(stream, Some(total)).await
        };
        self.inner.schedule(&self.file.runtime, user_data, future)
    }

    fn wait(&mut self) -> Result<Option<(U, Cow<'_, [T]>)>> {
        Ok(self.inner.wait::<T>()?.map(|(u, v)| (u, Cow::Owned(v))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pipeline_can_schedule_starts_true_and_blocks_when_full() {
        let (tx, rx) = PipelineInner::<u32>::default_channel();
        let mut inner: PipelineInner<u32> = PipelineInner::new(tx, rx);
        assert!(inner.can_schedule());
        for i in 0..BLOB_PIPELINE_CAPACITY as u64 {
            inner.pending.insert(i, i as u32);
        }
        assert!(!inner.can_schedule());
    }

    #[test]
    fn pipeline_schedule_returns_queue_full_when_capacity_reached() {
        let runtime = BridgeRuntime::global();
        let (tx, rx) = PipelineInner::<u32>::default_channel();
        let mut inner: PipelineInner<u32> = PipelineInner::new(tx, rx);
        for i in 0..BLOB_PIPELINE_CAPACITY as u64 {
            inner.pending.insert(i, i as u32);
        }
        let err = inner
            .schedule(&runtime, 999, async { Ok(Bytes::from_static(b"x")) })
            .unwrap_err();
        assert!(matches!(err, UniversalIoError::QueueIsFull));
    }

    #[test]
    fn pipeline_schedule_and_wait_round_trip() {
        let runtime = BridgeRuntime::global();
        let (tx, rx) = PipelineInner::<u32>::default_channel();
        let mut inner: PipelineInner<u32> = PipelineInner::new(tx, rx);
        inner
            .schedule(&runtime, 111, async { Ok(Bytes::from_static(b"hello")) })
            .expect("schedule");
        let (user, bytes) = inner.wait::<u8>().expect("wait ok").expect("some");
        assert_eq!(user, 111);
        assert_eq!(&bytes[..], b"hello");
    }

    #[test]
    fn pipeline_out_of_order_completion_preserves_user_data() {
        let runtime = BridgeRuntime::global();
        let (tx, rx) = PipelineInner::<u32>::default_channel();
        let mut inner: PipelineInner<u32> = PipelineInner::new(tx, rx);
        for (i, bytes) in [b"aaaa".as_slice(), b"bb".as_slice(), b"cccccc".as_slice()]
            .iter()
            .enumerate()
        {
            let bytes = *bytes;
            inner
                .schedule(
                    &runtime,
                    i as u32,
                    async move { Ok(Bytes::from_static(bytes)) },
                )
                .unwrap();
        }
        let mut seen = std::collections::HashSet::new();
        for _ in 0..3 {
            let (user, _bytes) = inner.wait::<u8>().unwrap().unwrap();
            assert!(seen.insert(user), "user_data {user} seen twice");
        }
        assert_eq!(seen, [0u32, 1, 2].into_iter().collect());
        assert!(inner.wait::<u8>().unwrap().is_none());
    }

    /// Two distinct runtimes feed reply responses into one pipeline; both
    /// runtimes execute work in parallel and the pipeline accepts both replies.
    #[test]
    fn pipeline_collects_replies_from_multiple_runtimes() {
        let rt_a = BridgeRuntime::new().expect("rt_a");
        let rt_b = BridgeRuntime::new().expect("rt_b");
        let (tx, rx) = PipelineInner::<u32>::default_channel();
        let mut inner: PipelineInner<u32> = PipelineInner::new(tx, rx);
        inner
            .schedule(&rt_a, 1, async { Ok(Bytes::from_static(b"AAAA")) })
            .unwrap();
        inner
            .schedule(&rt_b, 2, async { Ok(Bytes::from_static(b"BB")) })
            .unwrap();
        let mut seen: AHashMap<u32, Vec<u8>> = AHashMap::new();
        for _ in 0..2 {
            let (user, bytes) = inner.wait::<u8>().unwrap().unwrap();
            seen.insert(user, bytes);
        }
        assert_eq!(seen[&1], b"AAAA");
        assert_eq!(seen[&2], b"BB");
    }
}
