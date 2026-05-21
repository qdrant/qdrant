use std::borrow::Cow;
use std::future::Future;
use std::marker::PhantomData;

use ahash::AHashMap;
use common::generic_consts::AccessPattern;
use common::universal_io::{
    BorrowedReadPipeline, OwnedReadPipeline, ReadRange, Result, UniversalIoError, UserData,
};
use futures::StreamExt;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;

use crate::file::BlobFile;
use crate::read::AsyncRead;
use crate::runtime::{BridgeRequest, BridgeResponse, BridgeRuntime};
use crate::writer::AlignedBufWriter;

pub(crate) const BLOB_PIPELINE_CAPACITY: usize = 256;

/// Send-able raw byte pointer used to hand the future a writable view into a
/// `Vec<T>` that is owned by the pipeline's `pending` map.
///
/// # Why this exists
///
/// The runtime worker thread runs the future and writes the response bytes;
/// the originating pipeline lives on a different thread. We want the
/// destination buffer (`Vec<T>`) to be allocated once and written into once —
/// no `Bytes` aggregation, no `pod_collect_to_vec` re-copy. To do that the
/// future needs a `&mut [u8]` view of the buffer, but a normal mutable
/// reference cannot cross threads through a boxed `dyn Future`. This struct
/// is a `Send` wrapper around the raw pointer + length pair.
///
/// # Safety invariant
///
/// The `Vec<T>` from which the `SendBytePtr` is derived must:
/// 1. Remain alive and at its original heap allocation address until the
///    future referencing this pointer has completed (success or error).
/// 2. Not be reallocated (no `push`, `reserve`, `shrink_to_fit`, etc.).
/// 3. Not be observed by the main thread (no `&` or `&mut` access) while the
///    future may be using the pointer.
///
/// `PipelineInner::schedule` enforces this by moving the `Vec<T>` into
/// `pending` before dispatching the future and never touching it until
/// `wait()` removes it after the matching `BridgeResponse` arrives. The heap
/// allocation address is stable across `Vec` moves and `AHashMap` rehashes
/// (those only relocate the `(ptr, len, cap)` triplet, not the bytes).
pub(crate) struct SendBytePtr {
    ptr: *mut u8,
    len: usize,
}

// SAFETY: Exclusive access to the underlying bytes is guaranteed by the
// pipeline's ownership invariant — the future is the sole accessor for the
// lifetime of `SendBytePtr`.
unsafe impl Send for SendBytePtr {}

impl SendBytePtr {
    /// Derive a `SendBytePtr` from a `Vec<T>`'s heap allocation.
    ///
    /// # Safety
    /// The caller guarantees the `Vec<T>` will not be reallocated, freed, or
    /// otherwise accessed by `&`/`&mut` while any `&mut [u8]` view obtained
    /// through `as_slice_mut` is alive. In practice the pipeline upholds this
    /// by moving the `Vec<T>` into `pending` and not touching it until the
    /// future completes.
    pub(crate) unsafe fn from_vec<T: bytemuck::Pod>(buf: &mut Vec<T>) -> Self {
        Self {
            ptr: buf.as_mut_ptr().cast::<u8>(),
            len: buf.len() * size_of::<T>(),
        }
    }

    /// # Safety
    /// The caller (the future) must hold exclusive access to the underlying
    /// buffer per the invariant documented on [`SendBytePtr`]. Only one
    /// `&mut [u8]` view may exist at a time.
    pub(crate) unsafe fn as_slice_mut(&mut self) -> &mut [u8] {
        // SAFETY: ptr/len valid per the invariant; exclusivity is the
        // caller's contract.
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

/// Pipeline-side bookkeeping.
///
/// Owns the reply channel, per-request slot tracking, and — critically — the
/// destination `Vec<T>` for every in-flight request. The future writes its
/// bytes directly into the `Vec<T>` via a `SendBytePtr`; the channel reply
/// only carries the completion status, so neither the runtime nor the channel
/// touch the typed payload.
///
/// The [`BridgeRuntime`] is intentionally not stored here — callers supply it
/// on every `schedule` call. This keeps the pipeline cheap (one channel +
/// a map) and lets the same pipeline collect replies from multiple runtimes
/// if desired.
pub(crate) struct PipelineInner<T, U> {
    /// Sender for the reply channel. Cloned into every outgoing
    /// [`BridgeRequest`] as the worker's return address.
    tx: mpsc::Sender<BridgeResponse>,
    /// Receiver for the reply channel. `wait` blocks on this.
    rx: mpsc::Receiver<BridgeResponse>,
    /// `slot -> (user_data, destination buffer)` map. The `Vec<T>` here is the
    /// buffer the future writes into via its captured [`SendBytePtr`]. Keyed
    /// by the slot assigned at `schedule` time so out-of-order completion
    /// still reunites the buffer with the right caller context. `AHashMap` is
    /// used over `std::collections::HashMap` for its faster hashing on small
    /// integer keys.
    pub(crate) pending: AHashMap<u64, (U, Vec<T>)>,
    /// Monotonic counter that assigns a fresh slot id to every scheduled
    /// request. Wraps around on overflow — collisions only occur if more than
    /// `u64::MAX` requests are simultaneously pending, which is structurally
    /// impossible given the channel capacity.
    next_slot: u64,
}

impl<T, U> PipelineInner<T, U>
where
    T: bytemuck::Pod,
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

    /// Enqueue an async byte-producing operation on `runtime`, taking
    /// ownership of the destination `Vec<T>` and tagging the slot with
    /// `user_data` for out-of-order reassembly at `wait` time.
    ///
    /// The future is expected to write its bytes into the destination buffer
    /// via a [`SendBytePtr`] previously derived (by the caller) from the same
    /// `Vec<T>` and captured in the future's closure. The future's
    /// `Result<()>` only signals completion — no payload travels on the
    /// channel.
    ///
    /// # Parameters
    /// - `runtime`: the [`BridgeRuntime`] that will drive the future. The
    ///   pipeline is runtime-agnostic, so different `schedule` calls on the
    ///   same pipeline can target different runtimes.
    /// - `user_data`: opaque caller context (e.g. a request id, point id)
    ///   stored in `pending` under the freshly assigned slot. Returned
    ///   alongside the destination buffer from [`Self::wait`].
    /// - `buf`: pre-allocated destination of exact byte size. Moved into
    ///   `pending` before the future is dispatched; the heap allocation
    ///   address is stable across this move, so the [`SendBytePtr`] captured
    ///   in `future` remains valid.
    /// - `future`: the async work to perform. Must resolve to `Result<()>`,
    ///   be `Send + 'static`, and own all of its captured state — it will be
    ///   moved to the worker thread and `.await`-ed there.
    ///
    /// # Errors
    /// - [`UniversalIoError::QueueIsFull`] if the pipeline already has
    ///   [`BLOB_PIPELINE_CAPACITY`] pending requests. The `buf` is dropped.
    /// - [`UniversalIoError::S3RuntimeShutDown`] if the runtime's request
    ///   channel has been closed. The `buf` is dropped after being briefly
    ///   inserted-then-removed from `pending`.
    pub(crate) fn schedule<F>(
        &mut self,
        runtime: &BridgeRuntime,
        user_data: U,
        buf: Vec<T>,
        future: F,
    ) -> Result<()>
    where
        F: Future<Output = Result<(), UniversalIoError>> + Send + 'static,
    {
        if !self.can_schedule() {
            return Err(UniversalIoError::QueueIsFull);
        }
        let slot = self.next_slot;
        self.next_slot = self.next_slot.wrapping_add(1);
        self.pending.insert(slot, (user_data, buf));

        let req = BridgeRequest::new(future, self.tx.clone(), slot);
        if runtime.tx().try_send(req).is_err() {
            // Roll back so the slot/buf aren't orphaned if the runtime is
            // dead. The future is dropped together with the unsent
            // BridgeRequest; the captured SendBytePtr is never dereferenced.
            self.pending.remove(&slot);
            return Err(UniversalIoError::S3RuntimeShutDown);
        }
        Ok(())
    }

    pub(crate) fn wait(&mut self) -> Result<Option<(U, Vec<T>)>> {
        if self.pending.is_empty() {
            return Ok(None);
        }
        let response = self
            .rx
            .blocking_recv()
            .expect("tx held by self; cannot disconnect");
        let (user_data, buf) = self
            .pending
            .remove(&response.slot)
            .expect("response slot must be in pending");
        response.result?;
        Ok(Some((user_data, buf)))
    }
}

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
        let item_size = size_of::<T>() as u64;
        let start = range.byte_offset;
        let end = start + range.length * item_size;

        let mut buf: Vec<T> = vec![T::zeroed(); range.length as usize];
        // SAFETY: `buf` is moved into `inner.pending` below before the future
        // runs; the heap allocation address survives the move. The pipeline
        // never reads or mutates the buffer until `wait` removes it after the
        // matching BridgeResponse arrives, at which point the future is no
        // longer running. The pipeline never reallocates `buf`.
        let mut dst = unsafe { SendBytePtr::from_vec(&mut buf) };

        let stream_fut = file.inner.read_range(&file.path, start..end);
        let future = async move {
            let mut stream = stream_fut.await?;
            // SAFETY: this future is the sole accessor of the buffer for its
            // lifetime per the SendBytePtr invariant.
            let slice = unsafe { dst.as_slice_mut() };
            let mut writer = AlignedBufWriter::from_raw_bytes(slice);
            while let Some(chunk) = stream.next().await {
                writer
                    .write_all(&chunk?)
                    .await
                    .map_err(UniversalIoError::s3)?;
            }
            if writer.written() != writer.capacity() {
                return Err(UniversalIoError::S3Config {
                    description: format!(
                        "short read: expected {} bytes, got {}",
                        writer.capacity(),
                        writer.written(),
                    ),
                });
            }
            Ok(())
        };
        inner.schedule(&file.runtime, user_data, buf, future)
    }

    fn wait(&mut self) -> Result<Option<(U, Cow<'file, [T]>)>> {
        let Some(inner) = self.inner.as_mut() else {
            return Ok(None);
        };
        Ok(inner.wait()?.map(|(u, v)| (u, Cow::Owned(v))))
    }
}

/// `OwnedReadPipeline` impl that takes ownership of a [`BlobFile`] and routes
/// every `schedule` call through the file's [`BridgeRuntime`]. Allocates its
/// channel up-front in `new`, unlike [`BorrowedBlobPipeline`] which is lazy.
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
        let item_size = size_of::<T>() as u64;
        let start = range.byte_offset;
        let end = start + range.length * item_size;

        let mut buf: Vec<T> = vec![T::zeroed(); range.length as usize];
        // SAFETY: see `BorrowedBlobPipeline::schedule` for the rationale.
        let mut dst = unsafe { SendBytePtr::from_vec(&mut buf) };

        let stream_fut = self.file.inner.read_range(&self.file.path, start..end);
        let future = async move {
            let mut stream = stream_fut.await?;
            // SAFETY: see `BorrowedBlobPipeline::schedule`.
            let slice = unsafe { dst.as_slice_mut() };
            let mut writer = AlignedBufWriter::from_raw_bytes(slice);
            while let Some(chunk) = stream.next().await {
                writer
                    .write_all(&chunk?)
                    .await
                    .map_err(UniversalIoError::s3)?;
            }
            if writer.written() != writer.capacity() {
                return Err(UniversalIoError::S3Config {
                    description: format!(
                        "short read: expected {} bytes, got {}",
                        writer.capacity(),
                        writer.written(),
                    ),
                });
            }
            Ok(())
        };
        self.inner
            .schedule(&self.file.runtime, user_data, buf, future)
    }

    fn wait(&mut self) -> Result<Option<(U, Cow<'_, [T]>)>> {
        Ok(self.inner.wait()?.map(|(u, v)| (u, Cow::Owned(v))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a future that writes `data` into the destination buffer reachable
    /// through `dst`. Mirrors what the real pipeline schedule path does, just
    /// from a static byte slice instead of a network stream.
    async fn write_future(
        mut dst: SendBytePtr,
        data: &'static [u8],
    ) -> Result<(), UniversalIoError> {
        // SAFETY: the test holds the matching `Vec<u8>` in
        // `inner.pending` for the duration of the future, satisfying the
        // SendBytePtr invariant.
        let slice = unsafe { dst.as_slice_mut() };
        assert_eq!(
            slice.len(),
            data.len(),
            "test bug: destination buffer size does not match payload",
        );
        slice.copy_from_slice(data);
        Ok(())
    }

    #[test]
    fn pipeline_can_schedule_starts_true_and_blocks_when_full() {
        let (tx, rx) = PipelineInner::<u8, u32>::default_channel();
        let mut inner: PipelineInner<u8, u32> = PipelineInner::new(tx, rx);
        assert!(inner.can_schedule());
        for i in 0..BLOB_PIPELINE_CAPACITY as u64 {
            inner.pending.insert(i, (i as u32, Vec::new()));
        }
        assert!(!inner.can_schedule());
    }

    #[test]
    fn pipeline_schedule_returns_queue_full_when_capacity_reached() {
        let runtime = BridgeRuntime::global();
        let (tx, rx) = PipelineInner::<u8, u32>::default_channel();
        let mut inner: PipelineInner<u8, u32> = PipelineInner::new(tx, rx);
        for i in 0..BLOB_PIPELINE_CAPACITY as u64 {
            inner.pending.insert(i, (i as u32, Vec::new()));
        }
        let buf: Vec<u8> = vec![0u8; 1];
        let err = inner
            .schedule(&runtime, 999, buf, async { Ok(()) })
            .unwrap_err();
        assert!(matches!(err, UniversalIoError::QueueIsFull));
    }

    #[test]
    fn pipeline_schedule_and_wait_round_trip() {
        let runtime = BridgeRuntime::global();
        let (tx, rx) = PipelineInner::<u8, u32>::default_channel();
        let mut inner: PipelineInner<u8, u32> = PipelineInner::new(tx, rx);

        let mut buf: Vec<u8> = vec![0u8; 5];
        // SAFETY: `buf` moves into `inner.pending` below; the test does not
        // touch it again until `wait` returns it.
        let dst = unsafe { SendBytePtr::from_vec(&mut buf) };
        inner
            .schedule(&runtime, 111, buf, write_future(dst, b"hello"))
            .expect("schedule");

        let (user, bytes) = inner.wait().expect("wait ok").expect("some");
        assert_eq!(user, 111);
        assert_eq!(&bytes[..], b"hello");
    }

    #[test]
    fn pipeline_out_of_order_completion_preserves_user_data() {
        let runtime = BridgeRuntime::global();
        let (tx, rx) = PipelineInner::<u8, u32>::default_channel();
        let mut inner: PipelineInner<u8, u32> = PipelineInner::new(tx, rx);
        for (i, bytes) in [b"aaaa".as_slice(), b"bb".as_slice(), b"cccccc".as_slice()]
            .iter()
            .enumerate()
        {
            let bytes = *bytes;
            let mut buf: Vec<u8> = vec![0u8; bytes.len()];
            // SAFETY: see `pipeline_schedule_and_wait_round_trip`.
            let dst = unsafe { SendBytePtr::from_vec(&mut buf) };
            inner
                .schedule(&runtime, i as u32, buf, write_future(dst, bytes))
                .unwrap();
        }
        let mut seen = std::collections::HashSet::new();
        for _ in 0..3 {
            let (user, _bytes) = inner.wait().unwrap().unwrap();
            assert!(seen.insert(user), "user_data {user} seen twice");
        }
        assert_eq!(seen, [0u32, 1, 2].into_iter().collect());
        assert!(inner.wait().unwrap().is_none());
    }

    /// Two distinct runtimes feed reply responses into one pipeline; both
    /// runtimes execute work in parallel and the pipeline accepts both replies.
    #[test]
    fn pipeline_collects_replies_from_multiple_runtimes() {
        let rt_a = BridgeRuntime::new().expect("rt_a");
        let rt_b = BridgeRuntime::new().expect("rt_b");
        let (tx, rx) = PipelineInner::<u8, u32>::default_channel();
        let mut inner: PipelineInner<u8, u32> = PipelineInner::new(tx, rx);

        let mut buf_a: Vec<u8> = vec![0u8; 4];
        // SAFETY: see `pipeline_schedule_and_wait_round_trip`.
        let dst_a = unsafe { SendBytePtr::from_vec(&mut buf_a) };
        inner
            .schedule(&rt_a, 1, buf_a, write_future(dst_a, b"AAAA"))
            .unwrap();

        let mut buf_b: Vec<u8> = vec![0u8; 2];
        // SAFETY: see `pipeline_schedule_and_wait_round_trip`.
        let dst_b = unsafe { SendBytePtr::from_vec(&mut buf_b) };
        inner
            .schedule(&rt_b, 2, buf_b, write_future(dst_b, b"BB"))
            .unwrap();

        let mut seen: AHashMap<u32, Vec<u8>> = AHashMap::new();
        for _ in 0..2 {
            let (user, bytes) = inner.wait().unwrap().unwrap();
            seen.insert(user, bytes);
        }
        assert_eq!(seen[&1], b"AAAA");
        assert_eq!(seen[&2], b"BB");
    }
}
