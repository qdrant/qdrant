use std::future::Future;
use std::panic::AssertUnwindSafe;

use common::universal_io::{Result, UniversalIoError, UserData};
use futures::FutureExt as _;
use tokio::sync::mpsc;

use super::BLOB_PIPELINE_CAPACITY;
use super::slots::PendingSlots;
use crate::runtime::{BridgeResponse, BridgeRuntime};

/// Best-effort extraction of a human-readable message from a caught panic
/// payload. `panic!` payloads are most commonly `&'static str` or `String`;
/// anything else is reported generically.
fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = panic.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}

/// Pipeline-side bookkeeping.
///
/// Owns the reply channel and — via [`PendingSlots`] — the destination `Vec<T>`
/// for every in-flight request. The future writes its bytes directly into the
/// `Vec<T>` via a `SendBytePtr`; the channel reply only carries the completion
/// status, so neither the runtime nor the channel touch the typed payload.
///
/// The [`BridgeRuntime`] is intentionally not stored here — callers supply it
/// on every `schedule` call. This keeps the pipeline cheap (one channel +
/// a map) and lets the same pipeline collect replies from multiple runtimes
/// if desired.
pub(crate) struct PipelineInner<T, U> {
    /// Sender for the reply channel. Cloned into every spawned read task as
    /// its return address.
    tx: mpsc::Sender<BridgeResponse>,
    /// Receiver for the reply channel. `wait` blocks on this.
    rx: mpsc::Receiver<BridgeResponse>,
    /// In-flight reads awaiting a reply, each owning its destination buffer,
    /// tagged by slot.
    slots: PendingSlots<T, U>,
}

impl<T, U> PipelineInner<T, U>
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
            slots: PendingSlots::new(),
        }
    }

    /// Standard `(tx, rx)` pair sized to [`BLOB_PIPELINE_CAPACITY`]. Most
    /// pipelines should use this and feed it directly into [`Self::new`].
    pub(crate) fn default_channel() -> (mpsc::Sender<BridgeResponse>, mpsc::Receiver<BridgeResponse>)
    {
        mpsc::channel(BLOB_PIPELINE_CAPACITY)
    }

    pub(crate) fn can_schedule(&self) -> bool {
        self.slots.len() < BLOB_PIPELINE_CAPACITY
    }

    /// Enqueue an async read on `runtime`, taking ownership of the destination
    /// `Vec<T>` and tagging the slot with `user_data` for out-of-order
    /// reassembly at `wait` time.
    ///
    /// The future is expected to write its bytes into the destination buffer
    /// via a `SendBytePtr` previously derived (by the caller) from the same
    /// `Vec<T>` and captured in the future's closure. The future's `Result<()>`
    /// only signals completion — no payload travels on the channel.
    ///
    /// # Parameters
    /// - `runtime`: the [`BridgeRuntime`] that will drive the future. The
    ///   pipeline is runtime-agnostic, so different `schedule` calls on the
    ///   same pipeline can target different runtimes.
    /// - `user_data`: opaque caller context (e.g. a request id, point id)
    ///   stored under the freshly assigned slot. Returned alongside the
    ///   destination buffer from [`Self::wait`].
    /// - `buf`: pre-allocated destination of exact byte size. Moved into the
    ///   slot map before the future is dispatched; the heap allocation address
    ///   is stable across this move, so the `SendBytePtr` captured in `future`
    ///   remains valid.
    /// - `future`: the async work to perform. Must resolve to `Result<()>`, be
    ///   `Send + 'static`, and own all of its captured state — it is spawned
    ///   onto `runtime` and `.await`-ed on a runtime worker thread.
    ///
    /// # Errors
    /// - [`UniversalIoError::QueueIsFull`] if the pipeline already has
    ///   [`BLOB_PIPELINE_CAPACITY`] pending requests. The `buf` is dropped.
    pub(crate) fn schedule<F>(
        &mut self,
        runtime: &BridgeRuntime,
        user_data: U,
        buf: Vec<T>,
        future: F,
    ) -> Result<()>
    where
        F: Future<Output = Result<()>> + Send + 'static,
    {
        if !self.can_schedule() {
            return Err(UniversalIoError::QueueIsFull);
        }
        let slot = self.slots.insert(user_data, buf);

        // Spawn the read directly onto the runtime; the task writes into the
        // slot-owned buffer and ships only its completion status back over the
        // reply channel, tagged with `slot`. The reply channel is sized to the
        // pipeline capacity, so the send never blocks on backpressure — the
        // only `send` error is the pipeline being dropped before collecting,
        // in which case discarding the status is correct.
        let reply_tx = self.tx.clone();
        runtime.handle().spawn(async move {
            // Catch a panic in the read future and turn it into an error reply,
            // so every scheduled slot is always answered. Without this, a
            // panicking task would unwind and drop its reply sender without
            // sending; `wait` holds the only other sender, so its `blocking_recv`
            // would then block forever waiting for a reply that never comes.
            // `AssertUnwindSafe`: the buffer is dropped (never read) on the
            // error path, so the future's captured state is never observed after.
            let result = match AssertUnwindSafe(future).catch_unwind().await {
                Ok(result) => result,
                Err(panic) => Err(UniversalIoError::TaskPanicked(panic_message(&*panic))),
            };
            std::mem::drop(reply_tx.send(BridgeResponse::new(slot, result)).await);
        });
        Ok(())
    }

    pub(crate) fn wait(&mut self) -> Result<Option<(U, Vec<T>)>> {
        if self.slots.is_empty() {
            return Ok(None);
        }
        let response = self
            .rx
            .blocking_recv()
            .expect("tx held by self; cannot disconnect");
        let (user_data, buf) = self
            .slots
            .remove(response.slot)
            .expect("response slot must be in pending");
        response.result?;
        Ok(Some((user_data, buf)))
    }
}

#[cfg(test)]
mod tests {
    use ahash::AHashMap;

    use super::*;
    use crate::pipeline::buffer::SendBytePtr;

    /// Synchronously write `data` into the destination buffer reachable through
    /// `dst`. Callers wrap the call in `async move { ...; Ok(()) }` to feed it
    /// into `PipelineInner::schedule`, mirroring what the real schedule path
    /// does (just from a static byte slice instead of a network stream).
    fn write_into(mut dst: SendBytePtr, data: &'static [u8]) {
        // SAFETY: the test holds the matching `Vec<u8>` in `inner`'s slot map
        // for the duration of the wrapping future, satisfying the SendBytePtr
        // invariant.
        let slice = unsafe { dst.as_slice_mut() };
        assert_eq!(
            slice.len(),
            data.len(),
            "test bug: destination buffer size does not match payload",
        );
        slice.copy_from_slice(data);
    }

    #[test]
    fn pipeline_can_schedule_starts_true_and_blocks_when_full() {
        let (tx, rx) = PipelineInner::<u8, u32>::default_channel();
        let mut inner: PipelineInner<u8, u32> = PipelineInner::new(tx, rx);
        assert!(inner.can_schedule());
        for i in 0..BLOB_PIPELINE_CAPACITY {
            inner.slots.insert(i as u32, Vec::new());
        }
        assert!(!inner.can_schedule());
    }

    #[test]
    fn pipeline_schedule_returns_queue_full_when_capacity_reached() {
        let runtime = BridgeRuntime::global();
        let (tx, rx) = PipelineInner::<u8, u32>::default_channel();
        let mut inner: PipelineInner<u8, u32> = PipelineInner::new(tx, rx);
        for i in 0..BLOB_PIPELINE_CAPACITY {
            inner.slots.insert(i as u32, Vec::new());
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
        // SAFETY: `buf` moves into the slot map below; the test does not touch
        // it again until `wait` returns it.
        let dst = unsafe { SendBytePtr::from_vec(&mut buf) };
        inner
            .schedule(&runtime, 111, buf, async move {
                write_into(dst, b"hello");
                Ok(())
            })
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
                .schedule(&runtime, i as u32, buf, async move {
                    write_into(dst, bytes);
                    Ok(())
                })
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

    /// A panicking read future must surface as a [`UniversalIoError::TaskPanicked`]
    /// reply rather than leaving its slot unanswered and hanging `wait` forever.
    #[test]
    #[expect(unreachable_code, reason = "panic diverges before the typed tail")]
    fn pipeline_panicking_future_yields_error_not_hang() {
        let runtime = BridgeRuntime::global();
        let (tx, rx) = PipelineInner::<u8, u32>::default_channel();
        let mut inner: PipelineInner<u8, u32> = PipelineInner::new(tx, rx);
        inner
            .schedule(&runtime, 7, vec![0u8; 1], async {
                panic!("boom");
                Ok(())
            })
            .expect("schedule");
        let err = inner.wait().unwrap_err();
        assert!(matches!(err, UniversalIoError::TaskPanicked(msg) if msg == "boom"));
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
            .schedule(&rt_a, 1, buf_a, async move {
                write_into(dst_a, b"AAAA");
                Ok(())
            })
            .unwrap();

        let mut buf_b: Vec<u8> = vec![0u8; 2];
        // SAFETY: see `pipeline_schedule_and_wait_round_trip`.
        let dst_b = unsafe { SendBytePtr::from_vec(&mut buf_b) };
        inner
            .schedule(&rt_b, 2, buf_b, async move {
                write_into(dst_b, b"BB");
                Ok(())
            })
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
