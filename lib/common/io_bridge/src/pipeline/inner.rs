use std::future::Future;
use std::panic::AssertUnwindSafe;

use aligned_vec::{AVec, RuntimeAlign};
use common::universal_io::{UioResult, UniversalIoError, UserData};
use futures::FutureExt as _;
use slab::Slab;
use tokio::sync::mpsc;

use super::BLOB_PIPELINE_CAPACITY;
use crate::runtime::{BridgeResponse, BridgeRuntime};

/// A completed read drained from a pipeline: the caller `user_data`, the
/// destination buffer, and the time the read took (see [`BridgeResponse::duration`]).
type CompletedRead<U> = (U, AVec<u8, RuntimeAlign>, std::time::Duration);

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
/// Owns the reply channel and — via a [`Slab`] — the caller-side `user_data`
/// for every in-flight request, keyed by the slot assigned at schedule time so
/// out-of-order replies still reunite with the right caller context. The
/// destination aligned byte buffer lives inside the future itself and comes
/// back through the reply channel as the future's output, so the pipeline
/// never shares mutable buffer state with the worker task.
///
/// The [`BridgeRuntime`] is intentionally not stored here — callers supply it
/// on every `schedule` call. This keeps the pipeline cheap (one channel +
/// a slab) and lets the same pipeline collect replies from multiple runtimes
/// if desired.
pub(crate) struct PipelineInner<U> {
    /// Sender for the reply channel. Cloned into every spawned read task as
    /// its return address.
    tx: mpsc::Sender<BridgeResponse>,
    /// Receiver for the reply channel. `wait` blocks on this.
    rx: mpsc::Receiver<BridgeResponse>,
    /// In-flight reads awaiting a reply, tagged by slot.
    slots: Slab<U>,
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
            slots: Slab::with_capacity(BLOB_PIPELINE_CAPACITY),
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

    /// Enqueue an async read on `runtime` and tag the slot with `user_data` for
    /// out-of-order reassembly at `wait` time. The future owns its destination
    /// buffer and returns it as its output; no mutable buffer state is shared
    /// between this thread and the worker.
    ///
    /// # Parameters
    /// - `runtime`: the [`BridgeRuntime`] that will drive the future. The
    ///   pipeline is runtime-agnostic, so different `schedule` calls on the
    ///   same pipeline can target different runtimes.
    /// - `user_data`: opaque caller context (e.g. a request id, point id)
    ///   stored under the freshly assigned slot. Returned alongside the
    ///   destination buffer from [`Self::wait`].
    /// - `future`: the async work to perform. Must resolve to a
    ///   `Result<AVec<u8, RuntimeAlign>>`, be `Send + 'static`, and own all
    ///   of its captured state — it is spawned onto `runtime` and `.await`-ed
    ///   on a runtime worker thread.
    ///
    /// # Errors
    /// - [`UniversalIoError::QueueIsFull`] if the pipeline already has
    ///   [`BLOB_PIPELINE_CAPACITY`] pending requests.
    pub(crate) fn schedule<F>(
        &mut self,
        runtime: &BridgeRuntime,
        user_data: U,
        future: F,
    ) -> UioResult<()>
    where
        F: Future<Output = UioResult<AVec<u8, RuntimeAlign>>> + Send + 'static,
    {
        if !self.can_schedule() {
            return Err(UniversalIoError::QueueIsFull);
        }
        let slot = self.slots.insert(user_data);

        // Spawn the read directly onto the runtime; the task ships its output
        // (the destination aligned byte buffer) back over the pipeline's reply
        // channel, tagged with `slot`. The reply channel is sized to the
        // pipeline capacity, so the send never blocks on backpressure — the
        // only `send` error is the pipeline being dropped before collecting,
        // in which case discarding the buffer is correct.
        // Stamp the request at enqueue time so the measured latency spans the
        // full life of the request — including any time it waits for a free
        // runtime worker — not just the read future's execution. This matters
        // when a caller schedules a burst of reads at once (e.g. a batched
        // scroll), where queueing delay is part of the real per-request cost.
        let started = std::time::Instant::now();
        let reply_tx = self.tx.clone();
        runtime.handle().spawn(async move {
            // Catch a panic in the read future and turn it into an error reply,
            // so every scheduled slot is always answered. Without this, a
            // panicking task would unwind and drop its reply sender without
            // sending; `wait` holds the only other sender, so its `blocking_recv`
            // would then block forever waiting for a reply that never comes.
            // `AssertUnwindSafe`: the buffer captured by the future is dropped
            // on unwind, so the future's state is never observed afterwards.
            let result = match AssertUnwindSafe(future).catch_unwind().await {
                Ok(result) => result,
                Err(panic) => Err(UniversalIoError::TaskPanicked(panic_message(&*panic))),
            };
            let duration = started.elapsed();
            let _ = reply_tx
                .send(BridgeResponse::new(slot, result, duration))
                .await;
        });
        Ok(())
    }

    /// Block until any in-flight read completes, returning its caller
    /// `user_data`, destination buffer, and the time the read itself took.
    pub(crate) fn wait(&mut self) -> UioResult<Option<CompletedRead<U>>> {
        if self.slots.is_empty() {
            return Ok(None);
        }
        let response = self
            .rx
            .blocking_recv()
            .expect("tx held by self; cannot disconnect");
        let user_data = self
            .slots
            .try_remove(response.slot)
            .expect("response slot must be in pending");
        let duration = response.duration;
        let buf = response.result?;
        Ok(Some((user_data, buf, duration)))
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use ahash::AHashMap;

    use super::*;

    fn avec(bytes: &[u8]) -> AVec<u8, RuntimeAlign> {
        AVec::from_slice(1, bytes)
    }

    #[test]
    fn pipeline_can_schedule_starts_true_and_blocks_when_full() {
        let (tx, rx) = PipelineInner::<u32>::default_channel();
        let mut inner: PipelineInner<u32> = PipelineInner::new(tx, rx);
        assert!(inner.can_schedule());
        for i in 0..BLOB_PIPELINE_CAPACITY {
            inner.slots.insert(i as u32);
        }
        assert!(!inner.can_schedule());
    }

    #[test]
    fn pipeline_schedule_returns_queue_full_when_capacity_reached() {
        let runtime = BridgeRuntime::global();
        let (tx, rx) = PipelineInner::<u32>::default_channel();
        let mut inner: PipelineInner<u32> = PipelineInner::new(tx, rx);
        for i in 0..BLOB_PIPELINE_CAPACITY {
            inner.slots.insert(i as u32);
        }
        let err = inner
            .schedule(&runtime, 999, async { Ok(avec(&[0])) })
            .unwrap_err();
        assert_matches!(err, UniversalIoError::QueueIsFull);
    }

    #[test]
    fn pipeline_schedule_and_wait_round_trip() {
        let runtime = BridgeRuntime::global();
        let (tx, rx) = PipelineInner::<u32>::default_channel();
        let mut inner: PipelineInner<u32> = PipelineInner::new(tx, rx);
        inner
            .schedule(&runtime, 111, async { Ok(avec(b"hello")) })
            .expect("schedule");
        let (user, bytes, _took) = inner.wait().expect("wait ok").expect("some");
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
                .schedule(&runtime, i as u32, async move { Ok(avec(bytes)) })
                .unwrap();
        }
        let mut seen = std::collections::HashSet::new();
        for _ in 0..3 {
            let (user, _bytes, _took) = inner.wait().unwrap().unwrap();
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
        let (tx, rx) = PipelineInner::<u32>::default_channel();
        let mut inner: PipelineInner<u32> = PipelineInner::new(tx, rx);
        inner
            .schedule(&runtime, 7, async {
                panic!("boom");
                Ok(avec(&[]))
            })
            .expect("schedule");
        let err = inner.wait().unwrap_err();
        assert_matches!(err, UniversalIoError::TaskPanicked(msg) if msg == "boom");
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
            .schedule(&rt_a, 1, async { Ok(avec(b"AAAA")) })
            .unwrap();
        inner.schedule(&rt_b, 2, async { Ok(avec(b"BB")) }).unwrap();
        let mut seen: AHashMap<u32, Vec<u8>> = AHashMap::new();
        for _ in 0..2 {
            let (user, bytes, _took) = inner.wait().unwrap().unwrap();
            seen.insert(user, bytes.to_vec());
        }
        assert_eq!(seen[&1], b"AAAA");
        assert_eq!(seen[&2], b"BB");
    }
}
