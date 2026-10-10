use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use std::{task, thread};

use super::event::Event;
use super::sink::{SINK, elapsed_ns, enabled};

#[derive(Clone, Copy)]
pub struct Context(pub(super) u64);

struct ContextGuard(u64);

pub struct WithCtx<F> {
    ctx: Context,
    future: F,
}

pub struct Phase(Option<ActivePhase>);

struct ActivePhase {
    id: u64,
    start: Instant,
    name: &'static str,
}

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

impl Context {
    fn slot() -> &'static thread::LocalKey<Cell<u64>> {
        thread_local! {
            static CURRENT: Cell<u64> = const { Cell::new(0) };
        }
        &CURRENT
    }

    pub fn current() -> Self {
        Self(Self::slot().get())
    }

    fn enter_guard(self) -> ContextGuard {
        ContextGuard(Self::slot().replace(self.0))
    }

    pub fn enter<R>(self, f: impl FnOnce() -> R) -> R {
        let _entered = self.enter_guard();
        f()
    }

    pub fn wrap<F: Future>(self, future: F) -> WithCtx<F> {
        WithCtx { ctx: self, future }
    }
}

impl Drop for ContextGuard {
    fn drop(&mut self) {
        Context::slot().set(self.0);
    }
}

impl<F: Future> Future for WithCtx<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<F::Output> {
        // SAFETY: `future` is pinned through `self` and never moved out of it.
        let this = unsafe { self.get_unchecked_mut() };
        let future = unsafe { Pin::new_unchecked(&mut this.future) };
        let _entered = this.ctx.enter_guard();
        future.poll(cx)
    }
}

impl Phase {
    pub fn start(name: &'static str) -> Self {
        Self(enabled().then(|| ActivePhase {
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            start: Instant::now(),
            name,
        }))
    }

    pub fn enter<R>(&self, f: impl FnOnce() -> R) -> R {
        Context(self.0.as_ref().map_or(0, |active| active.id)).enter(f)
    }
}

impl Drop for Phase {
    fn drop(&mut self) {
        let (Some(active), Some(sink)) = (self.0.take(), SINK.get()) else {
            return;
        };
        sink.send(Event::Phase {
            id: active.id,
            start_ns: elapsed_ns(sink.origin, active.start),
            end_ns: elapsed_ns(sink.origin, Instant::now()),
            name: active.name,
        });
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::ambient::trace::{IoRequest, Op, Outcome, mark, start};

    #[test]
    fn records_nested_spans_from_every_thread() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("uio.jsonl");
        let guard = start(&path).expect("output file");

        let phase = Phase::start("open");
        phase.enter(|| {
            mark!("round 0 begin ({} points)", 3);
            let mut request = IoRequest::new(Op::Read, Path::new("links.bin"), 4_096..8_192);
            request.start();
            request.finish(Outcome::Ok);
            drop(IoRequest::new(Op::Read, Path::new("unsent.bin"), 0..1));
        });
        let ctx = phase.enter(Context::current);
        std::thread::spawn(move || {
            ctx.enter(|| IoRequest::new(Op::Len, Path::new("meta.json"), 0..0).start())
        })
        .join()
        .expect("thread");
        drop(phase);
        drop(guard);

        let written = fs_err::read_to_string(&path).expect("trace file");
        let events: Vec<serde_json::Value> = written
            .lines()
            .map(|line| serde_json::from_str(line).expect("json line"))
            .collect();
        let events: Vec<_> = events
            .into_iter()
            .filter(|event| event["kind"] != "cpu")
            .collect();
        let kinds: Vec<_> = events.iter().map(|event| event["kind"].as_str()).collect();
        assert_eq!(
            kinds,
            [
                Some("mark"),
                Some("request"),
                Some("request"),
                Some("phase"),
            ],
            "{written}"
        );
        assert_eq!(events[0]["text"], "round 0 begin (3 points)");
        assert_eq!(events[0]["parent"], events[3]["id"]);
        assert_eq!(events[1]["length"], 4_096);
        assert_eq!(events[2]["outcome"], "cancelled");
        assert_eq!(events[2]["parent"], events[3]["id"]);
    }
}
