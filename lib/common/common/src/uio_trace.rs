//! Network request tracing for UIO.
//!
//! Visualize with `tools/uio-trace-visualizer.html`.

use std::cell::Cell;
use std::future::Future;
use std::io::{self, BufWriter, Write as _};
use std::ops::Range;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::time::{Duration, Instant};
use std::{task, thread};

use fs_err::File;
use serde::Serialize;

type Nanoseconds = u64;

/// A single event in the log.
/// Timestamps are relative to the trace start.
#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Event {
    /// Span-like event. Can be nested.
    /// Create with [`Phase::start`].
    Phase {
        /// Other events can have `parent` set to this ID.
        id: u64,
        start_ns: Nanoseconds,
        end_ns: Nanoseconds,
        name: &'static str,
    },
    /// Text log-like event.
    /// Create with [`mark!`].
    Mark {
        parent: u64,
        at_ns: Nanoseconds,
        text: String,
    },
    /// Single GET request.
    /// Created by UIO backend implementations, with [`Request::start`].
    Request {
        parent: u64,
        start_ns: Nanoseconds,
        end_ns: Nanoseconds,
        op: Op,
        path: String,
        offset: u64,
        length: u64,
        outcome: Outcome,
    },
    /// Description of the file structure.
    /// Lets the visualizer distinguish offsets and links within the same file.
    /// Create with [`file_sections`].
    Sections {
        path: String,
        sections: Vec<(&'static str, u64)>,
    },
    /// CPU usage.
    /// Written automatically.
    Cpu {
        at_ns: Nanoseconds,
        cpu_ns: Nanoseconds,
    },
    /// Emitted when can't keep up.
    /// Written automatically.
    Dropped { count: u64 },
}

/// Request operation kind.
#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Op {
    List,
    Exists,
    Read,
    ReadFrom,
    Len,
}

/// Request outcome.
#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Ok,
    Err,
    Cancelled,
}

/// Enable UIO tracing, write events to the given `path`.
pub fn start(path: impl Into<PathBuf>) -> io::Result<FlushGuard> {
    const QUEUE_LEN: usize = 16384;

    let file = File::create(path)?;
    let (tx, rx) = mpsc::sync_channel(QUEUE_LEN);
    let origin = Instant::now();
    let writer = thread::Builder::new()
        .name("uio-trace".to_owned())
        .spawn(move || write_events_thread(rx, BufWriter::new(file), origin))?;
    let sink = Sink {
        tx,
        origin,
        dropped: AtomicU64::new(0),
    };
    SINK.set(sink)
        .map_err(|_| io::Error::other("tracing is already started"))?;
    Ok(FlushGuard(Some(writer)))
}

pub fn enabled() -> bool {
    SINK.get().is_some()
}

/// `println!`-like macro to record a [Event::Mark] in the trace.
#[doc(hidden)]
#[macro_export]
macro_rules! __uio_trace_mark {
    ($($arg:tt)*) => {
        if $crate::uio_trace::enabled() {
            $crate::uio_trace::__record_mark(format!($($arg)*));
        }
    };
}
pub use __uio_trace_mark as mark;

#[doc(hidden)]
pub fn __record_mark(text: String) {
    let Some(sink) = SINK.get() else { return };
    sink.send(Event::Mark {
        parent: Context::current().0,
        at_ns: elapsed_ns(sink.origin, Instant::now()),
        text,
    });
}

/// Record [Event::Sections].
pub fn file_sections(path: &str, sections: Vec<(&'static str, u64)>) {
    if let Some(sink) = SINK.get() {
        let path = path.to_owned();
        sink.send(Event::Sections { path, sections });
    }
}

pub struct FlushGuard(Option<thread::JoinHandle<io::Result<()>>>);

#[derive(Clone, Copy)]
pub struct Context(u64);

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

pub struct Request(Option<ActiveRequest>);

struct ActiveRequest {
    parent: u64,
    start: Instant,
    op: Op,
    path: String,
    range: Range<u64>,
    outcome: Option<Outcome>,
}

struct Sink {
    tx: SyncSender<SinkMessage>,
    origin: Instant,
    dropped: AtomicU64,
}

enum SinkMessage {
    Event(Event),
    Stop,
}

static SINK: OnceLock<Sink> = OnceLock::new();
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

    fn enter(self) -> ContextGuard {
        ContextGuard(Self::slot().replace(self.0))
    }

    pub fn in_scope<R>(self, f: impl FnOnce() -> R) -> R {
        let _entered = self.enter();
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
        let _entered = this.ctx.enter();
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

    pub fn in_scope<R>(&self, f: impl FnOnce() -> R) -> R {
        Context(self.0.as_ref().map_or(0, |active| active.id)).in_scope(f)
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

impl Request {
    pub fn start(op: Op, path: &str, range: Range<u64>) -> Self {
        Self(enabled().then(|| ActiveRequest {
            parent: Context::current().0,
            start: Instant::now(),
            op,
            path: path.to_owned(),
            range,
            outcome: None,
        }))
    }

    pub fn set(&mut self, outcome: Outcome) {
        if let Some(active) = &mut self.0 {
            active.outcome.get_or_insert(outcome);
        }
    }

    pub fn set_result<T, E>(&mut self, result: &Result<T, E>) {
        self.set(match result {
            Ok(_) => Outcome::Ok,
            Err(_) => Outcome::Err,
        });
    }
}

impl Drop for Request {
    fn drop(&mut self) {
        let (Some(active), Some(sink)) = (self.0.take(), SINK.get()) else {
            return;
        };
        sink.send(Event::Request {
            parent: active.parent,
            start_ns: elapsed_ns(sink.origin, active.start),
            end_ns: elapsed_ns(sink.origin, Instant::now()),
            op: active.op,
            path: active.path,
            offset: active.range.start,
            length: active.range.end - active.range.start,
            outcome: active.outcome.unwrap_or(Outcome::Cancelled),
        });
    }
}

impl Sink {
    fn send(&self, event: Event) {
        match self.tx.try_send(SinkMessage::Event(event)) {
            Ok(()) => (),
            Err(TrySendError::Full(_)) => _ = self.dropped.fetch_add(1, Ordering::Relaxed),
            Err(TrySendError::Disconnected(_)) => (),
        }
    }
}

impl Drop for FlushGuard {
    fn drop(&mut self) {
        let Some(sink) = SINK.get() else { return };
        let count = sink.dropped.load(Ordering::Relaxed);
        if count > 0 {
            sink.tx
                .send(SinkMessage::Event(Event::Dropped { count }))
                .ok();
        }
        sink.tx.send(SinkMessage::Stop).ok();
        let Some(writer) = self.0.take() else { return };
        match writer.join() {
            Ok(Ok(())) => {}
            Ok(Err(err)) => log::error!("uio trace: writing failed: {err}"),
            Err(_panic) => log::error!("uio trace: the writer thread panicked"),
        }
    }
}

fn write_events_thread(
    rx: Receiver<SinkMessage>,
    mut out: BufWriter<File>,
    origin: Instant,
) -> io::Result<()> {
    const CPU_INTERVAL: Duration = Duration::from_millis(2);

    let mut write_event = |event: &Event| {
        serde_json::to_writer(&mut out, event)?;
        out.write_all(b"\n")
    };

    let mut due = origin;
    loop {
        let now = Instant::now();
        if now >= due {
            due = now + CPU_INTERVAL;
            let event = Event::Cpu {
                at_ns: elapsed_ns(origin, now),
                cpu_ns: process_cpu_ns(),
            };
            write_event(&event)?;
        }
        match rx.recv_timeout(due.saturating_duration_since(now)) {
            Ok(SinkMessage::Event(event)) => write_event(&event)?,
            Ok(SinkMessage::Stop) | Err(RecvTimeoutError::Disconnected) => break,
            Err(RecvTimeoutError::Timeout) => (),
        }
    }
    out.flush()
}

fn elapsed_ns(base: Instant, at: Instant) -> Nanoseconds {
    at.duration_since(base).as_nanos() as Nanoseconds
}

#[cfg(target_os = "linux")]
fn process_cpu_ns() -> Nanoseconds {
    let mut ts = nix::libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: clock_gettime with CLOCK_PROCESS_CPUTIME_ID is always valid.
    let ret = unsafe { nix::libc::clock_gettime(nix::libc::CLOCK_PROCESS_CPUTIME_ID, &mut ts) };
    if ret == 0 {
        ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
    } else {
        0
    }
}

#[cfg(not(target_os = "linux"))]
fn process_cpu_ns() -> Nanoseconds {
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_nested_spans_from_every_thread() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("uio.jsonl");
        let guard = start(&path).expect("output file");

        let phase = Phase::start("open");
        phase.in_scope(|| {
            mark!("round 0 begin ({} points)", 3);
            let mut request = Request::start(Op::Read, "links.bin", 4_096..8_192);
            request.set(Outcome::Ok);
        });
        let ctx = phase.in_scope(Context::current);
        std::thread::spawn(move || {
            ctx.in_scope(|| drop(Request::start(Op::Len, "meta.json", 0..0)))
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
