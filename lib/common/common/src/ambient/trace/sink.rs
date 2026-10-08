use std::io::{self, BufWriter, Write as _};
use std::path::PathBuf;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::thread;
use std::time::{Duration, Instant};

use fs_err::File;

use super::cpu::process_cpu_ns;
use super::event::{Event, Nanoseconds};

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

pub struct FlushGuard(Option<thread::JoinHandle<io::Result<()>>>);

pub(super) struct Sink {
    tx: SyncSender<SinkMessage>,
    pub(super) origin: Instant,
    dropped: AtomicU64,
}

enum SinkMessage {
    Event(Event),
    Stop,
}

pub(super) static SINK: OnceLock<Sink> = OnceLock::new();

impl Sink {
    pub(super) fn send(&self, event: Event) {
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
    const CPU_INTERVAL: Duration = Duration::from_millis(10);

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

pub(super) fn elapsed_ns(base: Instant, at: Instant) -> Nanoseconds {
    at.duration_since(base).as_nanos() as Nanoseconds
}
