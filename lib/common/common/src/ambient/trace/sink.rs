//! Trace sink that writes into a file.

use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, OnceLock};
use std::thread;

use arc_swap::ArcSwapOption;
use fs_err::File;
use parking_lot::Mutex;

use super::Event;

/// Events waiting to be written; the rest are dropped and counted.
const QUEUE_LEN: usize = 16_384;

static GLOBAL: OnceLock<Sink> = OnceLock::new();

/// Trace everything: request go to `sink`, once it's started.
pub fn install(sink: Sink) -> Result<(), Sink> {
    GLOBAL.set(sink)
}

/// The installed sink, if started.
pub fn global() -> Option<&'static Sink> {
    GLOBAL.get().filter(|sink| sink.is_started())
}

/// Writes events from a background thread, one JSON line each.
#[derive(Clone)]
pub struct Sink(Arc<Inner>);

struct Inner {
    log: Arc<Mutex<File>>,
    worker: Mutex<Option<thread::JoinHandle<()>>>,
    /// Mirrors `worker`, lock-free for [`Sink::send`].
    sender: ArcSwapOption<SyncSender<Event>>,
    /// Since the last written batch.
    dropped_events_count: Arc<AtomicU64>,
}

impl Sink {
    /// Write into the file. The file is overridden/truncated.
    pub fn file(path: PathBuf) -> io::Result<Self> {
        Ok(Self::new(File::create(path)?))
    }

    fn new(log: File) -> Self {
        Self(Arc::new(Inner {
            log: Arc::new(Mutex::new(log)),
            worker: Mutex::new(None),
            sender: ArcSwapOption::empty(),
            dropped_events_count: Arc::default(),
        }))
    }

    pub fn is_started(&self) -> bool {
        self.0.sender.load().is_some()
    }

    /// Start the background thread, if not already started.
    pub fn start(&self) -> io::Result<()> {
        let mut worker = self.0.worker.lock();
        if worker.is_some() {
            return Ok(());
        }
        let (sender, receiver) = mpsc::sync_channel(QUEUE_LEN);
        let (log, dropped) = (
            Arc::clone(&self.0.log),
            Arc::clone(&self.0.dropped_events_count),
        );
        let thread = thread::Builder::new()
            .name("uio-trace-sink".to_owned())
            .spawn(move || write_events(&receiver, &log, &dropped))?;
        *worker = Some(thread);
        self.0.sender.store(Some(Arc::new(sender)));
        Ok(())
    }

    /// Stop the background thread, blocking until all queued traces are written.
    pub fn stop(&self) {
        let worker = {
            let mut worker = self.0.worker.lock();
            self.0.sender.store(None);
            worker.take()
        };
        if let Some(thread) = worker {
            thread
                .join()
                .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
        }
    }

    /// Record an `event`.
    /// The event is dropped if not started or if the queue is full.
    pub fn send(&self, event: Event) {
        if let Some(sender) = &*self.0.sender.load()
            && sender.try_send(event).is_err()
        {
            self.0.dropped_events_count.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn write_events(receiver: &Receiver<Event>, log: &Arc<Mutex<File>>, dropped: &AtomicU64) {
    let mut lines = Vec::new();
    for event in receiver {
        push_line(&mut lines, &event);
        while let Ok(event) = receiver.try_recv() {
            push_line(&mut lines, &event);
        }
        write_lines(&mut lines, dropped, log);
    }
    // Dropped after the last burst.
    write_lines(&mut lines, dropped, log);
}

/// One write per burst, with the count of events dropped so far.
fn write_lines(lines: &mut Vec<u8>, dropped: &AtomicU64, log: &Arc<Mutex<File>>) {
    let count = dropped.swap(0, Ordering::Relaxed);
    if count > 0 {
        push_line(lines, &Event::Dropped { count });
    }
    if lines.is_empty() {
        return;
    }
    if let Err(err) = log.lock().write_all(lines) {
        log::warn!("Failed to write uio trace: {err}");
    }
    lines.clear();
}

fn push_line(lines: &mut Vec<u8>, event: &Event) {
    serde_json::to_writer(&mut *lines, event).expect("events are serializable");
    lines.push(b'\n');
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::ambient::trace::clock::now;
    use crate::ambient::trace::testing::lines;

    impl Sink {
        pub(crate) fn current_path(&self) -> PathBuf {
            self.0.log.lock().path().to_owned()
        }
    }

    #[test]
    fn stop_writes_queued_events() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let sink = Sink::file(file.path().to_owned()).unwrap();
        sink.send(mark()); // not started: dropped
        sink.start().unwrap();
        sink.send(mark());
        sink.send(mark());
        sink.stop();
        sink.send(mark()); // stopped: dropped

        assert_eq!(
            json!(lines(&sink)),
            json!([
                {"kind": "mark", "parent": 0, "timestamp": "*", "text": "x"},
                {"kind": "mark", "parent": 0, "timestamp": "*", "text": "x"},
            ])
        );
    }

    #[test]
    fn concurrent_start_stop_keeps_started_in_sync() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let sink = Sink::file(file.path().to_owned()).unwrap();
        std::thread::scope(|s| {
            for _ in 0..2 {
                s.spawn(|| {
                    for _ in 0..100 {
                        sink.start().unwrap();
                        sink.stop();
                        let worker = sink.0.worker.lock();
                        assert_eq!(sink.is_started(), worker.is_some());
                    }
                });
            }
        });
        sink.stop();
        assert!(!sink.is_started());
    }

    #[test]
    fn dropped_events_are_counted() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let sink = Sink::file(file.path().to_owned()).unwrap();
        sink.start().unwrap();
        let total = QUEUE_LEN + 10;
        {
            // Park the writer on the lock with one event, then fill the queue.
            let _blocked = sink.0.log.lock();
            sink.send(mark());
            std::thread::sleep(std::time::Duration::from_millis(20));
            for _ in 0..total {
                sink.send(mark());
            }
        }
        sink.stop();

        let lines = lines(&sink);
        let marks = lines.iter().filter(|line| line["kind"] == "mark").count();
        let dropped: u64 = lines
            .iter()
            .filter(|line| line["kind"] == "dropped")
            .map(|line| line["count"].as_u64().unwrap())
            .sum();
        // Exactly 10 only if the writer took just the first event before parking.
        assert!(dropped > 0);
        assert_eq!(marks + dropped as usize, total + 1);
    }

    fn mark() -> Event {
        Event::Mark {
            parent: 0,
            timestamp: now(),
            text: "x".into(),
        }
    }
}
