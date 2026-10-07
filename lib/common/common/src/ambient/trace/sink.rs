//! Trace sink that writes into rotating gzipped files.

use std::collections::BTreeMap;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, OnceLock};
use std::thread;

use arc_swap::ArcSwapOption;
use bytes::Bytes;
use flate2::read::GzEncoder;
use fs_err::{self as fs, File};
use futures::{Stream, StreamExt as _, stream};
use parking_lot::Mutex;
use tokio_util::io::{ReaderStream, SyncIoBridge};

use super::Event;
use super::clock::now;
use super::event::Timestamp;

/// Rotate once the file grows past this.
const MAX_FILE_SIZE: u64 = 32_000_000;
/// How many files to keep, including the current one.
const MAX_FILES: usize = 10;
/// Events waiting to be written; the rest are dropped and counted.
const QUEUE_LEN: usize = 16_384;
/// Buffer between the reading thread and the stream.
const READ_BUFFER: usize = 64 * 1024;

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
    /// Where to rotate into; `None` for a single file.
    dir: Option<PathBuf>,
    log: Arc<Mutex<Log>>,
    worker: Mutex<Option<thread::JoinHandle<()>>>,
    /// Mirrors `worker`, lock-free for [`Sink::send`].
    sender: ArcSwapOption<SyncSender<Event>>,
    /// Since the last written batch.
    dropped_events_count: Arc<AtomicU64>,
}

/// The file being written.
struct Log {
    file: File,
    path: PathBuf,
    len: u64,
}

impl Sink {
    /// Write into the file. The file is overridden/truncated.
    pub fn file(path: PathBuf) -> io::Result<Self> {
        Ok(Self::new(None, Log::create(path)?))
    }

    /// Rotating `trace.<timestamp>.jsonl[.gz]` files.
    pub fn dir(dir: PathBuf) -> io::Result<Self> {
        fs::create_dir_all(&dir)?;
        for (timestamp, plain) in list_logs(&dir)? {
            if plain {
                spawn_compress(log_path(&dir, timestamp, false))?;
            }
        }
        let log = Log::create(log_path(&dir, now(), false))?;
        Ok(Self::new(Some(dir), log))
    }

    fn new(dir: Option<PathBuf>, log: Log) -> Self {
        Self(Arc::new(Inner {
            dir,
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
        let (dir, log, dropped) = (
            self.0.dir.clone(),
            Arc::clone(&self.0.log),
            Arc::clone(&self.0.dropped_events_count),
        );
        let thread = thread::Builder::new()
            .name("uio-trace-sink".to_owned())
            .spawn(move || write_events(&receiver, dir.as_deref(), &log, &dropped))?;
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

    /// Read all traces as a stream of gzipped bytes, oldest first.
    pub fn read(&self) -> impl Stream<Item = io::Result<Bytes>> + use<> {
        let sink = self.clone();
        let (read_half, write_half) = tokio::io::duplex(READ_BUFFER);
        let task = tokio::task::spawn_blocking(move || {
            let mut output = SyncIoBridge::new(write_half);
            copy_logs(&sink.0, &mut output)?;
            output.shutdown()
        });
        ReaderStream::new(read_half).chain(
            stream::once(async move { task.await.map_err(io::Error::other)? })
                .filter_map(|result| async move { result.err().map(Err) }),
        )
    }
}

fn write_events(
    receiver: &Receiver<Event>,
    dir: Option<&Path>,
    log: &Arc<Mutex<Log>>,
    dropped: &AtomicU64,
) {
    let mut lines = Vec::new();
    for event in receiver {
        push_line(&mut lines, &event);
        while let Ok(event) = receiver.try_recv() {
            push_line(&mut lines, &event);
        }
        write_lines(&mut lines, dropped, dir, log);
    }
    // Dropped after the last burst.
    write_lines(&mut lines, dropped, dir, log);
}

/// One write per burst, with the count of events dropped so far.
fn write_lines(
    lines: &mut Vec<u8>,
    dropped: &AtomicU64,
    dir: Option<&Path>,
    log: &Arc<Mutex<Log>>,
) {
    let count = dropped.swap(0, Ordering::Relaxed);
    if count > 0 {
        push_line(lines, &Event::Dropped { count });
    }
    if lines.is_empty() {
        return;
    }
    let mut current = log.lock();
    let result = current.file.write_all(lines).and_then(|()| {
        current.len += lines.len() as u64;
        if let Some(dir) = dir
            && current.len >= MAX_FILE_SIZE
        {
            rotate(dir, &mut current)?;
        }
        Ok(())
    });
    if let Err(err) = result {
        log::warn!("Failed to write uio trace: {err}");
    }
    lines.clear();
}

fn push_line(lines: &mut Vec<u8>, event: &Event) {
    serde_json::to_writer(&mut *lines, event).expect("events are serializable");
    lines.push(b'\n');
}

impl Log {
    fn create(path: PathBuf) -> io::Result<Self> {
        let file = File::create(&path)?;
        Ok(Self { file, path, len: 0 })
    }
}

/// Start a new file.
fn rotate(dir: &Path, current: &mut Log) -> io::Result<()> {
    let old = std::mem::replace(current, Log::create(log_path(dir, now(), false))?);
    // Plain ones are still being gzipped; they go on a later rotation.
    for (&timestamp, &plain) in list_logs(dir)?.iter().rev().skip(MAX_FILES) {
        if !plain {
            fs::remove_file(log_path(dir, timestamp, true))?;
        }
    }
    // Gzip the old file in the background, so writes don't wait for it.
    spawn_compress(old.path)
}

fn spawn_compress(plain: PathBuf) -> io::Result<()> {
    thread::Builder::new()
        .name("uio-trace-gzip".to_owned())
        .spawn(move || {
            if let Err(err) = compress(&plain) {
                log::warn!("Failed to gzip uio trace: {err}");
            }
        })?;
    Ok(())
}

fn compress(plain: &Path) -> io::Result<()> {
    let file = File::create(plain.with_added_extension("gz"))?;
    let mut encoder = flate2::write::GzEncoder::new(file, flate2::Compression::default());
    io::copy(&mut File::open(plain)?, &mut encoder)?;
    encoder.finish()?;
    fs::remove_file(plain)
}

fn copy_logs(inner: &Inner, output: &mut impl Write) -> io::Result<()> {
    let (current, len, logs) = {
        let current = inner.log.lock();
        let logs = inner.dir.as_deref().map(list_logs).transpose()?;
        (current.path.clone(), current.len, logs)
    };
    let Some((dir, logs)) = inner.dir.as_deref().zip(logs) else {
        io::copy(&mut gzip(File::open(&current)?.take(len)), output)?;
        return Ok(());
    };
    for timestamp in logs.into_keys() {
        let log = log_path(dir, timestamp, false);
        let len = if log == current { len } else { u64::MAX };
        match copy_log(&log, len, output) {
            // Retention got there first.
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            result => result?,
        }
    }
    Ok(())
}

/// The plain file gzipped on the fly, or the gzipped one once it's there.
fn copy_log(plain: &Path, len: u64, output: &mut impl Write) -> io::Result<()> {
    match File::open(plain) {
        Ok(log) => io::copy(&mut gzip(log.take(len)), output)?,
        // `compress` removes the plain one only after the gzipped one is complete.
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            io::copy(&mut File::open(plain.with_added_extension("gz"))?, output)?
        }
        Err(err) => return Err(err),
    };
    Ok(())
}

/// Files by timestamp, oldest first; `true` if not gzipped yet.
fn list_logs(dir: &Path) -> io::Result<BTreeMap<Timestamp, bool>> {
    let mut logs = BTreeMap::new();
    for entry in fs::read_dir(dir)? {
        let name = entry?.file_name();
        let Some(name) = name.to_str().and_then(|name| name.strip_prefix("trace.")) else {
            continue;
        };
        let (name, plain) = match name.strip_suffix(".gz") {
            Some(name) => (name, false),
            None => (name, true),
        };
        if let Some(Ok(timestamp)) = name.strip_suffix(".jsonl").map(str::parse) {
            *logs.entry(timestamp).or_default() |= plain;
        }
    }
    Ok(logs)
}

fn log_path(dir: &Path, timestamp: Timestamp, gzipped: bool) -> PathBuf {
    let gz = if gzipped { ".gz" } else { "" };
    dir.join(format!("trace.{timestamp}.jsonl{gz}"))
}

fn gzip<R: Read>(log: R) -> GzEncoder<R> {
    GzEncoder::new(log, flate2::Compression::fast())
}

#[cfg(test)]
mod tests {
    use std::pin::pin;

    use flate2::read::MultiGzDecoder;
    use futures::TryStreamExt as _;
    use serde_json::json;

    use super::*;
    use crate::ambient::trace::testing::lines;

    impl Sink {
        pub(crate) fn current_path(&self) -> PathBuf {
            self.0.log.lock().path.clone()
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

    #[tokio::test]
    async fn read_all_logs_oldest_first() {
        let dir = tempfile::tempdir().unwrap();
        let mut oldest = Vec::new();
        gzip(&b"oldest\n"[..]).read_to_end(&mut oldest).unwrap();
        fs::write(dir.path().join("trace.1.jsonl.gz"), &oldest).unwrap();
        fs::write(dir.path().join("trace.2.jsonl"), b"older\n").unwrap();

        let sink = Sink::dir(dir.path().to_owned()).unwrap();
        sink.start().unwrap();
        sink.send(mark());
        sink.stop();
        let read = sink.read().map_ok(Vec::from).try_concat().await.unwrap();

        // Already gzipped: as is.
        assert!(read.starts_with(&oldest));
        let mut decoded = String::new();
        MultiGzDecoder::new(&read[..])
            .read_to_string(&mut decoded)
            .unwrap();
        let current = fs::read_to_string(sink.current_path()).unwrap();
        assert_eq!(decoded, format!("oldest\nolder\n{current}"));
    }

    #[tokio::test]
    async fn read_reports_open_errors() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let sink = Sink::file(file.path().to_owned()).unwrap();
        fs::remove_file(sink.current_path()).unwrap();

        let err = sink
            .read()
            .map_ok(Vec::from)
            .try_concat()
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn read_reports_copy_errors_after_partial_output() {
        let dir = tempfile::tempdir().unwrap();
        let sink = Sink::dir(dir.path().to_owned()).unwrap();
        fs::write(dir.path().join("trace.1.jsonl.gz"), b"first log").unwrap();
        fs::create_dir(dir.path().join("trace.2.jsonl.gz")).unwrap();

        let mut read = pin!(sink.read());
        assert_eq!(read.next().await.unwrap().unwrap(), &b"first log"[..]);
        let err = read.next().await.unwrap().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::IsADirectory);
        assert!(read.next().await.is_none());
    }

    #[tokio::test]
    async fn read_does_not_block_writes() {
        let dir = tempfile::tempdir().unwrap();
        let big = vec![0; 4 * READ_BUFFER];
        fs::write(dir.path().join("trace.1.jsonl.gz"), big).unwrap();

        let sink = Sink::dir(dir.path().to_owned()).unwrap();
        let mut read = pin!(sink.read());
        read.next().await.unwrap().unwrap(); // mid-stream
        sink.start().unwrap();
        sink.send(mark());
        sink.stop();
        assert_eq!(
            json!(lines(&sink)),
            json!([{"kind": "mark", "parent": 0, "timestamp": "*", "text": "x"}])
        );
    }

    #[test]
    fn copy_log_falls_back_to_gzipped() {
        let dir = tempfile::tempdir().unwrap();
        let plain = dir.path().join("trace.1.jsonl");
        fs::write(plain.with_added_extension("gz"), b"gzipped").unwrap();
        let mut output = Vec::new();
        copy_log(&plain, u64::MAX, &mut output).unwrap();
        assert_eq!(output, b"gzipped");
    }

    #[test]
    fn writer_rotates_past_max_file_size() {
        let dir = tempfile::tempdir().unwrap();
        let sink = Sink::dir(dir.path().to_owned()).unwrap();
        sink.0.log.lock().len = MAX_FILE_SIZE - 1;
        sink.start().unwrap();
        sink.send(mark());
        sink.stop();
        assert_eq!(list_logs(dir.path()).unwrap().len(), 2);
        assert!(lines(&sink).is_empty());
    }

    #[tokio::test]
    async fn rotate_keeps_max_files_and_gzips() {
        let dir = tempfile::tempdir().unwrap();
        let sink = Sink::dir(dir.path().to_owned()).unwrap();
        for i in 0..MAX_FILES + 2 {
            {
                let mut current = sink.0.log.lock();
                writeln!(current.file, "{i}").unwrap();
                current.len += 2;
                rotate(dir.path(), &mut current).unwrap();
            }
            // Retention skips files still being gzipped.
            while list_logs(dir.path())
                .unwrap()
                .values()
                .filter(|&&plain| plain)
                .count()
                > 1
            {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        }
        assert_eq!(list_logs(dir.path()).unwrap().len(), MAX_FILES);

        let read = sink.read().map_ok(Vec::from).try_concat().await.unwrap();
        let mut decoded = String::new();
        MultiGzDecoder::new(&read[..])
            .read_to_string(&mut decoded)
            .unwrap();
        assert_eq!(decoded, "3\n4\n5\n6\n7\n8\n9\n10\n11\n");
    }

    fn mark() -> Event {
        Event::Mark {
            parent: 0,
            timestamp: now(),
            text: "x".into(),
        }
    }
}
