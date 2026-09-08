#![cfg(test)]

use std::future::Future;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::universal_io::{ListedFile, UioResult, UniversalIoError, UniversalKind};
use futures::stream::BoxStream;
use tokio::sync::Semaphore;

use crate::read::{AsyncRead, OffsetByteStream};
use crate::write::AsyncWrite;
use crate::{BlobFs, BridgeRuntime};

#[derive(Clone)]
struct Observed {
    in_flight: Arc<AtomicUsize>,
    peak_in_flight: Arc<AtomicUsize>,
    /// Saves park here once counted, until the test lets them through.
    gate: Arc<Semaphore>,
}

/// Backend whose saves need a Tokio reactor: each one parks on a timer first,
/// which only a Tokio context can drive.
#[derive(Clone)]
struct TimerWriteSource {
    observed: Observed,
}

impl TimerWriteSource {
    fn new(gate_permits: usize) -> Self {
        Self {
            observed: Observed {
                in_flight: Arc::default(),
                peak_in_flight: Arc::default(),
                gate: Arc::new(Semaphore::new(gate_permits)),
            },
        }
    }
}

impl AsyncRead for TimerWriteSource {
    type Config = usize;

    fn open(gate_permits: &usize) -> UioResult<Self> {
        Ok(Self::new(*gate_permits))
    }

    fn list_files(
        &self,
        _prefix: &Path,
    ) -> impl Future<Output = UioResult<Vec<ListedFile>>> + Send + 'static {
        std::future::ready(Ok(Vec::new()))
    }

    fn exists(&self, _path: &Path) -> impl Future<Output = UioResult<bool>> + Send + 'static {
        std::future::ready(Ok(false))
    }

    fn read_range(
        &self,
        path: &Path,
        _range: Range<u64>,
    ) -> impl Future<Output = UioResult<BoxStream<'static, UioResult<Bytes>>>> + Send + 'static
    {
        std::future::ready(Err(UniversalIoError::NotFound { path: path.into() }))
    }

    fn read_from(
        &self,
        path: &Path,
        _from: u64,
    ) -> impl Future<Output = UioResult<(u64, OffsetByteStream)>> + Send + 'static {
        std::future::ready(Err(UniversalIoError::NotFound { path: path.into() }))
    }

    fn len(&self, path: &Path) -> impl Future<Output = UioResult<u64>> + Send + 'static {
        std::future::ready(Err(UniversalIoError::NotFound { path: path.into() }))
    }

    fn kind() -> UniversalKind {
        UniversalKind::S3
    }
}

impl AsyncWrite for TimerWriteSource {
    fn create(&self, _path: &Path) -> impl Future<Output = UioResult<()>> + Send + 'static {
        std::future::ready(Ok(()))
    }

    fn remove(&self, _path: &Path) -> impl Future<Output = UioResult<()>> + Send + 'static {
        std::future::ready(Ok(()))
    }

    fn save(
        &self,
        _path: &Path,
        _bytes: Bytes,
    ) -> impl Future<Output = UioResult<()>> + Send + 'static {
        let observed = self.observed.clone();
        async move {
            tokio::time::sleep(Duration::from_millis(1)).await;
            let now = observed.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            observed.peak_in_flight.fetch_max(now, Ordering::SeqCst);
            let _through = observed.gate.acquire().await;
            observed.in_flight.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        }
    }
}

fn blob_fs(gate_permits: usize) -> (BlobFs<TimerWriteSource>, Observed, BridgeRuntime) {
    let source = TimerWriteSource::new(gate_permits);
    let observed = source.observed.clone();
    let runtime = BridgeRuntime::new().expect("new runtime");
    (BlobFs::new(source, runtime.clone()), observed, runtime)
}

fn wait_until(what: &str, condition: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting until {what}");
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// The reason the write futures are spawned: a caller with no runtime of its
/// own drives them on a bare executor, and the reactor-bound work still runs.
#[test]
fn a_save_resolves_on_a_bare_executor() {
    let (fs, _observed, _runtime) = blob_fs(Semaphore::MAX_PERMITS);

    futures::executor::block_on(fs.save_async("object".into(), vec![1, 2, 3]))
        .expect("the save resolves without an ambient runtime");
}

#[test]
fn a_remove_resolves_on_a_bare_executor() {
    let (fs, _observed, _runtime) = blob_fs(Semaphore::MAX_PERMITS);

    futures::executor::block_on(fs.remove_async("object".into()))
        .expect("the remove resolves without an ambient runtime");
}

/// Nothing runs before the first poll, so a collected wave stays a plan.
#[test]
fn a_save_does_not_start_until_polled() {
    let (fs, observed, _runtime) = blob_fs(Semaphore::MAX_PERMITS);

    let save = fs.save_async("object".into(), vec![1]);
    std::thread::sleep(Duration::from_millis(20));
    assert_eq!(observed.peak_in_flight.load(Ordering::SeqCst), 0);

    futures::executor::block_on(save).expect("the save lands once polled");
    assert_eq!(observed.peak_in_flight.load(Ordering::SeqCst), 1);
}

/// The backend caps its own in-flight writes, so callers cannot exceed the
/// depth by launching a wider wave.
#[test]
fn the_backend_caps_writes_in_flight() {
    let (fs, observed, runtime) = blob_fs(0);
    let depth = runtime.max_concurrent_writes();
    let saves: Vec<_> = (0..depth * 3)
        .map(|i| fs.save_async(format!("object-{i}").into(), vec![i as u8]))
        .collect();

    let wave =
        std::thread::spawn(move || futures::executor::block_on(futures::future::join_all(saves)));

    // Nothing completes while the gate is shut, so every admitted save stays
    // counted: the peak is exactly what the permits let through.
    let in_flight = observed.in_flight.clone();
    wait_until("the permitted saves are parked at the gate", || {
        in_flight.load(Ordering::SeqCst) >= depth
    });
    observed.gate.add_permits(depth * 3);

    for result in wave.join().expect("the wave thread completes") {
        result.expect("every save lands");
    }
    assert_eq!(
        observed.peak_in_flight.load(Ordering::SeqCst),
        depth,
        "the wave fills the backend's depth and never exceeds it"
    );
}
