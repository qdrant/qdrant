#![cfg(test)]

use std::future::Future;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use common::universal_io::{ListedFile, UioResult, UniversalIoError, UniversalKind};
use futures::stream::BoxStream;

use crate::read::{AsyncRead, OffsetByteStream};
use crate::write::AsyncWrite;
use crate::{BlobFs, BridgeRuntime};

/// Backend whose saves need a Tokio reactor: each one parks on a timer, which
/// only a Tokio context can drive.
#[derive(Clone, Default)]
struct TimerWriteSource {
    saves: Arc<AtomicUsize>,
}

impl AsyncRead for TimerWriteSource {
    type Config = ();

    fn open(_config: &()) -> UioResult<Self> {
        Ok(Self::default())
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
        let saves = self.saves.clone();
        async move {
            tokio::time::sleep(Duration::from_millis(1)).await;
            saves.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
}

fn blob_fs() -> (BlobFs<TimerWriteSource>, Arc<AtomicUsize>, BridgeRuntime) {
    let source = TimerWriteSource::default();
    let saves = source.saves.clone();
    let runtime = BridgeRuntime::new().expect("new runtime");
    (BlobFs::new(source, runtime.clone()), saves, runtime)
}

/// The reason the write futures are spawned: a caller with no runtime of its
/// own drives them on a bare executor, and the reactor-bound work still runs.
#[test]
fn a_save_resolves_on_a_bare_executor() {
    let (fs, saves, _runtime) = blob_fs();

    futures::executor::block_on(fs.save_async("object".into(), vec![1, 2, 3]))
        .expect("the save resolves without an ambient runtime");

    assert_eq!(saves.load(Ordering::SeqCst), 1);
}

#[test]
fn a_remove_resolves_on_a_bare_executor() {
    let (fs, _saves, _runtime) = blob_fs();

    futures::executor::block_on(fs.remove_async("object".into()))
        .expect("the remove resolves without an ambient runtime");
}

/// Nothing runs before the first poll, so a collected wave stays a plan.
#[test]
fn a_save_does_not_start_until_polled() {
    let (fs, saves, _runtime) = blob_fs();

    let save = fs.save_async("object".into(), vec![1]);
    std::thread::sleep(Duration::from_millis(20));
    assert_eq!(saves.load(Ordering::SeqCst), 0);

    futures::executor::block_on(save).expect("the save lands once polled");
    assert_eq!(saves.load(Ordering::SeqCst), 1);
}
