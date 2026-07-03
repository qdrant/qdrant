#![cfg(test)]

use std::future::Future;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use common::generic_consts::Sequential;
use common::universal_io::{
    DiskCacheConfig, DiskCacheFs, DiskCacheFsContext, OpenOptions, OwnedPipeline, Populate,
    ReadRange, Result, UniversalIoError, UniversalKind, UniversalRead, UniversalReadFileOps,
    UniversalReadFs,
};
use futures::stream::{BoxStream, StreamExt};

use crate::read::AsyncRead;
use crate::{BlobFile, BridgeRuntime};

/// Request counters shared with every clone of a [`CountingSource`], so a test
/// can assert how many remote requests of each kind a read path issued.
#[derive(Clone, Default)]
struct Counters {
    len: Arc<AtomicUsize>,
    whole: Arc<AtomicUsize>,
    range: Arc<AtomicUsize>,
    /// Open-ended tail GETs: `read_from` with a positive offset.
    from: Arc<AtomicUsize>,
}

#[derive(Clone)]
struct CountingConfig {
    data: Bytes,
    counters: Counters,
}

/// Test [`AsyncRead`] backend serving a fixed blob and counting the remote
/// requests it receives: `len` (HEAD), `read_whole` (single GET) and
/// `read_range` (ranged GET).
#[derive(Clone)]
struct CountingSource {
    data: Bytes,
    counters: Counters,
}

impl CountingSource {
    fn new(data: &'static [u8]) -> Self {
        Self {
            data: Bytes::from_static(data),
            counters: Counters::default(),
        }
    }

    fn config(&self) -> CountingConfig {
        CountingConfig {
            data: self.data.clone(),
            counters: self.counters.clone(),
        }
    }
}

impl AsyncRead for CountingSource {
    type Config = CountingConfig;

    fn open(config: &Self::Config) -> Result<Self> {
        Ok(Self {
            data: config.data.clone(),
            counters: config.counters.clone(),
        })
    }

    fn list_files(
        &self,
        _prefix: &Path,
    ) -> impl Future<Output = Result<Vec<(PathBuf, u64)>>> + Send + 'static {
        std::future::ready(Ok(vec![]))
    }

    fn exists(&self, _path: &Path) -> impl Future<Output = Result<bool>> + Send + 'static {
        std::future::ready(Ok(true))
    }

    fn create(&self, _path: &Path) -> impl Future<Output = Result<()>> + Send + 'static {
        std::future::ready(Ok(()))
    }

    fn remove(&self, _path: &Path) -> impl Future<Output = Result<()>> + Send + 'static {
        std::future::ready(Ok(()))
    }

    fn remove_dir(&self, _path: &Path) -> impl Future<Output = Result<()>> + Send + 'static {
        std::future::ready(Ok(()))
    }

    fn atomic_save(
        &self,
        _path: &Path,
        _bytes: Bytes,
    ) -> impl Future<Output = Result<()>> + Send + 'static {
        std::future::ready(Ok(()))
    }

    fn read_range(
        &self,
        _path: &Path,
        range: Range<u64>,
    ) -> impl Future<Output = Result<BoxStream<'static, Result<Bytes>>>> + Send + 'static {
        self.counters.range.fetch_add(1, Ordering::Relaxed);
        let bytes = self.data.slice(range.start as usize..range.end as usize);
        async move { Ok(futures::stream::once(async move { Ok(bytes) }).boxed()) }
    }

    fn read_from(
        &self,
        _path: &Path,
        from: u64,
    ) -> impl Future<Output = Result<(u64, BoxStream<'static, Result<Bytes>>)>> + Send + 'static
    {
        if from == 0 {
            self.counters.whole.fetch_add(1, Ordering::Relaxed);
        } else {
            self.counters.from.fetch_add(1, Ordering::Relaxed);
        }
        let size = self.data.len() as u64;
        let data = self.data.clone();
        async move {
            // A positive offset at or past EOF is an unsatisfiable range; mimic
            // the backend's 416 rather than yielding an empty body, so the
            // pipeline's empty-tail disambiguation is exercised.
            if from > 0 && from >= size {
                return Err(UniversalIoError::S3Config {
                    description: "requested range not satisfiable".into(),
                });
            }
            let tail = data.slice(from as usize..);
            Ok((size, futures::stream::once(async move { Ok(tail) }).boxed()))
        }
    }

    fn len(&self, _path: &Path) -> impl Future<Output = Result<u64>> + Send + 'static {
        self.counters.len.fetch_add(1, Ordering::Relaxed);
        let len = self.data.len() as u64;
        async move { Ok(len) }
    }

    fn kind() -> UniversalKind {
        UniversalKind::S3
    }
}

const DATA: &[u8] = b"the quick brown fox jumps over the lazy dog";

#[test]
fn blob_file_read_whole_uses_single_get_without_head() {
    let source = CountingSource::new(DATA);
    let counters = source.counters.clone();
    let file = BlobFile::new(source, BridgeRuntime::global(), "obj");

    let bytes = file.read_whole::<u8>().expect("read_whole");

    assert_eq!(&bytes[..], DATA);
    assert_eq!(
        counters.whole.load(Ordering::Relaxed),
        1,
        "read_whole should issue exactly one whole-object GET",
    );
    assert_eq!(
        counters.len.load(Ordering::Relaxed),
        0,
        "read_whole must not issue a separate len/HEAD request",
    );
    assert_eq!(counters.range.load(Ordering::Relaxed), 0);
}

#[test]
fn owned_pipeline_tail_read_uses_single_get_without_head() {
    let source = CountingSource::new(DATA);
    let counters = source.counters.clone();
    let file = BlobFile::new(source, BridgeRuntime::global(), "obj");

    let mut pipeline = OwnedPipeline::new(file).unwrap();
    let from = 10u64;
    pipeline.schedule_whole((), from).unwrap();
    let (_, bytes) = pipeline.wait().unwrap().expect("exactly one read");

    assert_eq!(&bytes[..], &DATA[from as usize..]);
    assert_eq!(
        counters.from.load(Ordering::Relaxed),
        1,
        "a tail read should issue exactly one open-ended GET",
    );
    assert_eq!(
        counters.len.load(Ordering::Relaxed),
        0,
        "a tail read must not issue a separate len/HEAD request",
    );
    assert_eq!(counters.whole.load(Ordering::Relaxed), 0);
    assert_eq!(counters.range.load(Ordering::Relaxed), 0);
}

#[test]
fn owned_pipeline_empty_tail_resolves_to_empty_read() {
    let source = CountingSource::new(DATA);
    let counters = source.counters.clone();
    let file = BlobFile::new(source, BridgeRuntime::global(), "obj");

    let mut pipeline = OwnedPipeline::new(file).unwrap();
    // Offset exactly at EOF: there is no tail to read.
    pipeline.schedule_whole((), DATA.len() as u64).unwrap();
    let (_, bytes) = pipeline
        .wait()
        .unwrap()
        .expect("an (empty) read is scheduled");

    assert!(bytes.is_empty(), "an offset at EOF yields an empty read");
    // The open-ended GET is attempted once, then a single len() confirms the
    // offset is past EOF — there is no cheaper way to learn the tail is empty.
    assert_eq!(counters.from.load(Ordering::Relaxed), 1);
    assert_eq!(counters.len.load(Ordering::Relaxed), 1);
    assert_eq!(counters.whole.load(Ordering::Relaxed), 0);
    assert_eq!(counters.range.load(Ordering::Relaxed), 0);
}

#[test]
fn disk_cache_read_whole_skips_remote_len() {
    let tmp = tempfile::Builder::new()
        .prefix("uio_whole_read")
        .tempdir()
        .unwrap();
    let local_dir = tmp.path().to_path_buf();

    let source = CountingSource::new(DATA);
    let counters = source.counters.clone();

    let config = DiskCacheConfig::new(PathBuf::from("bucket"), local_dir).unwrap();
    let fs = DiskCacheFs::<BlobFile<CountingSource>>::from_context(DiskCacheFsContext {
        config: Arc::new(config),
        remote: source.config(),
    })
    .unwrap();

    let file = fs
        .open(
            Path::new("bucket/data.bin"),
            OpenOptions {
                writeable: false,
                populate: Populate::No,
                ..OpenOptions::new_for_test()
            },
            (),
        )
        .unwrap();

    let bytes = file.read_whole::<u8>().expect("read_whole");
    assert_eq!(&bytes[..], DATA);
    assert_eq!(
        counters.whole.load(Ordering::Relaxed),
        1,
        "disk cache read_whole should issue a single whole-object GET",
    );
    assert_eq!(
        counters.len.load(Ordering::Relaxed),
        0,
        "disk cache read_whole must not HEAD the remote for its length",
    );

    let again = file
        .read::<Sequential, u8>(ReadRange::new(0, DATA.len() as u64))
        .expect("local read");
    assert_eq!(&again[..], DATA);
    assert_eq!(counters.whole.load(Ordering::Relaxed), 1);
    assert_eq!(counters.range.load(Ordering::Relaxed), 0);
    assert_eq!(counters.len.load(Ordering::Relaxed), 0);
}

#[test]
fn disk_cache_prefill_open_uses_whole_get_without_head() {
    let tmp = tempfile::Builder::new()
        .prefix("uio_whole_read")
        .tempdir()
        .unwrap();
    let local_dir = tmp.path().to_path_buf();

    let source = CountingSource::new(DATA);
    let counters = source.counters.clone();

    let config = DiskCacheConfig::new(PathBuf::from("bucket"), local_dir).unwrap();
    let fs = DiskCacheFs::<BlobFile<CountingSource>>::from_context(DiskCacheFsContext {
        config: Arc::new(config),
        remote: source.config(),
    })
    .unwrap();

    let file = fs
        .open(
            Path::new("bucket/data.bin"),
            OpenOptions {
                writeable: false,
                populate: Populate::Blocking,
                ..OpenOptions::new_for_test()
            },
            (),
        )
        .unwrap();

    assert_eq!(
        counters.whole.load(Ordering::Relaxed),
        1,
        "prefill should issue a single whole-object GET",
    );
    assert_eq!(
        counters.len.load(Ordering::Relaxed),
        0,
        "prefill must not HEAD the remote for its length",
    );
    assert_eq!(counters.range.load(Ordering::Relaxed), 0);

    let bytes = file.read_whole::<u8>().expect("read_whole");
    assert_eq!(&bytes[..], DATA);
    assert_eq!(counters.whole.load(Ordering::Relaxed), 1);
    assert_eq!(counters.len.load(Ordering::Relaxed), 0);
}
