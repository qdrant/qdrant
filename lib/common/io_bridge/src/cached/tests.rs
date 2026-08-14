//! Tests for [`CachedBlobFile`] appends through the caller-side rewrite
//! path, on a mock backend without native small appends.

use std::assert_matches;
use std::future::Future;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use common::universal_io::{
    DiskCacheConfig, ListedFile, OpenOptions, Populate, UniversalWriteFileOps as _,
};
use futures::stream::{BoxStream, StreamExt as _};

use super::*;
use crate::runtime::BridgeRuntime;
use crate::write::AsyncWrite;
use crate::{CachedBlobFs, OffsetByteStream};

/// An in-memory single-object backend shaped like an S3 store without
/// native append (part-copy strategy): direct appends only above the
/// 5 MiB threshold, so every small append goes through the caller-side
/// rewrite.
#[derive(Clone, Default)]
struct ThresholdMockSource {
    store: Arc<Mutex<Option<Vec<u8>>>>,
    direct_appends: Arc<AtomicUsize>,
}

impl ThresholdMockSource {
    fn content(&self) -> Option<Vec<u8>> {
        self.store.lock().unwrap().clone()
    }
}

impl AsyncRead for ThresholdMockSource {
    type Config = ();

    fn open(_config: &()) -> UioResult<Self> {
        Ok(Self::default())
    }

    fn list_files(
        &self,
        _prefix: &Path,
    ) -> impl Future<Output = UioResult<Vec<ListedFile>>> + Send + 'static {
        std::future::ready(Ok(vec![]))
    }

    fn exists(&self, _path: &Path) -> impl Future<Output = UioResult<bool>> + Send + 'static {
        std::future::ready(Ok(self.store.lock().unwrap().is_some()))
    }

    fn read_range(
        &self,
        path: &Path,
        range: Range<u64>,
    ) -> impl Future<Output = UioResult<BoxStream<'static, UioResult<Bytes>>>> + Send + 'static
    {
        let result = match &*self.store.lock().unwrap() {
            Some(data) => {
                let end = (range.end as usize).min(data.len());
                let start = (range.start as usize).min(end);
                Ok(Bytes::copy_from_slice(&data[start..end]))
            }
            None => Err(UniversalIoError::NotFound { path: path.into() }),
        };
        async move {
            let bytes = result?;
            Ok(futures::stream::once(async move { Ok(bytes) }).boxed())
        }
    }

    fn read_from(
        &self,
        path: &Path,
        from: u64,
    ) -> impl Future<Output = UioResult<(u64, OffsetByteStream)>> + Send + 'static {
        let result = match &*self.store.lock().unwrap() {
            Some(data) => Ok((
                data.len() as u64,
                Bytes::copy_from_slice(&data[from as usize..]),
            )),
            None => Err(UniversalIoError::NotFound { path: path.into() }),
        };
        async move {
            let (size, tail) = result?;
            Ok((
                size,
                futures::stream::once(async move { Ok((0, tail)) }).boxed(),
            ))
        }
    }

    fn len(&self, path: &Path) -> impl Future<Output = UioResult<u64>> + Send + 'static {
        let result = match &*self.store.lock().unwrap() {
            Some(data) => Ok(data.len() as u64),
            None => Err(UniversalIoError::NotFound { path: path.into() }),
        };
        std::future::ready(result)
    }

    fn kind() -> UniversalKind {
        UniversalKind::S3
    }
}

impl AsyncWrite for ThresholdMockSource {
    fn create(&self, _path: &Path) -> impl Future<Output = UioResult<()>> + Send + 'static {
        self.store.lock().unwrap().get_or_insert_with(Vec::new);
        std::future::ready(Ok(()))
    }

    fn remove(&self, path: &Path) -> impl Future<Output = UioResult<()>> + Send + 'static {
        let result = match self.store.lock().unwrap().take() {
            Some(_) => Ok(()),
            None => Err(UniversalIoError::NotFound { path: path.into() }),
        };
        std::future::ready(result)
    }

    fn save(
        &self,
        _path: &Path,
        bytes: Bytes,
    ) -> impl Future<Output = UioResult<()>> + Send + 'static {
        *self.store.lock().unwrap() = Some(bytes.to_vec());
        std::future::ready(Ok(()))
    }
}

impl AsyncAppend for ThresholdMockSource {
    fn append_support(&self) -> AppendSupport {
        AppendSupport::AboveThreshold {
            min_offset: 5 * 1024 * 1024,
        }
    }

    fn append(
        &self,
        _path: &Path,
        _offset: u64,
        _data: Bytes,
        _expected_etag: Option<String>,
    ) -> impl Future<Output = UioResult<u64>> + Send + 'static {
        self.direct_appends.fetch_add(1, Ordering::Relaxed);
        std::future::ready(Err(UniversalIoError::S3Config {
            description: "direct append below the advertised threshold".to_string(),
        }))
    }
}

fn cached_fs(source: &ThresholdMockSource, local_dir: &Path) -> CachedBlobFs<ThresholdMockSource> {
    let config = DiskCacheConfig::new(PathBuf::from("bucket"), local_dir.to_path_buf())
        .expect("local dir exists");
    CachedBlobFs::new(source.clone(), BridgeRuntime::global(), Arc::new(config))
}

fn open_options() -> OpenOptions {
    OpenOptions {
        need_sequential: false,
        populate: Populate::No,
        ..OpenOptions::new_for_test()
    }
}

/// An append at offset 0 to an object that does not exist yet creates
/// it, like the direct-append backends do — instead of surfacing the
/// mirror's `NotFound` from the offset check.
#[test]
fn rewrite_append_at_offset_zero_creates_the_missing_object() {
    let tmp = tempfile::tempdir().unwrap();
    let source = ThresholdMockSource::default();
    let mut file = cached_fs(&source, tmp.path())
        .open_append("bucket/obj", open_options())
        .unwrap();

    file.append(0, b"abc".as_slice()).unwrap();
    assert_eq!(source.content().unwrap(), b"abc");

    file.append(3, b"de".as_slice()).unwrap();
    assert_eq!(source.content().unwrap(), b"abcde");
    assert_eq!(file.len::<u8>().unwrap(), 5);

    // Both appends are below the threshold: rewrites only.
    assert_eq!(source.direct_appends.load(Ordering::Relaxed), 0);
}

/// A non-zero offset against a missing object is an offset conflict —
/// the object's length is zero, not unknowable.
#[test]
fn rewrite_append_at_nonzero_offset_on_a_missing_object_conflicts() {
    let tmp = tempfile::tempdir().unwrap();
    let source = ThresholdMockSource::default();
    let mut file = cached_fs(&source, tmp.path())
        .open_append("bucket/obj", open_options())
        .unwrap();

    let err = file.append(3, b"de".as_slice()).unwrap_err();
    assert_matches!(
        err,
        UniversalIoError::AppendOffsetConflict { offset: 3, .. }
    );
    assert!(source.content().is_none());
}
