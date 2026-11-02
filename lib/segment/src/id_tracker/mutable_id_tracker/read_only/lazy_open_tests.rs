use std::ops::Range;
use std::path::Path;

use bytes::Bytes;
use common::ambient;
use common::universal_io::{
    ListedFile, MmapFs, UioResult, UniversalIoError, UniversalKind, UniversalReadFs,
};
use fs_err as fs;
use futures::executor::block_on;
use futures::stream::{BoxStream, StreamExt};
use io_bridge_object_store::{AsyncRead, BlobFile, BlobFs, BridgeRuntime};
use tempfile::Builder;

use super::{ReadOnlyAppendableIdTracker, TrackerProbe};
use crate::id_tracker::mutable_id_tracker::MutableIdTracker;
use crate::id_tracker::{IdTracker, IdTrackerRead};

/// Local files behind `BlobFile`, which, like an object store, opens without touching the file,
/// so a missing one surfaces as `NotFound` only from `len` or a read.
#[derive(Clone)]
struct LazyLocalSource;

fn read_local(path: &Path) -> UioResult<Bytes> {
    fs::read(path)
        .map(Bytes::from)
        .map_err(|err| UniversalIoError::extract_not_found(err, path))
}

impl AsyncRead for LazyLocalSource {
    type Config = ();

    fn open(_config: &()) -> UioResult<Self> {
        Ok(Self)
    }

    fn list_files(
        &self,
        prefix: &Path,
    ) -> impl Future<Output = UioResult<Vec<ListedFile>>> + Send + 'static {
        std::future::ready(MmapFs.list_files(prefix))
    }

    fn exists(&self, path: &Path) -> impl Future<Output = UioResult<bool>> + Send + 'static {
        std::future::ready(MmapFs.exists(path))
    }

    fn read_range(
        &self,
        path: &Path,
        range: Range<u64>,
    ) -> impl Future<Output = UioResult<BoxStream<'static, UioResult<Bytes>>>> + Send + 'static
    {
        let bytes =
            read_local(path).map(|bytes| bytes.slice(range.start as usize..range.end as usize));
        std::future::ready(bytes.map(|bytes| futures::stream::iter([Ok(bytes)]).boxed()))
    }

    fn read_from(
        &self,
        path: &Path,
        from: u64,
    ) -> impl Future<Output = UioResult<(u64, BoxStream<'static, UioResult<(u64, Bytes)>>)>>
    + Send
    + 'static {
        let tail = read_local(path).map(|bytes| {
            let total = bytes.len() as u64;
            let tail = bytes.slice(from as usize..);
            (total, futures::stream::iter([Ok((0, tail))]).boxed())
        });
        std::future::ready(tail)
    }

    fn len(&self, path: &Path) -> impl Future<Output = UioResult<u64>> + Send + 'static {
        let len = fs::metadata(path)
            .map(|meta| meta.len())
            .map_err(|err| UniversalIoError::extract_not_found(err, path));
        std::future::ready(len)
    }

    fn kind() -> UniversalKind {
        UniversalKind::S3
    }
}

#[test]
fn test_probe_treats_lazily_opened_absent_files_as_empty() {
    let _scope = ambient::test_guard();
    let segment_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let blob_fs = BlobFs::new(LazyLocalSource, BridgeRuntime::global());

    // An empty appendable segment has no tracker files, yet the open keeps a handle to each.
    let mut read_only = ReadOnlyAppendableIdTracker::<BlobFile<LazyLocalSource>>::open(
        &blob_fs,
        segment_dir.path(),
        None,
    )
    .unwrap();

    for _ in 0..2 {
        let probe = block_on(read_only.probe_committed(&blob_fs)).unwrap();
        assert_eq!(
            probe,
            TrackerProbe::Unchanged {
                max_committed_id: 0
            },
        );
    }

    let mut mutable = MutableIdTracker::open(segment_dir.path(), None).unwrap();
    mutable.set_link(100.into(), 0).unwrap();
    mutable.set_internal_version(0, 10).unwrap();
    mutable.mapping_flusher()().unwrap();
    mutable.versions_flusher()().unwrap();

    let probe = block_on(read_only.probe_committed(&blob_fs)).unwrap();
    assert_eq!(
        probe,
        TrackerProbe::Changed {
            max_committed_id: 1
        },
    );

    read_only
        .live_reload(&blob_fs, probe.max_committed_id())
        .unwrap();
    read_only.publish_staged();
    assert_eq!(read_only.available_point_count(), 1);
}
