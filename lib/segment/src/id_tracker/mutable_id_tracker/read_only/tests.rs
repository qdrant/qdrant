use std::io::Write;

use common::types::PointOffsetType;
use common::universal_io::{MmapFile, MmapFs};
use fs_err as fs;
use tempfile::Builder;

use super::{LiveReloadResult, ReadOnlyAppendableIdTracker};
use crate::id_tracker::mutable_id_tracker::MutableIdTracker;
use crate::id_tracker::mutable_id_tracker::mappings_storage::mappings_path;
use crate::id_tracker::mutable_id_tracker::versions_storage::versions_path;
use crate::id_tracker::{IdTracker, IdTrackerRead};
use crate::types::{PointIdType, SeqNumberType};

type ReadOnlyTracker = ReadOnlyAppendableIdTracker<MmapFile>;

/// Flush both mapping and version changes of a mutable tracker to disk.
fn flush(tracker: &MutableIdTracker) {
    tracker.mapping_flusher()().unwrap();
    tracker.versions_flusher()().unwrap();
}

/// Insert a point with a version into the mutable tracker.
fn insert(
    tracker: &mut MutableIdTracker,
    external: PointIdType,
    internal: PointOffsetType,
    version: SeqNumberType,
) {
    tracker.set_link(external, internal).unwrap();
    tracker.set_internal_version(internal, version).unwrap();
}

/// Assert that the read-only tracker exposes the same live points and versions as the mutable one.
///
/// Note `total_point_count` is not compared: a point inserted and deleted before its version was
/// committed never enters the read-only mapping, while the mutable tracker keeps it as a deleted
/// slot, so the read-only total can legitimately be smaller. The live points must still match.
fn assert_in_sync(read_only: &ReadOnlyTracker, mutable: &MutableIdTracker) {
    assert_eq!(
        read_only.available_point_count(),
        mutable.available_point_count(),
    );
    for internal_id in 0..mutable.total_point_count() as PointOffsetType {
        assert_eq!(
            read_only.is_deleted_point(internal_id),
            mutable.is_deleted_point(internal_id),
            "deleted state mismatch at offset {internal_id}",
        );
        assert_eq!(
            read_only.external_id(internal_id),
            mutable.external_id(internal_id),
            "external id mismatch at offset {internal_id}",
        );
        // Versions are only meaningful for live points. A deleted point's version is considered
        // gone, the read-only tracker does not refresh it (it keeps whatever stale value it had).
        if !mutable.is_deleted_point(internal_id) {
            assert_eq!(
                read_only.internal_version(internal_id),
                mutable.internal_version(internal_id),
                "version mismatch at offset {internal_id}",
            );
        }
    }
}

#[test]
fn test_open_matches_mutable_tracker() {
    let segment_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let mut mutable = MutableIdTracker::open(segment_dir.path(), None).unwrap();
    insert(&mut mutable, 100.into(), 0, 10);
    insert(&mut mutable, 200.into(), 1, 11);
    insert(&mut mutable, 300.into(), 2, 12);
    mutable.drop(200.into()).unwrap();
    flush(&mutable);

    let read_only = ReadOnlyTracker::open(&MmapFs, segment_dir.path(), None).unwrap();
    assert_in_sync(&read_only, &mutable);

    // Spot check resolved ids
    assert_eq!(
        read_only
            .internal_id_with_behavior(100.into(), common::types::DeferredBehavior::VisibleOnly),
        Some(0)
    );
    assert_eq!(
        read_only
            .internal_id_with_behavior(200.into(), common::types::DeferredBehavior::VisibleOnly),
        None
    );
    assert_eq!(read_only.external_id(2), Some(300.into()));
}

#[test]
fn test_open_without_storage_is_empty() {
    let segment_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    // No storage exists yet: the files are absent while empty, so a read-only view opens as an
    // empty tracker (matching `MutableIdTracker::open`) rather than erroring.
    let read_only = ReadOnlyTracker::open(&MmapFs, segment_dir.path(), None).unwrap();
    assert_eq!(read_only.available_point_count(), 0);
    assert_eq!(read_only.total_point_count(), 0);
}

#[test]
fn test_open_without_storage_is_not_persisted() {
    let segment_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    // Opens empty, like the test above — but with nothing there to open.
    let read_only = ReadOnlyTracker::open(&MmapFs, segment_dir.path(), None).unwrap();
    assert!(!read_only.is_persisted());
}

#[test]
fn test_open_after_a_flush_is_persisted() {
    let segment_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let mut mutable = MutableIdTracker::open(segment_dir.path(), None).unwrap();
    insert(&mut mutable, 100.into(), 0, 10);
    flush(&mutable);

    let read_only = ReadOnlyTracker::open(&MmapFs, segment_dir.path(), None).unwrap();
    assert!(read_only.is_persisted());
}

#[test]
fn test_a_tracker_emptied_by_deletes_is_still_persisted() {
    let segment_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    // Emptied by deletes, so the counts match a segment that never had a tracker. The logs do not.
    let mut mutable = MutableIdTracker::open(segment_dir.path(), None).unwrap();
    insert(&mut mutable, 100.into(), 0, 10);
    mutable.drop(100.into()).unwrap();
    flush(&mutable);

    let read_only = ReadOnlyTracker::open(&MmapFs, segment_dir.path(), None).unwrap();
    assert_eq!(read_only.available_point_count(), 0);
    assert!(read_only.is_persisted());
}

#[test]
fn test_open_with_missing_versions_is_empty() {
    let segment_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    // Create only the mappings file by writing then removing the versions file.
    let mut mutable = MutableIdTracker::open(segment_dir.path(), None).unwrap();
    insert(&mut mutable, 100.into(), 0, 10);
    flush(&mutable);
    fs::remove_file(versions_path(segment_dir.path())).unwrap();

    // With no versions file no point is committed (a point becomes visible only once its version is
    // present), so the read-only view opens empty instead of erroring.
    let read_only = ReadOnlyTracker::open(&MmapFs, segment_dir.path(), None).unwrap();
    assert_eq!(read_only.available_point_count(), 0);
}

#[test]
fn test_live_reload_reports_inserts_and_deletes() {
    let segment_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    // Initial state: three points, persisted before opening the read-only view
    let mut mutable = MutableIdTracker::open(segment_dir.path(), None).unwrap();
    insert(&mut mutable, 100.into(), 0, 10);
    insert(&mut mutable, 200.into(), 1, 11);
    insert(&mut mutable, 300.into(), 2, 12);
    flush(&mutable);

    let mut read_only = ReadOnlyTracker::open(&MmapFs, segment_dir.path(), None).unwrap();
    assert_in_sync(&read_only, &mutable);

    // A reload with no new changes reports nothing
    assert_eq!(
        read_only.live_reload(&MmapFs).unwrap(),
        LiveReloadResult::default(),
    );

    // Append more changes: insert two new points, delete one existing point
    insert(&mut mutable, 400.into(), 3, 13);
    insert(&mut mutable, 500.into(), 4, 14);
    mutable.drop(200.into()).unwrap();
    flush(&mutable);

    let result = read_only.live_reload(&MmapFs).unwrap();
    assert_eq!(result.inserted, vec![3, 4]);
    assert_eq!(result.deleted, vec![1]);
    assert_in_sync(&read_only, &mutable);

    // The deleted point is gone; the live ones carry their versions.
    assert!(read_only.is_deleted_point(1));
    assert_eq!(read_only.internal_version(3), Some(13));
    assert_eq!(read_only.internal_version(4), Some(14));
}

/// A point that is inserted and then deleted within a single un-reloaded batch was never observed
/// as available, so it must be reported as neither inserted nor deleted.
#[test]
fn test_live_reload_insert_then_delete_within_batch() {
    let segment_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let mut mutable = MutableIdTracker::open(segment_dir.path(), None).unwrap();
    insert(&mut mutable, 100.into(), 0, 10);
    flush(&mutable);

    let mut read_only = ReadOnlyTracker::open(&MmapFs, segment_dir.path(), None).unwrap();

    // Insert a brand-new point and delete it again, all before the read-only view reloads.
    insert(&mut mutable, 200.into(), 1, 11);
    mutable.drop(200.into()).unwrap();
    flush(&mutable);

    let result = read_only.live_reload(&MmapFs).unwrap();
    assert_eq!(result.inserted, Vec::<PointOffsetType>::new());
    assert_eq!(result.deleted, Vec::<PointOffsetType>::new());
    assert_in_sync(&read_only, &mutable);

    // The pre-existing point is untouched.
    assert_eq!(
        read_only
            .internal_id_with_behavior(100.into(), common::types::DeferredBehavior::VisibleOnly),
        Some(0)
    );
    assert!(read_only.is_deleted_point(1));
}

/// Upserting an existing point re-links its external id to a new offset and marks the old offset
/// deleted (append-only storage). A reload must report the new offset as inserted and the old one
/// as deleted, and resolve the external id to the new offset.
#[test]
fn test_live_reload_upsert_relinks_to_new_offset() {
    let segment_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let mut mutable = MutableIdTracker::open(segment_dir.path(), None).unwrap();
    insert(&mut mutable, 100.into(), 0, 10);
    flush(&mutable);

    let mut read_only = ReadOnlyTracker::open(&MmapFs, segment_dir.path(), None).unwrap();
    assert_eq!(
        read_only
            .internal_id_with_behavior(100.into(), common::types::DeferredBehavior::VisibleOnly),
        Some(0)
    );

    // Upsert point 100: it moves to a new offset, the old offset is marked deleted.
    mutable.set_link(100.into(), 1).unwrap();
    mutable.set_internal_version(1, 20).unwrap();
    flush(&mutable);

    let result = read_only.live_reload(&MmapFs).unwrap();
    assert_eq!(result.inserted, vec![1]);
    assert_eq!(result.deleted, vec![0]);
    assert_eq!(
        read_only
            .internal_id_with_behavior(100.into(), common::types::DeferredBehavior::VisibleOnly),
        Some(1)
    );
    assert_eq!(read_only.internal_version(1), Some(20));
    assert!(read_only.is_deleted_point(0));
    assert_in_sync(&read_only, &mutable);
}

/// The writer flushes mappings before data before versions. A point whose mapping is flushed but
/// whose version is not yet on disk must NOT be reported as inserted (its data may be partially
/// written). It is reported on a later reload, once its version lands, even if no new mapping
/// changes arrive in between.
#[test]
fn test_live_reload_withholds_insert_until_version_present() {
    let segment_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    // Baseline point, fully flushed.
    let mut mutable = MutableIdTracker::open(segment_dir.path(), None).unwrap();
    insert(&mut mutable, 100.into(), 0, 10);
    flush(&mutable);

    let mut read_only = ReadOnlyTracker::open(&MmapFs, segment_dir.path(), None).unwrap();

    // Insert a point but flush *only* the mapping, mimicking observing storage mid-flush-cycle.
    insert(&mut mutable, 200.into(), 1, 11);
    mutable.mapping_flusher()().unwrap();

    // The version is not flushed yet, so the point is withheld from the result and, crucially, is
    // not present in the mapping at all (its data may be partially written).
    let result = read_only.live_reload(&MmapFs).unwrap();
    assert_eq!(result, LiveReloadResult::default());
    assert_eq!(
        read_only
            .internal_id_with_behavior(200.into(), common::types::DeferredBehavior::VisibleOnly),
        None
    );
    assert_eq!(read_only.internal_version(1), None);

    // The writer flushes the versions afterwards. A reload with no new mapping changes now reports
    // the insert, links it into the mapping, and reconciles the version.
    mutable.versions_flusher()().unwrap();

    let result = read_only.live_reload(&MmapFs).unwrap();
    assert_eq!(result.inserted, vec![1]);
    assert_eq!(result.deleted, Vec::<PointOffsetType>::new());
    assert_eq!(
        read_only
            .internal_id_with_behavior(200.into(), common::types::DeferredBehavior::VisibleOnly),
        Some(1)
    );
    assert_eq!(read_only.internal_version(1), Some(11));
    assert_in_sync(&read_only, &mutable);
}

/// A partially-written trailing mapping entry (e.g. a flush observed mid-append) must be ignored,
/// and the next live-reload must re-read from the start of that incomplete entry.
#[test]
fn test_live_reload_ignores_partial_trailing_mapping_entry() {
    let segment_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    // Two complete, flushed points
    let mut mutable = MutableIdTracker::open(segment_dir.path(), None).unwrap();
    insert(&mut mutable, 100.into(), 0, 10);
    insert(&mut mutable, 200.into(), 1, 11);
    flush(&mutable);

    let mut read_only = ReadOnlyTracker::open(&MmapFs, segment_dir.path(), None).unwrap();
    assert_in_sync(&read_only, &mutable);

    // Byte offset of the last fully-consumed mapping entry, the position we expect to resume from.
    let complete_len = read_only.mappings_read_to;
    assert!(complete_len > 0);

    // Append a torn `InsertNum` entry: the type byte plus only part of its payload. A complete
    // `InsertNum` entry is 1 + 8 + 4 = 13 bytes; we write just 5, simulating a half-flushed record.
    let mappings_file_path = mappings_path(segment_dir.path());
    {
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&mappings_file_path)
            .unwrap();
        file.write_all(&[1, 0, 0, 0, 0]).unwrap();
    }

    // The partial entry is ignored and we don't advance past it.
    let result = read_only.live_reload(&MmapFs).unwrap();
    assert_eq!(result, LiveReloadResult::default());
    assert_eq!(
        read_only.mappings_read_to, complete_len,
        "must not consume the partial trailing entry",
    );

    // The writer's next flush truncates the torn bytes (the file is longer than its persisted size)
    // and appends the real next entry, and a later live-reload resumes from the preserved position
    // and picks it up. That truncation is a `set_len`, which Windows forbids while the read-only
    // view keeps the file mapped, so the resume check only runs off Windows.
    #[cfg(not(target_os = "windows"))]
    {
        insert(&mut mutable, 300.into(), 2, 12);
        flush(&mutable);

        let result = read_only.live_reload(&MmapFs).unwrap();
        assert_eq!(result.inserted, vec![2]);
        assert_eq!(result.deleted, Vec::<PointOffsetType>::new());
        assert_in_sync(&read_only, &mutable);
    }
}

/// A partially-written trailing versions entry (a u64 that isn't fully flushed) must be ignored
/// rather than producing a corrupt version or an error.
#[test]
fn test_open_and_reload_ignore_partial_trailing_version() {
    let segment_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let mut mutable = MutableIdTracker::open(segment_dir.path(), None).unwrap();
    insert(&mut mutable, 100.into(), 0, 10);
    insert(&mut mutable, 200.into(), 1, 11);
    flush(&mutable);

    // Append a few stray bytes, fewer than a full 8-byte version entry.
    let versions_file_path = versions_path(segment_dir.path());
    {
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&versions_file_path)
            .unwrap();
        file.write_all(&[1, 2, 3]).unwrap();
    }

    // Open ignores the partial trailing bytes, versions match the complete entries.
    let read_only = ReadOnlyTracker::open(&MmapFs, segment_dir.path(), None).unwrap();
    assert_eq!(read_only.internal_version(0), Some(10));
    assert_eq!(read_only.internal_version(1), Some(11));
    assert_in_sync(&read_only, &mutable);
}

/// During live-reload a point whose version is only partially written must be treated like a point
/// with no version at all: withheld until the full version is flushed.
#[test]
fn test_live_reload_withholds_partially_written_version() {
    let segment_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let mut mutable = MutableIdTracker::open(segment_dir.path(), None).unwrap();
    insert(&mut mutable, 100.into(), 0, 10);
    insert(&mut mutable, 200.into(), 1, 11);
    flush(&mutable);

    let mut read_only = ReadOnlyTracker::open(&MmapFs, segment_dir.path(), None).unwrap();

    // Insert a third point and flush its mapping, then simulate a torn version flush by appending
    // only part of its 8-byte version entry.
    insert(&mut mutable, 300.into(), 2, 12);
    mutable.mapping_flusher()().unwrap();
    {
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(versions_path(segment_dir.path()))
            .unwrap();
        file.write_all(&[1, 2, 3]).unwrap();
    }

    // Only part of the version is written, so the point is withheld.
    let result = read_only.live_reload(&MmapFs).unwrap();
    assert_eq!(result.inserted, Vec::<PointOffsetType>::new());
    assert_eq!(read_only.internal_version(2), None);

    // Completing the version flush truncates the torn bytes (a `set_len`) and writes the full entry,
    // and a later live-reload picks up the now-complete version. That truncation is a `set_len`,
    // which Windows forbids while the read-only view keeps the versions file mapped, so this resume
    // check only runs off Windows.
    #[cfg(not(target_os = "windows"))]
    {
        mutable.versions_flusher()().unwrap();

        let result = read_only.live_reload(&MmapFs).unwrap();
        assert_eq!(result.inserted, vec![2]);
        assert_eq!(
            read_only.internal_id_with_behavior(
                300.into(),
                common::types::DeferredBehavior::VisibleOnly
            ),
            Some(2)
        );
        assert_eq!(read_only.internal_version(2), Some(12));
        assert_in_sync(&read_only, &mutable);
    }
}

#[test]
fn test_merge_accumulates_unapplied_delta() {
    // A delete folded into an earlier insert/delete delta keeps both lists sorted
    // and deduplicated, and survives so a failed reload can replay it.
    let mut pending = LiveReloadResult {
        inserted: vec![5, 1, 3],
        deleted: vec![10, 2],
    };
    pending.merge(LiveReloadResult {
        inserted: vec![7],
        deleted: vec![8, 2],
    });
    assert_eq!(pending.inserted, vec![1, 3, 5, 7]);
    assert_eq!(pending.deleted, vec![2, 8, 10]);
}

#[test]
fn test_merge_cancels_insert_then_delete() {
    // An offset inserted (but not yet applied) and then deleted is ultimately gone:
    // dropped from `inserted`, kept in `deleted` so a partially-applied component
    // drops it on replay.
    let mut pending = LiveReloadResult {
        inserted: vec![3, 4],
        deleted: vec![],
    };
    pending.merge(LiveReloadResult {
        inserted: vec![],
        deleted: vec![4],
    });
    assert_eq!(pending.inserted, vec![3]);
    assert_eq!(pending.deleted, vec![4]);
    assert!(!pending.is_empty());
}

#[test]
fn test_merge_empty_is_noop() {
    let mut pending = LiveReloadResult {
        inserted: vec![1],
        deleted: vec![2],
    };
    pending.merge(LiveReloadResult::default());
    assert_eq!(pending.inserted, vec![1]);
    assert_eq!(pending.deleted, vec![2]);

    let mut empty = LiveReloadResult::default();
    assert!(empty.is_empty());
    empty.merge(LiveReloadResult::default());
    assert!(empty.is_empty());
}

/// Over an object store a handle is handed out before the object is touched, so a missing log only
/// reports itself on the first read. These exercise the real stack the serverless read path uses.
mod over_object_storage {
    use std::fmt;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use common::universal_io::{DiskCache, DiskCacheConfig, DiskCacheFs, UioResult, UniversalKind};
    use futures::stream::BoxStream;
    use io_bridge_object_store::{BlobBackend, BlobFile, BlobFs, BridgeRuntime, ObjectStoreSource};
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjectPath;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };

    use super::*;

    /// An in-memory store behind the `BlobBackend` bound the bridge wants.
    #[derive(Debug)]
    struct TestStore(InMemory);

    impl fmt::Display for TestStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "TestStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for TestStore {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.0.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.0.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.0.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<ObjectPath>>,
        ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
            self.0.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.0.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> object_store::Result<ListResult> {
            self.0.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.0.copy_opts(from, to, options).await
        }
    }

    impl BlobBackend for TestStore {
        type Config = ();

        fn build_store(_config: &Self::Config) -> UioResult<Self> {
            Ok(Self(InMemory::new()))
        }

        fn kind() -> UniversalKind {
            UniversalKind::S3
        }
    }

    type Remote = BlobFile<ObjectStoreSource<TestStore>>;
    type Cached = DiskCacheFs<Remote>;

    /// The cached blob filesystem the read path opens segments through, rooted at the bucket.
    fn cached_fs(store: TestStore, scratch: &Path) -> Cached {
        let remote = BlobFs::new(
            ObjectStoreSource::new(Arc::new(store)),
            BridgeRuntime::global(),
        );
        let config = DiskCacheConfig::new(PathBuf::new(), scratch.to_path_buf()).unwrap();
        Cached::new(Arc::new(config), remote)
    }

    /// Mirror a locally written tracker's files into the store under `prefix`.
    fn upload(store: &TestStore, local: &Path, prefix: &str) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        for path in [mappings_path(local), versions_path(local)] {
            let Ok(body) = fs::read(&path) else { continue };
            let name = path.file_name().unwrap().to_str().unwrap();
            let key = ObjectPath::from(format!("{prefix}/{name}"));
            runtime.block_on(store.put(&key, body.into())).unwrap();
        }
    }

    #[test]
    fn test_a_segment_with_no_objects_is_not_persisted() {
        let scratch = Builder::new().prefix("scratch").tempdir().unwrap();
        let fs = cached_fs(TestStore(InMemory::new()), scratch.path());

        let read_only =
            ReadOnlyAppendableIdTracker::<DiskCache<Remote>>::open(&fs, Path::new("seg"), None)
                .unwrap();

        assert_eq!(read_only.available_point_count(), 0);
        assert!(
            !read_only.is_persisted(),
            "the store hands out a handle before touching the object, so only a read can tell",
        );
    }

    #[test]
    fn test_an_uploaded_tracker_is_persisted() {
        let segment_dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let mut mutable = MutableIdTracker::open(segment_dir.path(), None).unwrap();
        insert(&mut mutable, 100.into(), 0, 10);
        flush(&mutable);

        let store = TestStore(InMemory::new());
        upload(&store, segment_dir.path(), "seg");
        let scratch = Builder::new().prefix("scratch").tempdir().unwrap();
        let fs = cached_fs(store, scratch.path());

        let read_only =
            ReadOnlyAppendableIdTracker::<DiskCache<Remote>>::open(&fs, Path::new("seg"), None)
                .unwrap();

        assert_eq!(read_only.available_point_count(), 1);
        assert!(read_only.is_persisted());
    }
}
