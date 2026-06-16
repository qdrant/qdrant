//! End-to-end check that a segment opened read-only over the universal-IO
//! filesystem answers queries identically to the mutable `load_segment`. Both
//! read the exact same on-disk files, so vector search, filtered reads and
//! payload reads must match point-for-point.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::AtomicBool;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::DeferredBehavior;
use common::universal_io::{MmapFile, MmapFs};
use tempfile::Builder;

use crate::data_types::named_vectors::NamedVectors;
use crate::data_types::query_context::QueryContext;
use crate::data_types::vectors::{DEFAULT_VECTOR_NAME, QueryVector, VectorInternal};
use crate::entry::entry_point::{
    NonAppendableSegmentEntry as _, ReadSegmentEntry, SegmentEntry as _, StorageSegmentEntry as _,
};
use crate::json_path::JsonPath;
use crate::segment::Segment;
use crate::segment::read_only::ReadOnlySegment;
use crate::segment_constructor::build_segment;
use crate::segment_constructor::segment_builder::SegmentBuilder;
use crate::types::{
    Condition, Distance, FieldCondition, Filter, HnswConfig, HnswGlobalConfig, Indexes, Match,
    PayloadFieldSchema, PayloadSchemaType, PointIdType, SegmentConfig, ValueVariants,
    VectorDataConfig, VectorStorageType, WithPayload,
};

const DIM: usize = 8;
const NUM_POINTS: usize = 100;

/// Build a non-appendable, on-disk segment (Mmap storage + HNSW index + two
/// indexed payload fields) the read-only opener can consume: an appendable
/// source is populated, then optimized into the immutable target by
/// `SegmentBuilder` (Mmap storage forces `is_appendable() = false`).
fn build_immutable_segment(segments_path: &Path, temp_path: &Path) -> Segment {
    let hw = HardwareCounterCell::new();

    let source_dir = Builder::new().prefix("ro_source").tempdir().unwrap();
    let mut source = build_segment(
        source_dir.path(),
        &SegmentConfig {
            vector_data: HashMap::from([(
                DEFAULT_VECTOR_NAME.to_owned(),
                VectorDataConfig {
                    size: DIM,
                    distance: Distance::Cosine,
                    storage_type: VectorStorageType::default(),
                    index: Indexes::Plain {},
                    quantization_config: None,
                    multivector_config: None,
                    datatype: None,
                },
            )]),
            sparse_vector_data: Default::default(),
            payload_storage_type: Default::default(),
        },
        None,
        true,
    )
    .unwrap();

    for i in 0..NUM_POINTS {
        let vector: Vec<f32> = (0..DIM)
            .map(|j| ((i * 7 + j * 3) % 13) as f32 + 0.5)
            .collect();
        let vectors = NamedVectors::from_ref(DEFAULT_VECTOR_NAME, vector.as_slice().into());
        let point_id = (i as u64 + 1).into();
        let op_num = (i + 1) as u64;
        source.upsert_point(op_num, point_id, vectors, &hw).unwrap();
        let kw = ["red", "green", "blue"][i % 3];
        let payload = serde_json::from_value(serde_json::json!({
            "kw": kw,
            "num": i as i64,
        }))
        .unwrap();
        source
            .set_full_payload(op_num, point_id, &payload, &hw)
            .unwrap();
    }
    source
        .create_field_index(
            NUM_POINTS as u64 + 1,
            &JsonPath::new("kw"),
            Some(&PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword)),
            &hw,
        )
        .unwrap();
    source
        .create_field_index(
            NUM_POINTS as u64 + 2,
            &JsonPath::new("num"),
            Some(&PayloadFieldSchema::FieldType(PayloadSchemaType::Integer)),
            &hw,
        )
        .unwrap();

    let target_config = SegmentConfig {
        vector_data: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: DIM,
                distance: Distance::Cosine,
                storage_type: VectorStorageType::Mmap,
                index: Indexes::Hnsw(HnswConfig::default()),
                quantization_config: None,
                multivector_config: None,
                datatype: None,
            },
        )]),
        sparse_vector_data: Default::default(),
        payload_storage_type: Default::default(),
    };
    assert!(!target_config.is_appendable());

    let mut builder =
        SegmentBuilder::new(temp_path, &target_config, &HnswGlobalConfig::default()).unwrap();
    builder
        .update(&[&source], &AtomicBool::new(false), &hw)
        .unwrap();
    builder.build_for_test(segments_path)
}

fn keyword_filter(value: &str) -> Filter {
    Filter::new_must(Condition::Field(FieldCondition::new_match(
        JsonPath::new("kw"),
        Match::new_value(ValueVariants::String(value.into())),
    )))
}

fn sorted_filtered(segment: &impl ReadSegmentEntry, filter: &Filter) -> Vec<PointIdType> {
    let hw = HardwareCounterCell::new();
    let mut ids = segment
        .read_filtered(
            None,
            None,
            Some(filter),
            &AtomicBool::new(false),
            &hw,
            DeferredBehavior::WithDeferred,
        )
        .unwrap();
    ids.sort_unstable();
    ids
}

/// Assert a read-only segment answers vector search, filtered reads and payload
/// reads identically to a reference (mutable) segment over the same data.
fn assert_query_equivalence(reference: &impl ReadSegmentEntry, candidate: &impl ReadSegmentEntry) {
    let query = QueryVector::Nearest(VectorInternal::Dense(
        (0..DIM).map(|j| (j % 5) as f32 + 0.25).collect(),
    ));
    let query_context = QueryContext::default();
    let sqc = query_context.get_segment_query_context();

    let reference_hits = reference
        .search_batch(
            DEFAULT_VECTOR_NAME,
            &[&query],
            &WithPayload::default(),
            &false.into(),
            None,
            NUM_POINTS,
            None,
            &sqc,
        )
        .unwrap();
    let candidate_hits = candidate
        .search_batch(
            DEFAULT_VECTOR_NAME,
            &[&query],
            &WithPayload::default(),
            &false.into(),
            None,
            NUM_POINTS,
            None,
            &sqc,
        )
        .unwrap();
    assert!(!reference_hits[0].is_empty());
    assert_eq!(
        reference_hits[0], candidate_hits[0],
        "vector search mismatch"
    );

    for value in ["red", "green", "blue"] {
        let filter = keyword_filter(value);
        assert_eq!(
            sorted_filtered(reference, &filter),
            sorted_filtered(candidate, &filter),
            "filtered read mismatch for kw={value}",
        );
    }

    let hw = HardwareCounterCell::new();
    for i in 0..NUM_POINTS {
        let point_id: PointIdType = (i as u64 + 1).into();
        assert_eq!(
            reference.payload(point_id, &hw).unwrap(),
            candidate.payload(point_id, &hw).unwrap(),
            "payload mismatch for point {point_id}",
        );
    }
}

#[test]
fn read_only_segment_matches_mutable() {
    let segments_dir = Builder::new().prefix("ro_segments").tempdir().unwrap();
    let temp_dir = Builder::new().prefix("ro_builder").tempdir().unwrap();

    let mutable = build_immutable_segment(segments_dir.path(), temp_dir.path());
    let segment_path = mutable.data_path();
    let segment_uuid = mutable.uuid;

    // Open the very same directory read-only over the universal mmap fs.
    let read_only = ReadOnlySegment::<MmapFile>::open(&MmapFs, &segment_path, segment_uuid, None)
        .expect("read-only open");

    assert_eq!(read_only.available_point_count(), NUM_POINTS);
    assert_eq!(read_only.segment_uuid(), segment_uuid);

    assert_query_equivalence(&mutable, &read_only);
}

/// Open a segment straight from an S3-compatible store (rustfs/minio) over
/// `BlobFs` and assert it answers queries identically to the local reference.
///
/// Ignored by default; run with a server up:
/// `S3_INTEGRATION_TEST=1 cargo test -p segment read_only_segment_over_s3 -- --ignored`
#[test]
#[ignore = "requires a running S3-compatible server (set S3_INTEGRATION_TEST=1)"]
fn read_only_segment_over_s3() {
    use std::sync::Arc;

    use bytes::Bytes;
    use common::universal_io::UniversalReadFileOps;
    use io_bridge_object_store::backends::aws::{AwsConfig, AwsCredentials};
    use io_bridge_object_store::{BlobBackend, BlobFile, BlobFs};
    use object_store::ObjectStoreExt;
    use object_store::aws::AmazonS3;
    use object_store::path::Path as ObjectPath;

    if std::env::var("S3_INTEGRATION_TEST").as_deref() != Ok("1") {
        eprintln!("skipping read_only_segment_over_s3: set S3_INTEGRATION_TEST=1");
        return;
    }

    let aws_config = AwsConfig {
        bucket: std::env::var("RUSTFS_BUCKET").unwrap_or_else(|_| "test-bucket".into()),
        region: Some("us-east-1".into()),
        endpoint: Some(
            std::env::var("RUSTFS_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".into()),
        ),
        credentials: AwsCredentials::Static {
            access_key_id: std::env::var("RUSTFS_ACCESS_KEY")
                .unwrap_or_else(|_| "rustfsadmin".into()),
            secret_access_key: std::env::var("RUSTFS_SECRET_KEY")
                .unwrap_or_else(|_| "rustfsadmin".into()),
            session_token: None,
        },
    };

    // Build an immutable segment locally as the reference.
    let segments_dir = Builder::new().prefix("ro_s3_segments").tempdir().unwrap();
    let temp_dir = Builder::new().prefix("ro_s3_builder").tempdir().unwrap();
    let mutable = build_immutable_segment(segments_dir.path(), temp_dir.path());
    let segment_uuid = mutable.uuid;
    let local_path = mutable.data_path();

    // Mirror the whole segment directory into the bucket under `seg/<uuid>/...`.
    let store = AmazonS3::build_store(&aws_config).expect("build S3 store");
    let key_prefix = format!("seg/{segment_uuid}");
    let upload_runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    upload_runtime.block_on(async {
        for entry in walkdir::WalkDir::new(&local_path) {
            let entry = entry.unwrap();
            if !entry.file_type().is_file() {
                continue;
            }
            let rel = entry.path().strip_prefix(&local_path).unwrap();
            let key = format!("{key_prefix}/{}", rel.to_string_lossy());
            let bytes = fs_err::read(entry.path()).unwrap();
            store
                .put(&ObjectPath::from(key.as_str()), Bytes::from(bytes).into())
                .await
                .expect("upload segment file");
        }
    });

    // Open the segment straight from S3 and compare to the local reference.
    let blob_fs = BlobFs::<Arc<AmazonS3>>::from_context(aws_config).expect("blob fs");
    let read_only = ReadOnlySegment::<BlobFile<Arc<AmazonS3>>>::open(
        &blob_fs,
        Path::new(&key_prefix),
        segment_uuid,
        None,
    )
    .expect("read-only open over S3");

    assert_eq!(read_only.available_point_count(), NUM_POINTS);
    assert_query_equivalence(&mutable, &read_only);
}

/// Open + query a segment from S3 through a local disk cache
/// (`DiskCacheFs<BlobFile>`), measuring cold (lazy S3 block fetch) vs warm
/// (local cache hit) latency. The cache mirrors only the blocks the queries
/// touch, so a large segment can be served without pulling it whole.
///
/// Ignored by default; run with a server up:
/// `S3_INTEGRATION_TEST=1 cargo test -p segment read_only_segment_over_s3_disk_cache -- --ignored --nocapture`
#[test]
#[ignore = "requires a running S3-compatible server (set S3_INTEGRATION_TEST=1)"]
fn read_only_segment_over_s3_disk_cache() {
    use std::sync::Arc;
    use std::time::Instant;

    use bytes::Bytes;
    use common::universal_io::{
        DiskCache, DiskCacheConfig, DiskCacheFs, DiskCacheFsContext, UniversalReadFileOps,
    };
    use io_bridge_object_store::backends::aws::{AwsConfig, AwsCredentials};
    use io_bridge_object_store::{BlobBackend, BlobFile};
    use object_store::ObjectStoreExt;
    use object_store::aws::AmazonS3;
    use object_store::path::Path as ObjectPath;

    if std::env::var("S3_INTEGRATION_TEST").as_deref() != Ok("1") {
        eprintln!("skipping read_only_segment_over_s3_disk_cache: set S3_INTEGRATION_TEST=1");
        return;
    }

    let aws_config = AwsConfig {
        bucket: std::env::var("RUSTFS_BUCKET").unwrap_or_else(|_| "test-bucket".into()),
        region: Some("us-east-1".into()),
        endpoint: Some(
            std::env::var("RUSTFS_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".into()),
        ),
        credentials: AwsCredentials::Static {
            access_key_id: std::env::var("RUSTFS_ACCESS_KEY")
                .unwrap_or_else(|_| "rustfsadmin".into()),
            secret_access_key: std::env::var("RUSTFS_SECRET_KEY")
                .unwrap_or_else(|_| "rustfsadmin".into()),
            session_token: None,
        },
    };

    // Build + upload an immutable segment.
    let segments_dir = Builder::new()
        .prefix("ro_cache_segments")
        .tempdir()
        .unwrap();
    let temp_dir = Builder::new().prefix("ro_cache_builder").tempdir().unwrap();
    let mutable = build_immutable_segment(segments_dir.path(), temp_dir.path());
    let segment_uuid = mutable.uuid;
    let local_path = mutable.data_path();
    let key_prefix = format!("seg/{segment_uuid}");

    let store = AmazonS3::build_store(&aws_config).expect("build S3 store");
    let upload_runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    upload_runtime.block_on(async {
        for entry in walkdir::WalkDir::new(&local_path) {
            let entry = entry.unwrap();
            if !entry.file_type().is_file() {
                continue;
            }
            let rel = entry.path().strip_prefix(&local_path).unwrap();
            let key = format!("{key_prefix}/{}", rel.to_string_lossy());
            let bytes = fs_err::read(entry.path()).unwrap();
            store
                .put(&ObjectPath::from(key.as_str()), Bytes::from(bytes).into())
                .await
                .expect("upload segment file");
        }
    });

    // S3 remote behind a fresh (cold) local disk cache. `remote_dir = "seg"` is a
    // logical prefix (never exists locally) stripped from the S3 keys.
    type S3File = BlobFile<Arc<AmazonS3>>;
    let cache_dir = Builder::new().prefix("ro_cache_local").tempdir().unwrap();
    let fs = DiskCacheFs::<S3File>::from_context(DiskCacheFsContext {
        config: Arc::new(
            DiskCacheConfig::new(
                std::path::PathBuf::from("seg"),
                cache_dir.path().to_path_buf(),
            )
            .unwrap(),
        ),
        remote: aws_config,
    })
    .expect("disk-cache fs");

    // Cold: empty cache → every touched block is fetched from S3 on demand.
    let t = Instant::now();
    let read_only =
        ReadOnlySegment::<DiskCache<S3File>>::open(&fs, Path::new(&key_prefix), segment_uuid, None)
            .expect("read-only open over S3 + disk cache");
    let open_cold = t.elapsed();
    assert_eq!(read_only.available_point_count(), NUM_POINTS);

    let query = QueryVector::Nearest(VectorInternal::Dense(
        (0..DIM).map(|j| (j % 5) as f32 + 0.25).collect(),
    ));
    let query_context = QueryContext::default();
    let sqc = query_context.get_segment_query_context();
    let run_search = || {
        read_only
            .search_batch(
                DEFAULT_VECTOR_NAME,
                &[&query],
                &WithPayload::default(),
                &false.into(),
                None,
                NUM_POINTS,
                None,
                &sqc,
            )
            .unwrap()
    };

    let t = Instant::now();
    let cold_hits = run_search();
    let search_cold = t.elapsed();
    let t = Instant::now();
    let warm_hits = run_search();
    let search_warm = t.elapsed();

    assert!(!cold_hits[0].is_empty());
    assert_eq!(cold_hits, warm_hits);

    // Full correctness vs the local mutable reference (over the cached S3 fs).
    assert_query_equivalence(&mutable, &read_only);

    let cached_bytes: u64 = walkdir::WalkDir::new(cache_dir.path())
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .filter_map(|e| e.metadata().ok())
        .map(|m| m.len())
        .sum();

    eprintln!(
        "[s3+disk-cache] points={NUM_POINTS} open(cold)={open_cold:?} \
         search(cold)={search_cold:?} search(warm)={search_warm:?} cached={} KiB",
        cached_bytes / 1024,
    );
}

/// Open + query a segment from real Google Cloud Storage through a local disk
/// cache. Stages a segment in the bucket, reads it back over
/// `DiskCacheFs<BlobFile<GoogleCloudStorage>>`, compares to the local reference,
/// then deletes the staged objects.
///
/// Ignored by default; run with credentials:
/// `GCS_INTEGRATION_TEST=1 GCS_BUCKET=read-segment GCS_SA_KEY=/path/key.json \
///   cargo test -p segment read_only_segment_over_gcs_disk_cache -- --ignored --nocapture`
#[test]
#[ignore = "requires GCS access (GCS_INTEGRATION_TEST=1, optional GCS_SA_KEY / GCS_BUCKET)"]
fn read_only_segment_over_gcs_disk_cache() {
    use std::sync::Arc;
    use std::time::Instant;

    use bytes::Bytes;
    use common::universal_io::{
        DiskCache, DiskCacheConfig, DiskCacheFs, DiskCacheFsContext, UniversalReadFileOps,
    };
    use io_bridge_object_store::backends::gcp::{GcsConfig, GcsCredentials};
    use io_bridge_object_store::{BlobBackend, BlobFile};
    use object_store::ObjectStoreExt;
    use object_store::gcp::GoogleCloudStorage;
    use object_store::path::Path as ObjectPath;

    if std::env::var("GCS_INTEGRATION_TEST").as_deref() != Ok("1") {
        eprintln!("skipping read_only_segment_over_gcs_disk_cache: set GCS_INTEGRATION_TEST=1");
        return;
    }

    let gcs_config = GcsConfig {
        bucket: std::env::var("GCS_BUCKET").unwrap_or_else(|_| "read-segment".into()),
        credentials: match std::env::var("GCS_SA_KEY") {
            Ok(path) => GcsCredentials::ServiceAccountPath(path),
            Err(_) => GcsCredentials::Default,
        },
    };

    // Build an immutable segment locally as the reference.
    let segments_dir = Builder::new().prefix("ro_gcs_segments").tempdir().unwrap();
    let temp_dir = Builder::new().prefix("ro_gcs_builder").tempdir().unwrap();
    let mutable = build_immutable_segment(segments_dir.path(), temp_dir.path());
    let segment_uuid = mutable.uuid;
    let local_path = mutable.data_path();
    let key_prefix = format!("seg/{segment_uuid}");

    // Stage the segment in the bucket.
    let store = GoogleCloudStorage::build_store(&gcs_config).expect("build GCS store");
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    let uploaded_keys: Vec<String> = runtime.block_on(async {
        let mut keys = Vec::new();
        for entry in walkdir::WalkDir::new(&local_path) {
            let entry = entry.unwrap();
            if !entry.file_type().is_file() {
                continue;
            }
            let rel = entry.path().strip_prefix(&local_path).unwrap();
            let key = format!("{key_prefix}/{}", rel.to_string_lossy());
            let bytes = fs_err::read(entry.path()).unwrap();
            store
                .put(&ObjectPath::from(key.as_str()), Bytes::from(bytes).into())
                .await
                .expect("upload segment file");
            keys.push(key);
        }
        keys
    });

    // GCS remote behind a fresh (cold) local disk cache.
    type GcsFile = BlobFile<Arc<GoogleCloudStorage>>;
    let cache_dir = Builder::new().prefix("ro_gcs_cache").tempdir().unwrap();
    let fs = DiskCacheFs::<GcsFile>::from_context(DiskCacheFsContext {
        config: Arc::new(
            DiskCacheConfig::new(
                std::path::PathBuf::from("seg"),
                cache_dir.path().to_path_buf(),
            )
            .unwrap(),
        ),
        remote: gcs_config,
    })
    .expect("disk-cache fs");

    let t = Instant::now();
    let read_only = ReadOnlySegment::<DiskCache<GcsFile>>::open(
        &fs,
        Path::new(&key_prefix),
        segment_uuid,
        None,
    )
    .expect("read-only open over GCS + disk cache");
    let open_cold = t.elapsed();
    assert_eq!(read_only.available_point_count(), NUM_POINTS);

    let query = QueryVector::Nearest(VectorInternal::Dense(
        (0..DIM).map(|j| (j % 5) as f32 + 0.25).collect(),
    ));
    let query_context = QueryContext::default();
    let sqc = query_context.get_segment_query_context();
    let run_search = || {
        read_only
            .search_batch(
                DEFAULT_VECTOR_NAME,
                &[&query],
                &WithPayload::default(),
                &false.into(),
                None,
                NUM_POINTS,
                None,
                &sqc,
            )
            .unwrap()
    };

    let t = Instant::now();
    let cold_hits = run_search();
    let search_cold = t.elapsed();
    let t = Instant::now();
    let warm_hits = run_search();
    let search_warm = t.elapsed();

    assert!(!cold_hits[0].is_empty());
    assert_eq!(cold_hits, warm_hits);
    assert_query_equivalence(&mutable, &read_only);

    let cached_bytes: u64 = walkdir::WalkDir::new(cache_dir.path())
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .filter_map(|e| e.metadata().ok())
        .map(|m| m.len())
        .sum();

    eprintln!(
        "[gcs+disk-cache] points={NUM_POINTS} open(cold)={open_cold:?} \
         search(cold)={search_cold:?} search(warm)={search_warm:?} cached={} KiB",
        cached_bytes / 1024,
    );

    // Clean up the staged objects (best-effort).
    runtime.block_on(async {
        for key in &uploaded_keys {
            let _ = store.delete(&ObjectPath::from(key.as_str())).await;
        }
    });
}
