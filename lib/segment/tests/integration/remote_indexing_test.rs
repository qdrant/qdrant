// `on_disk` is deprecated, but still the way to pin the graph in RAM in tests
#![allow(deprecated)]

//! Remote indexing through the in-process loopback indexer: quality, failures, retries,
//! lease expiry, cancellation, and the CPU permit.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use atomic_refcell::AtomicRefCell;
use common::ambient;
use common::budget::{ResourceBudget, ResourcePermit};
use common::flags::FeatureFlags;
use common::progress_tracker::ProgressTracker;
use ordered_float::OrderedFloat;
use rand::prelude::StdRng;
use rand::{RngExt, SeedableRng};
use segment::common::operation_error::{OperationError, OperationResult};
use segment::data_types::vectors::{DEFAULT_VECTOR_NAME, QueryVector, only_default_vector};
use segment::entry::entry_point::{NonAppendableSegmentEntry as _, SegmentEntry};
use segment::fixtures::payload_fixtures::{random_int_payload, random_vector};
use segment::index::hnsw_index::hnsw::{HNSWIndex, HnswIndexOpenArgs};
use segment::index::hnsw_index::remote::executor::RunOutcome;
use segment::index::hnsw_index::remote::loopback::{LoopbackBehavior, LoopbackIndexer};
use segment::index::hnsw_index::remote::{
    OnFailure, RemoteIndexer, RemoteIndexingConfig, with_thread_remote_indexer,
};
use segment::index::{PayloadIndex, VectorIndexRead};
use segment::json_path::JsonPath;
use segment::payload_json;
use segment::segment::Segment;
use segment::segment_constructor::{VectorIndexBuildArgs, build_segment};
use segment::types::{
    BinaryQuantization, BinaryQuantizationConfig, CompressionRatio, Condition, Distance,
    FieldCondition, Filter, HnswConfig, HnswGlobalConfig, Indexes, PayloadSchemaType,
    ProductQuantization, ProductQuantizationConfig, QuantizationConfig, Range, ScalarQuantization,
    ScalarQuantizationConfig, ScalarType, SearchParams, SegmentConfig, SeqNumberType,
    TurboQuantBitSize, TurboQuantQuantizationConfig, TurboQuantization, VectorDataConfig,
    VectorStorageDatatype, VectorStorageType,
};
use segment::vector_storage::quantized::quantized_vectors::{
    QuantizedVectors, QuantizedVectorsStorageType,
};
use tempfile::{Builder, TempDir};

const DIM: usize = 16;
const NUM_VECTORS: u64 = 3_000;
const INT_KEY: &str = "int";

fn config(on_failure: OnFailure, heartbeat_timeout: Duration) -> RemoteIndexingConfig {
    RemoteIndexingConfig {
        min_index_size_bytes: 0,
        heartbeat_timeout,
        queue_timeout: heartbeat_timeout,
        on_failure,
        max_retries: 3,
    }
}

/// A segment with random vectors, an indexed integer payload, and every 10th point deleted.
fn segment(rng: &mut StdRng) -> (TempDir, Segment) {
    segment_with(rng, None, None)
}

/// Like [`segment`], with vectors of `datatype`, and `quantization` built.
fn segment_with(
    rng: &mut StdRng,
    datatype: Option<VectorStorageDatatype>,
    quantization: Option<QuantizationConfig>,
) -> (TempDir, Segment) {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let config = SegmentConfig {
        vector_data: HashMap::from([(
            DEFAULT_VECTOR_NAME.to_owned(),
            VectorDataConfig {
                size: DIM,
                distance: Distance::Cosine,
                storage_type: VectorStorageType::InRamChunkedMmap,
                index: Indexes::Plain {},
                quantization_config: None,
                multivector_config: None,
                datatype,
            },
        )]),
        sparse_vector_data: Default::default(),
        payload_storage_type: Default::default(),
        id_tracker_memory: None,
    };
    let (mut segment, _token) = build_segment(dir.path(), &config, None, true).unwrap();
    for n in 0..NUM_VECTORS {
        let vector = random_vector(rng, DIM);
        let payload = payload_json! {INT_KEY: random_int_payload(rng, 1..=1)};
        segment
            .upsert_point(n as SeqNumberType, n.into(), only_default_vector(&vector))
            .unwrap();
        segment
            .set_full_payload(n as SeqNumberType, n.into(), &payload)
            .unwrap();
    }
    for n in (0..NUM_VECTORS).step_by(10) {
        segment.delete_point(NUM_VECTORS + n, n.into()).unwrap();
    }
    segment
        .payload_index
        .borrow_mut()
        .set_indexed(&JsonPath::new(INT_KEY), PayloadSchemaType::Integer)
        .unwrap();
    if let Some(quantization) = quantization {
        let data = segment.vector_data.get_mut(DEFAULT_VECTOR_NAME).unwrap();
        let quantized = QuantizedVectors::create(
            &data.vector_storage.borrow(),
            &quantization,
            QuantizedVectorsStorageType::Immutable,
            &dir.path().join("quantized"),
            2,
            &AtomicBool::new(false),
        )
        .unwrap();
        data.quantized_vectors = Arc::new(AtomicRefCell::new(Some(quantized)));
    }
    (dir, segment)
}

fn build(
    segment: &Segment,
    permit: Arc<ResourcePermit>,
    stopped: &AtomicBool,
    rng: &mut StdRng,
) -> OperationResult<(TempDir, HNSWIndex)> {
    let hnsw_dir = Builder::new().prefix("hnsw_dir").tempdir().unwrap();
    let data = &segment.vector_data[DEFAULT_VECTOR_NAME];
    let index = HNSWIndex::build(
        HnswIndexOpenArgs {
            path: hnsw_dir.path(),
            id_tracker: segment.id_tracker.clone(),
            vector_storage: data.vector_storage.clone(),
            quantized_vectors: data.quantized_vectors.clone(),
            payload_index: segment.payload_index.clone(),
            hnsw_config: HnswConfig {
                memory: None,
                m: 16,
                ef_construct: 64,
                full_scan_threshold: 16, // KB: payload blocks of ~250 points
                max_indexing_threads: 2,
                on_disk: Some(false),
                payload_m: None,
                inline_storage: None,
            },
        },
        VectorIndexBuildArgs {
            permit,
            old_indices: &[],
            gpu_device: None,
            rng,
            stopped,
            hnsw_global_config: &HnswGlobalConfig::default(),
            feature_flags: FeatureFlags::default(),
            inline_vectors: false,
            progress: ProgressTracker::new_for_test(),
        },
    )?;
    Ok((hnsw_dir, index))
}

fn build_with(
    indexer: &Arc<LoopbackIndexer>,
    segment: &Segment,
    stopped: &AtomicBool,
    rng: &mut StdRng,
) -> OperationResult<(TempDir, HNSWIndex)> {
    let dyn_indexer: Arc<dyn RemoteIndexer> = Arc::new(indexer.clone());
    with_thread_remote_indexer(dyn_indexer, || {
        build(segment, Arc::new(ResourcePermit::dummy(2)), stopped, rng)
    })
}

/// Ids shared with exact search, out of 500 (100 queries, top 5): unfiltered, and with a
/// narrow range filter, which needs the payload block links. Ids, not scores: quantized
/// indexes score differently.
fn accuracy(segment: &Segment, index: &HNSWIndex, rng: &mut StdRng) -> (usize, usize) {
    let plain = segment.vector_data[DEFAULT_VECTOR_NAME]
        .vector_index
        .borrow();
    let params = SearchParams {
        hnsw_ef: Some(64),
        ..Default::default()
    };
    let mut hits = 0;
    let mut filtered_hits = 0;
    for _ in 0..100 {
        let query = QueryVector::from(random_vector(rng, DIM));
        let exact = plain
            .search(&[&query], None, 5, None, &Default::default())
            .unwrap();
        let found = index
            .search(&[&query], None, 5, Some(&params), &Default::default())
            .unwrap();
        hits += overlap(&exact[0], &found[0]);

        let left = rng.random_range(0..90);
        let filter = Filter::new_must(Condition::Field(FieldCondition::new_range(
            JsonPath::new(INT_KEY),
            Range {
                lt: None,
                gt: None,
                gte: Some(OrderedFloat(f64::from(left))),
                lte: Some(OrderedFloat(f64::from(left + 4))),
            },
        )));
        let exact = plain
            .search(&[&query], Some(&filter), 5, None, &Default::default())
            .unwrap();
        let found = index
            .search(
                &[&query],
                Some(&filter),
                5,
                Some(&params),
                &Default::default(),
            )
            .unwrap();
        filtered_hits += overlap(&exact[0], &found[0]);
    }
    (hits, filtered_hits)
}

fn overlap(
    exact: &[common::types::ScoredPointOffset],
    found: &[common::types::ScoredPointOffset],
) -> usize {
    exact
        .iter()
        .filter(|e| found.iter().any(|f| f.idx == e.idx))
        .count()
}

#[test]
fn remote_build_matches_local_quality() {
    let _scope = ambient::test_guard();
    let mut rng = StdRng::seed_from_u64(42);
    let (_dir, segment) = segment(&mut rng);
    let stopped = AtomicBool::new(false);

    let (_local_dir, local) = build(
        &segment,
        Arc::new(ResourcePermit::dummy(2)),
        &stopped,
        &mut rng,
    )
    .unwrap();
    let local_accuracy = accuracy(&segment, &local, &mut StdRng::seed_from_u64(1));

    let indexer = Arc::new(LoopbackIndexer::new(
        config(OnFailure::Local, Duration::from_secs(10)),
        vec![LoopbackBehavior::Build],
    ));
    let (_remote_dir, remote) = build_with(&indexer, &segment, &stopped, &mut rng).unwrap();
    assert_eq!(indexer.submissions.load(Ordering::SeqCst), 1);
    assert_eq!(
        indexer.wait_outcomes(1, Duration::from_secs(5)),
        vec![RunOutcome::Uploaded]
    );
    let remote_accuracy = accuracy(&segment, &remote, &mut StdRng::seed_from_u64(1));
    eprintln!("local {local_accuracy:?}, remote {remote_accuracy:?}");

    assert!(remote_accuracy.0 >= 450, "remote {remote_accuracy:?}");
    assert!(remote_accuracy.1 >= 450, "remote {remote_accuracy:?}");
    assert!(remote_accuracy.0 + 25 >= local_accuracy.0);
    assert!(remote_accuracy.1 + 25 >= local_accuracy.1);
}

/// Real remote builds are large enough for PiPNN, which the indexer then picks (D1). Its graph
/// must pass core's validation and search as well as a local build.
#[test]
fn remote_pipnn_graph_is_accepted() {
    let _scope = ambient::test_guard();
    let mut rng = StdRng::seed_from_u64(43);
    let (_dir, segment) = segment(&mut rng);
    let stopped = AtomicBool::new(false);

    let (_local_dir, local) = build(
        &segment,
        Arc::new(ResourcePermit::dummy(2)),
        &stopped,
        &mut rng,
    )
    .unwrap();
    let local_accuracy = accuracy(&segment, &local, &mut StdRng::seed_from_u64(1));

    let indexer = Arc::new(
        LoopbackIndexer::new(
            config(OnFailure::Local, Duration::from_secs(10)),
            vec![LoopbackBehavior::Build],
        )
        .with_pipnn_min_points(1000),
    );
    let (_remote_dir, remote) = build_with(&indexer, &segment, &stopped, &mut rng).unwrap();
    assert_eq!(
        indexer.wait_outcomes(1, Duration::from_secs(5)),
        vec![RunOutcome::Uploaded],
        "PiPNN graph rejected"
    );
    let remote_accuracy = accuracy(&segment, &remote, &mut StdRng::seed_from_u64(1));
    eprintln!("local {local_accuracy:?}, remote PiPNN {remote_accuracy:?}");
    assert!(
        remote_accuracy.0 + 25 >= local_accuracy.0,
        "{remote_accuracy:?}"
    );
    assert!(
        remote_accuracy.1 + 25 >= local_accuracy.1,
        "{remote_accuracy:?}"
    );
}

/// Runs a build where the indexer misbehaves per `behaviors`. Returns the indexer and whether
/// the index searches well, i.e. the build fell back or succeeded correctly.
fn check_recovers(
    on_failure: OnFailure,
    behaviors: Vec<LoopbackBehavior>,
    heartbeat_timeout: Duration,
) -> Arc<LoopbackIndexer> {
    let _scope = ambient::test_guard();
    let mut rng = StdRng::seed_from_u64(7);
    let (_dir, segment) = segment(&mut rng);
    let stopped = AtomicBool::new(false);
    let indexer = Arc::new(LoopbackIndexer::new(
        config(on_failure, heartbeat_timeout),
        behaviors,
    ));
    let (_hnsw_dir, index) = build_with(&indexer, &segment, &stopped, &mut rng).unwrap();
    let (hits, filtered_hits) = accuracy(&segment, &index, &mut rng);
    assert!(
        hits >= 450 && filtered_hits >= 450,
        "{hits} {filtered_hits}"
    );
    indexer
}

#[test]
fn failure_falls_back_to_local() {
    let indexer = check_recovers(
        OnFailure::Local,
        vec![LoopbackBehavior::Fail],
        Duration::from_secs(10),
    );
    assert_eq!(indexer.submissions.load(Ordering::SeqCst), 1);
}

#[test]
fn corrupted_graph_is_rejected() {
    let indexer = check_recovers(
        OnFailure::Local,
        vec![LoopbackBehavior::Corrupt],
        Duration::from_secs(10),
    );
    assert_eq!(indexer.submissions.load(Ordering::SeqCst), 1);
    let outcomes = indexer.wait_outcomes(1, Duration::from_secs(5));
    assert!(
        matches!(&outcomes[0], RunOutcome::Failed(reason) if reason.starts_with("upload")),
        "{outcomes:?}"
    );
}

#[test]
fn retries_with_new_token_until_success() {
    let indexer = check_recovers(
        OnFailure::Retry,
        vec![
            LoopbackBehavior::RefuseSubmit,
            LoopbackBehavior::Fail,
            LoopbackBehavior::Corrupt,
            LoopbackBehavior::Build,
        ],
        Duration::from_secs(10),
    );
    assert_eq!(indexer.submissions.load(Ordering::SeqCst), 4);
    let outcomes = indexer.wait_outcomes(3, Duration::from_secs(5));
    assert_eq!(outcomes.last(), Some(&RunOutcome::Uploaded), "{outcomes:?}");
}

#[test]
fn retries_exhausted_fall_back_to_local() {
    let indexer = check_recovers(
        OnFailure::Retry,
        vec![LoopbackBehavior::RefuseSubmit],
        Duration::from_secs(10),
    );
    // First attempt plus `max_retries`
    assert_eq!(indexer.submissions.load(Ordering::SeqCst), 4);
}

#[test]
fn expired_lease_falls_back_to_local() {
    let indexer = check_recovers(
        OnFailure::Local,
        vec![LoopbackBehavior::Ignore],
        Duration::from_millis(300),
    );
    assert_eq!(indexer.submissions.load(Ordering::SeqCst), 1);
}

#[test]
fn queued_task_outlives_heartbeat_timeout() {
    // Waits 1s in the indexer's queue: longer than the 300ms heartbeat lease, within the
    // queue lease
    let _scope = ambient::test_guard();
    let mut rng = StdRng::seed_from_u64(11);
    let (_dir, segment) = segment(&mut rng);
    let stopped = AtomicBool::new(false);
    let mut config = config(OnFailure::Local, Duration::from_millis(300));
    config.queue_timeout = Duration::from_secs(10);
    let indexer = Arc::new(LoopbackIndexer::new(
        config,
        vec![LoopbackBehavior::Queue(Duration::from_secs(1))],
    ));
    build_with(&indexer, &segment, &stopped, &mut rng).unwrap();
    assert_eq!(
        indexer.wait_outcomes(1, Duration::from_secs(5)),
        vec![RunOutcome::Uploaded]
    );
}

#[test]
fn heartbeats_keep_a_slow_task_alive() {
    // The stalled indexer heartbeats every 100ms: with a 300ms lease, the task stays alive
    // until the optimization is stopped
    let _scope = ambient::test_guard();
    let mut rng = StdRng::seed_from_u64(3);
    let (_dir, segment) = segment(&mut rng);
    let stopped = Arc::new(AtomicBool::new(false));
    let indexer = Arc::new(LoopbackIndexer::new(
        config(OnFailure::Local, Duration::from_millis(300)),
        vec![LoopbackBehavior::Stall],
    ));

    let stopper = {
        let stopped = stopped.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(1500));
            stopped.store(true, Ordering::Relaxed);
        })
    };
    let result = build_with(&indexer, &segment, &stopped, &mut rng);
    stopper.join().unwrap();
    assert!(
        matches!(result, Err(OperationError::Cancelled { .. })),
        "expected cancellation, got {:?}",
        result.map(|_| ())
    );
    assert_eq!(indexer.submissions.load(Ordering::SeqCst), 1);
    // The next heartbeat answers CANCEL (D6)
    assert_eq!(
        indexer.wait_outcomes(1, Duration::from_secs(5)),
        vec![RunOutcome::Dropped("cancelled by core".to_string())]
    );
}

#[test]
fn small_segments_stay_local() {
    let _scope = ambient::test_guard();
    let mut rng = StdRng::seed_from_u64(5);
    let (_dir, segment) = segment(&mut rng);
    let stopped = AtomicBool::new(false);
    let mut config = config(OnFailure::Local, Duration::from_secs(10));
    config.min_index_size_bytes = 1 << 30;
    let indexer = Arc::new(LoopbackIndexer::new(config, vec![LoopbackBehavior::Build]));
    build_with(&indexer, &segment, &stopped, &mut rng).unwrap();
    assert_eq!(indexer.submissions.load(Ordering::SeqCst), 0);
}

#[test]
fn cpu_permit_is_lent_while_waiting() {
    let _scope = ambient::test_guard();
    let mut rng = StdRng::seed_from_u64(9);
    let (_dir, segment) = segment(&mut rng);
    let stopped = Arc::new(AtomicBool::new(false));
    let budget = ResourceBudget::new(4, 4);
    let permit = Arc::new(budget.try_acquire(4, 0).unwrap());
    assert!(!budget.has_budget_exact(1, 0));

    let indexer = Arc::new(LoopbackIndexer::new(
        config(OnFailure::Local, Duration::from_secs(10)),
        vec![LoopbackBehavior::Stall],
    ));
    let watcher = {
        let budget = budget.clone();
        let stopped = stopped.clone();
        std::thread::spawn(move || {
            // The stalled task holds no CPU: all 4 are available to other optimizations
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            let mut lent = false;
            while std::time::Instant::now() < deadline {
                if budget.has_budget_exact(4, 0) {
                    lent = true;
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            stopped.store(true, Ordering::Relaxed);
            lent
        })
    };
    let dyn_indexer: Arc<dyn RemoteIndexer> = Arc::new(indexer.clone());
    let result = with_thread_remote_indexer(dyn_indexer, || {
        build(&segment, permit.clone(), &stopped, &mut rng)
    });
    assert!(watcher.join().unwrap(), "CPUs were not lent");
    assert!(result.is_err());
    drop(permit);
    assert!(budget.has_budget_exact(4, 0), "budget accounting broken");
}

/// Local and remote build of the same segment: the remote one must be as good, and must
/// have read `expected_bytes` (within 10%).
fn check_remote_matches_local(
    datatype: Option<VectorStorageDatatype>,
    quantization: Option<QuantizationConfig>,
    expected_bytes: usize,
) {
    let _scope = ambient::test_guard();
    let mut rng = StdRng::seed_from_u64(21);
    let (_dir, segment) = segment_with(&mut rng, datatype, quantization);
    let stopped = AtomicBool::new(false);

    let (_local_dir, local) = build(
        &segment,
        Arc::new(ResourcePermit::dummy(2)),
        &stopped,
        &mut rng,
    )
    .unwrap();
    let local_accuracy = accuracy(&segment, &local, &mut StdRng::seed_from_u64(1));

    let indexer = Arc::new(LoopbackIndexer::new(
        config(OnFailure::Local, Duration::from_secs(10)),
        vec![LoopbackBehavior::Build],
    ));
    let (_remote_dir, remote) = build_with(&indexer, &segment, &stopped, &mut rng).unwrap();
    assert_eq!(
        indexer.wait_outcomes(1, Duration::from_secs(5)),
        vec![RunOutcome::Uploaded]
    );
    let read = indexer.reports.lock()[0].vector_bytes_read as usize;
    let remote_accuracy = accuracy(&segment, &remote, &mut StdRng::seed_from_u64(1));
    eprintln!(
        "{datatype:?}: read {read} bytes (expected ~{expected_bytes}), \
         local {local_accuracy:?}, remote {remote_accuracy:?}"
    );
    assert!(
        read.abs_diff(expected_bytes) * 10 <= expected_bytes,
        "read {read} bytes, expected ~{expected_bytes}"
    );
    assert!(remote_accuracy.0 + 25 >= local_accuracy.0);
    assert!(remote_accuracy.1 + 25 >= local_accuracy.1);
}

/// Points not deleted by `segment_with`
const LIVE_POINTS: usize = (NUM_VECTORS - NUM_VECTORS / 10) as usize;

#[test]
fn scalar_quantization_sends_quantized_vectors_only() {
    // One byte per dimension instead of four, plus a 4-byte correction per vector, for all
    // offsets: quantized files are sent whole
    check_remote_matches_local(
        None,
        Some(QuantizationConfig::Scalar(ScalarQuantization {
            scalar: ScalarQuantizationConfig {
                r#type: ScalarType::Int8,
                quantile: None,
                always_ram: Some(true),
                memory: None,
            },
        })),
        NUM_VECTORS as usize * (DIM + 4),
    );
}

#[test]
fn binary_quantization_sends_quantized_vectors_only() {
    // One bit per dimension, padded to 128 bits: 16 bytes for these 16 dimensions
    check_remote_matches_local(
        None,
        Some(QuantizationConfig::Binary(BinaryQuantization {
            binary: BinaryQuantizationConfig {
                always_ram: Some(true),
                memory: None,
                encoding: None,
                query_encoding: None,
            },
        })),
        NUM_VECTORS as usize * 16,
    );
}

#[test]
fn turbo_quantization_sends_quantized_vectors_only() {
    // 8 bits per dimension plus a 4-byte trailer, for all offsets. The indexer reconstructs
    // query vectors from the codes, as a TurboQuant vector storage does; the local build uses
    // the originals, hence the tolerance of `check_remote_matches_local`
    check_remote_matches_local(
        None,
        Some(QuantizationConfig::Turbo(TurboQuantization {
            turbo: TurboQuantQuantizationConfig {
                always_ram: Some(true),
                memory: None,
                bits: Some(TurboQuantBitSize::Bits8),
            },
        })),
        NUM_VECTORS as usize * (DIM + 4),
    );
}

#[test]
fn product_quantization_sends_storage_vectors() {
    // PQ encodes queries from the original vectors, which are sent; the indexer builds with
    // them alone, without PQ
    check_remote_matches_local(
        None,
        Some(QuantizationConfig::Product(ProductQuantization {
            product: ProductQuantizationConfig {
                compression: CompressionRatio::X4,
                always_ram: Some(true),
                memory: None,
            },
        })),
        LIVE_POINTS * DIM * 4,
    );
}

#[test]
fn turbo_storage_is_built_remotely() {
    // Storage-native bytes of a TurboQuant storage are already encoded: 8 bits per
    // dimension, plus a 4-byte header per vector
    check_remote_matches_local(
        Some(VectorStorageDatatype::Turbo8),
        None,
        LIVE_POINTS * (DIM + 4),
    );
}
