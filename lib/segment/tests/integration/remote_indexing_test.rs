// `on_disk` is deprecated, but still the way to pin the graph in RAM in tests
#![allow(deprecated)]

//! Remote indexing through the in-process loopback indexer: quality, failures, retries,
//! lease expiry, cancellation, and the CPU permit.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

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
use segment::index::hnsw_index::remote::registry::REMOTE_INDEXING_TASKS;
use segment::index::hnsw_index::remote::{
    OnFailure, RemoteIndexer, RemoteIndexingConfig, with_thread_remote_indexer,
};
use segment::index::{PayloadIndex, VectorIndexRead};
use segment::json_path::JsonPath;
use segment::payload_json;
use segment::segment::Segment;
use segment::segment_constructor::VectorIndexBuildArgs;
use segment::segment_constructor::simple_segment_constructor::build_simple_segment;
use segment::types::{
    Condition, Distance, FieldCondition, Filter, HnswConfig, HnswGlobalConfig, PayloadSchemaType,
    Range, SearchParams, SeqNumberType,
};
use tempfile::{Builder, TempDir};

const DIM: usize = 16;
const NUM_VECTORS: u64 = 3_000;
const INT_KEY: &str = "int";

fn config(on_failure: OnFailure, heartbeat_timeout: Duration) -> RemoteIndexingConfig {
    RemoteIndexingConfig {
        min_index_size_bytes: 0,
        heartbeat_timeout,
        on_failure,
        max_retries: 3,
    }
}

/// A segment with random vectors, an indexed integer payload, and every 10th point deleted.
fn segment(rng: &mut StdRng) -> (TempDir, Segment) {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let mut segment = build_simple_segment(dir.path(), DIM, Distance::Cosine).unwrap();
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
        segment
            .delete_point(NUM_VECTORS + n, n.into())
            .unwrap();
    }
    segment
        .payload_index
        .borrow_mut()
        .set_indexed(&JsonPath::new(INT_KEY), PayloadSchemaType::Integer)
        .unwrap();
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

/// Hits out of 100 against exact search: unfiltered, and with a narrow range filter, which
/// needs the payload block links.
fn accuracy(segment: &Segment, index: &HNSWIndex, rng: &mut StdRng) -> (usize, usize) {
    let plain = segment.vector_data[DEFAULT_VECTOR_NAME].vector_index.borrow();
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
        hits += usize::from(exact == found);

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
            .search(&[&query], Some(&filter), 5, Some(&params), &Default::default())
            .unwrap();
        filtered_hits += usize::from(exact == found);
    }
    (hits, filtered_hits)
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

    assert!(remote_accuracy.0 >= 90, "remote {remote_accuracy:?}");
    assert!(remote_accuracy.1 >= 90, "remote {remote_accuracy:?}");
    assert!(remote_accuracy.0 + 5 >= local_accuracy.0);
    assert!(remote_accuracy.1 + 5 >= local_accuracy.1);
    assert!(REMOTE_INDEXING_TASKS.is_empty(), "token not revoked");
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
    assert!(hits >= 90 && filtered_hits >= 90, "{hits} {filtered_hits}");
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
