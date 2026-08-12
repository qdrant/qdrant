use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use api::rest::{VectorOutput, VectorStructOutput};
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::types::{DetailsLevel, TelemetryDetail};
use segment::types::{PointIdType, VectorNameBuf, WithPayloadInterface, WithVector};
use shard::scroll::ScrollRequestInternal;

use super::op::{canonical_sparse, dense_diff, dense_matches};
use super::{Model, ModelEntry, VectorValue};
use crate::collection::Collection;
use crate::operations::shard_selector_internal::ShardSelectorInternal;
use crate::shards::shard::{PeerId, ShardId};

pub(super) async fn collect_model_from_collection(collection: &Collection) -> Model {
    let scroll = collection
        .scroll_by(
            ScrollRequestInternal {
                offset: None,
                limit: Some(usize::MAX),
                filter: None,
                with_payload: Some(WithPayloadInterface::Bool(true)),
                with_vector: WithVector::Bool(true),
                order_by: None,
            },
            None,
            None,
            &ShardSelectorInternal::All,
            None,
            HwMeasurementAcc::new(),
        )
        .await
        .expect("scroll failed");

    let mut out = Model::new();
    for record in scroll.points {
        let named = match record.vector.expect("vector missing in scroll result") {
            VectorStructOutput::Named(m) => m,
            other @ (VectorStructOutput::Single(_) | VectorStructOutput::MultiDense(_)) => {
                panic!("expected Named vector struct, got {other:?}")
            }
        };
        let vectors = named_output_to_model(named);
        let payload = record.payload.unwrap_or_default();
        out.insert(record.id, ModelEntry { vectors, payload });
    }
    out
}

fn named_output_to_model(
    named: HashMap<VectorNameBuf, VectorOutput>,
) -> BTreeMap<VectorNameBuf, VectorValue> {
    let mut out = BTreeMap::new();
    for (name, value) in named {
        let v = match value {
            VectorOutput::Dense(v) => VectorValue::Dense(v),
            VectorOutput::Sparse(sv) => VectorValue::Sparse(canonical_sparse(&sv)),
            VectorOutput::MultiDense(matrix) => VectorValue::MultiDense(matrix),
        };
        out.insert(name, v);
    }
    out
}

/// Returns `(extra, missing)` — the ids in `actual` but not in `expected`, and vice versa.
/// Both lists are sorted for deterministic output (used by the trace writer and by the
/// size-mismatch panic in `assert_matches_model`).
pub(super) fn id_diff(actual: &Model, expected: &Model) -> (Vec<PointIdType>, Vec<PointIdType>) {
    let actual_ids: ahash::AHashSet<_> = actual.keys().copied().collect();
    let expected_ids: ahash::AHashSet<_> = expected.keys().copied().collect();
    let mut extra: Vec<_> = actual_ids.difference(&expected_ids).copied().collect();
    let mut missing: Vec<_> = expected_ids.difference(&actual_ids).copied().collect();
    extra.sort();
    missing.sort();
    (extra, missing)
}

/// Engine-side postmortem for points a reload lost, logged before the panic that reports them.
///
/// Says, per point, whether any segment still knows the id and in what state. The two answers
/// separate the candidate mechanisms:
///
/// - **unknown to every segment**: nothing on disk ever recorded the point, so the WAL was
///   acknowledged past a write that only ever existed in an in-memory pending buffer.
/// - **known but soft-deleted, or known at an older version**: the point did reach disk and a
///   later durable write (a copy-on-write move's source-side delete, typically) removed or
///   reverted it without its counterpart surviving.
///
/// The distinction matters because the restart is an in-process close+reopen, not a crash: the page
/// cache keeps every byte ever written whether or not it was fsynced, so anything genuinely absent
/// was never written at all.
pub(super) async fn describe_missing_points(
    collection: &Collection,
    missing: &[PointIdType],
) -> String {
    use common::types::DeferredBehavior;

    let holder = collection.shards_holder.read().await;
    let mut out = String::new();
    for (shard_id, replica_set) in holder.get_shards() {
        let Some(segments) = replica_set.segments_for_testing().await else {
            continue;
        };
        let segments = segments.read();
        for id in missing {
            let mut sightings = Vec::new();
            for (segment_id, locked) in segments.iter() {
                let segment = locked.get();
                let segment = segment.read();
                let visible = segment.has_point(*id, DeferredBehavior::WithDeferred);
                let version = segment.point_version(*id);
                if visible || version.is_some() {
                    sightings.push(format!(
                        "segment {segment_id}: visible={visible} version={version:?} \
                         (segment version={}, persisted={})",
                        segment.version(),
                        segment.persistent_version(),
                    ));
                }
            }
            let verdict = if sightings.is_empty() {
                "unknown to every segment (never written to disk)".to_string()
            } else {
                sightings.join("; ")
            };
            out.push_str(&format!("\n  shard {shard_id} id {id:?}: {verdict}"));
        }
    }
    out
}

/// Where every point lives, captured from the live engine: id -> per-segment sightings with the
/// segment's directory, the point's version there, and the segment's version/persisted pair.
///
/// Captured immediately before a restart's `stop_gracefully` and printed for any id the reload
/// loses, this pins the pre-close side of the custody question: the reload postmortem says which
/// reloaded directories lack the point, and this says which directory's in-memory state held it,
/// at what claimed durability. A segment that appears here as clean (version == persisted) but
/// whose reloaded directory lacks the point is a flush whose content does not match its claim.
pub(super) async fn capture_placement(
    collection: &Collection,
) -> HashMap<PointIdType, Vec<String>> {
    let holder = collection.shards_holder.read().await;
    let mut out: HashMap<PointIdType, Vec<String>> = HashMap::new();
    for (shard_id, replica_set) in holder.get_shards() {
        let Some(segments) = replica_set.segments_for_testing().await else {
            continue;
        };
        let segments = segments.read();
        for (segment_id, locked) in segments.iter() {
            let guard = locked.get();
            let read = guard.read();
            let dir = read
                .data_path()
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default();
            let (seg_version, seg_persisted) = (read.version(), read.persistent_version());
            for point_id in read.read_range(None, None) {
                let version = read.point_version(point_id);
                let deferred = read.point_is_deferred(point_id);
                out.entry(point_id).or_default().push(format!(
                    "shard {shard_id} segment {segment_id} dir {dir} point_version={version:?} \
                     deferred={deferred} segment={seg_version}/{seg_persisted}",
                ));
            }
        }
    }
    out
}

pub(super) fn assert_matches_model(actual: &Model, expected: &Model, ctx: &str) {
    if actual.len() != expected.len() {
        let (extra, missing) = id_diff(actual, expected);
        panic!(
            "{ctx}: collection has {} points, model has {}; \
             extra in collection (not in model): {extra:?}; \
             missing from collection (in model): {missing:?}",
            actual.len(),
            expected.len(),
        );
    }
    for (id, expected_entry) in expected {
        let actual_entry = actual
            .get(id)
            .unwrap_or_else(|| panic!("{ctx}: missing id {id:?}"));
        assert_eq!(
            actual_entry.vectors.keys().collect::<Vec<_>>(),
            expected_entry.vectors.keys().collect::<Vec<_>>(),
            "{ctx}: vector names mismatch for id {id:?}",
        );
        for (name, expected_value) in &expected_entry.vectors {
            let actual_value = &actual_entry.vectors[name];
            // Turbo4 dense values get a few-ulp tolerance (see `dense_matches`);
            // everything else stays exact.
            let matches = match (actual_value, expected_value) {
                (VectorValue::Dense(a), VectorValue::Dense(e)) => dense_matches(name, a, e),
                _ => actual_value == expected_value,
            };
            if !matches {
                // Dense mismatches get a per-component diff so a uniform rescale
                // is distinguishable from noise at a glance.
                let detail = match (actual_value, expected_value) {
                    (VectorValue::Dense(a), VectorValue::Dense(e)) => dense_diff(a, e),
                    _ => String::new(),
                };
                panic!(
                    "{ctx}: vector `{name}` value divergence for id {id:?}: \
                     engine {actual_value:?}, model {expected_value:?}; {detail}",
                );
            }
        }
        assert_eq!(
            actual_entry.payload, expected_entry.payload,
            "{ctx}: payload mismatch for id {id:?}",
        );
    }
}

/// Per-shard newest-clocks recovery point, flattened to `(peer_id, clock_id) -> tick`.
///
/// Captured on both sides of a close+reopen boundary (see [`collect_clock_ticks`] /
/// [`assert_clocks_match`]). Tokens are deliberately dropped: the tick is the semantic content
/// of a clock, tokens only disambiguate same-tick echoes. `BTreeMap`s keep the
/// `assert_clocks_match` panic output deterministically ordered.
pub(super) type ClockTicks = BTreeMap<ShardId, BTreeMap<(PeerId, u32), u64>>;

/// Snapshot every local shard's newest-clocks recovery point.
///
/// Only meaningful at a quiescent boundary (op loop idle, background snapshot drained, and,
/// post-reload, background WAL-tail updates plunged): clock ticks advance synchronously at update
/// submit, so with no op in flight the recovery point is stable.
///
/// Note: the recovery point is derived from the clocks *snapshot* when one is set
/// (`ClockMap::to_recovery_point`), but only the WAL-less shard-transfer flow sets one (never
/// this harness), so this always reads the live clocks.
pub(super) async fn collect_clock_ticks(collection: &Collection) -> ClockTicks {
    let holder = collection.shards_holder.read().await;
    let mut out = ClockTicks::new();
    for (shard_id, replica_set) in holder.get_shards() {
        let recovery_point = replica_set
            .shard_recovery_point()
            .await
            .expect("shard_recovery_point failed");
        let ticks = recovery_point
            .iter_as_clock_tags()
            .map(|tag| ((tag.peer_id, tag.clock_id), tag.clock_tick))
            .collect();
        out.insert(shard_id, ticks);
    }
    out
}

/// Assert the newest-clocks recovery point survived a graceful close+reopen *exactly*.
///
/// The reopened shard reconstructs it from the persisted clock-map file plus WAL replay, and
/// both directions of a mismatch are bugs:
/// - a tick **lost** across reload means clock durability broke (the tick reached neither the
///   stored clock map nor a replayable WAL entry), so the node would under-report what it has
///   seen when negotiating a WAL delta;
/// - a tick **gained** means the reload path over-advanced a clock, claiming ops this node never
///   applied, so a future WAL-delta transfer would silently skip them.
pub(super) fn assert_clocks_match(pre: &ClockTicks, post: &ClockTicks, ctx: &str) {
    assert_eq!(
        pre, post,
        "{ctx}: newest-clocks recovery point changed across close+reopen \
         (left = pre-close, right = post-reload, keyed shard_id -> (peer_id, clock_id) -> tick); \
         lost ticks = clock durability broke, gained ticks = replay over-advanced a clock",
    );
}

/// Returns `(segment_count, total_optimized_points)` summed across every local shard.
pub(super) async fn run_summary(collection: &Collection) -> (usize, usize) {
    // Level4 is needed for the per-segment telemetry to be populated; lower levels return
    // an empty `segments` Vec (see `local_shard/telemetry.rs:24`).
    let detail = TelemetryDetail {
        level: DetailsLevel::Level4,
        histograms: false,
        per_collection: false,
    };
    let telemetry = collection
        .get_telemetry_data(detail, Duration::from_secs(5))
        .await
        .expect("telemetry failed");
    let mut segments = 0;
    let mut optimized = 0;
    for local in telemetry
        .shards
        .iter()
        .flatten()
        .filter_map(|rs| rs.local.as_ref())
    {
        segments += local.segments.as_ref().map(Vec::len).unwrap_or(0);
        optimized += local.total_optimized_points;
    }
    (segments, optimized)
}

/// Drain every local shard's update worker queue.
///
/// After `Collection::load`, `load_from_wal` queues WAL entries past
/// `applied_seq + APPLIED_SEQ_SAVE_INTERVAL` to the update worker without a
/// callback (`local_shard/mod.rs:867-877`). This is **intentional**: in
/// production it makes `Collection::load` return quickly, with the tail of
/// the WAL applied in the background as the service warms up.
///
/// The soak's verification path scrolls immediately after reopen and asserts
/// against the model, so it has to explicitly wait for that background queue
/// to drain — otherwise the scroll sees segment state from before those
/// deferred ops have been applied (recent upserts appear missing, recent
/// deletes appear un-applied).
///
/// Send a `Plunger` to each local shard's update worker and wait for the
/// ack. The update queue is FIFO, so an ack means every op queued before the
/// plunger has been applied to in-memory segment state.
pub(super) async fn wait_for_pending_updates(collection: &Collection) {
    let receivers = {
        let holder = collection.shards_holder.read().await;
        let mut receivers = Vec::with_capacity(holder.all_shards().count());
        for replica_set in holder.all_shards() {
            if let Some(rx) = replica_set
                .plunge_local_async()
                .await
                .expect("plunge_local_async failed")
            {
                receivers.push(rx);
            }
        }
        receivers
    };
    for rx in receivers {
        rx.await.expect("plunger callback dropped");
    }
}

pub(super) async fn wait_for_optimizer(collection: &Collection) {
    let detail = TelemetryDetail {
        level: DetailsLevel::Level3,
        histograms: false,
        per_collection: false,
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        collection.trigger_optimizers().await;
        let telemetry = collection
            .get_telemetry_data(detail, Duration::from_secs(5))
            .await
            .expect("telemetry failed");
        let optimized: usize = telemetry
            .shards
            .iter()
            .flatten()
            .filter_map(|rs| rs.local.as_ref())
            .map(|local| local.total_optimized_points)
            .sum();
        if optimized > 0 {
            log::debug!("optimizer ran: {optimized} points optimized");
            return;
        }
        assert!(
            Instant::now() < deadline,
            "optimizer did not run within 30s",
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
