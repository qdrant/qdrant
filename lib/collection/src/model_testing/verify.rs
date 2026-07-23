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

/// Force-flush every local shard synchronously so all applied state is on disk.
///
/// The Edge follower's visibility contract is "flushed, then refreshed": segment files, the
/// mutable id tracker, and payload data reach disk only on flush, so every edge checkpoint
/// flushes first. Only meaningful at a quiescent boundary (op loop idle, background
/// snapshot drained), same as [`collect_clock_ticks`].
pub(super) async fn flush_all_local_shards(collection: &Collection) {
    let holder = collection.shards_holder.read().await;
    for (_shard_id, replica_set) in holder.get_shards() {
        replica_set.full_flush_local().await;
    }
}

/// Whether any local shard currently holds proxy segments, i.e. an optimization is in
/// flight. Deletes against a proxied segment live only in the proxy's in-memory map (and
/// the WAL) until the optimization finishes — no flush can put them into segment files —
/// so edge checkpoints skip the comparison while this is true.
pub(super) async fn has_proxy_segments(collection: &Collection) -> bool {
    let holder = collection.shards_holder.read().await;
    for (_shard_id, replica_set) in holder.get_shards() {
        if replica_set.local_has_proxy_segments().await {
            return true;
        }
    }
    false
}
