use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use api::rest::{VectorOutput, VectorStructOutput};
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::types::{DetailsLevel, TelemetryDetail};
use segment::types::{PointIdType, VectorNameBuf, WithPayloadInterface, WithVector};
use shard::scroll::ScrollRequestInternal;

use super::op::canonical_sparse;
use super::{Model, ModelEntry, VectorValue};
use crate::collection::Collection;
use crate::operations::shard_selector_internal::ShardSelectorInternal;

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
            actual_entry.vectors, expected_entry.vectors,
            "{ctx}: vectors mismatch for id {id:?}",
        );
        assert_eq!(
            actual_entry.payload, expected_entry.payload,
            "{ctx}: payload mismatch for id {id:?}",
        );
    }
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
