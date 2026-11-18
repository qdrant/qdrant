use std::sync::Arc;

use super::*;
use crate::ambient::hw::{HwMetric, HwScale, HwSharedDrain, ScoreKind};
use crate::ambient::{AmbientFutureExt, current};

fn profiled(mode: ProfileMode) -> (Profiler, AmbientContext, Arc<HwSharedDrain>) {
    let profiler = Profiler::new(mode);
    let drain = Arc::new(HwSharedDrain::default());
    let ctx = AmbientContext::request_profiled("test", drain.clone(), Some(&profiler));
    (profiler, ctx, drain)
}

fn segment_info(points: usize, unindexed_keys: Option<Vec<String>>) -> SegmentInfo {
    SegmentInfo {
        segment_id: "seg".to_string(),
        appendable: true,
        indexed: false,
        points,
        operation: "search",
        vector_name: Some(String::new()),
        batch_size: 1,
        limit: Some(10),
        unindexed_keys,
    }
}

#[test]
fn unprofiled_scope_is_a_noop() {
    let ctx = AmbientContext::request("test", Arc::default());
    ctx.measure(|| {
        assert_eq!(mode(), None);
        assert!(!is_explain());
        assert!(phase("plan").is_none());
        assert!(ShardScope::start(1, "query_batch").is_none());
        assert!(SegmentScope::start(&current(), || unreachable!()).is_none());
        note_strategy(SearchStrategy::Hnsw, StrategyReason::Unfiltered);
    });
    // Outside of any scope: no context, no panic
    assert_eq!(mode(), None);
}

#[tokio::test]
async fn shard_and_segment_are_collected() {
    let (profiler, ctx, drain) = profiled(ProfileMode::Analyze);

    async {
        assert_eq!(mode(), Some(ProfileMode::Analyze));
        record_phase("plan", std::time::Duration::from_millis(2));

        let shard = ShardScope::start(7, "query_batch").unwrap();
        shard
            .run(async {
                let handoff = current();
                std::thread::scope(|s| {
                    s.spawn(|| {
                        let segment = SegmentScope::start(&handoff, || {
                            segment_info(1000, Some(vec!["city".to_string()]))
                        })
                        .unwrap();
                        segment.handoff().enter(|| {
                            note_strategy(SearchStrategy::FullScan, StrategyReason::NotIndexed);
                            note_cardinality(0, 500, 1000, Vec::new);
                            note_full_scan_threshold(10_000);
                            let scale = HwScale {
                                cpu: 4,
                                vector_io_read: 0,
                                kind: ScoreKind::Original,
                            };
                            scale.cpu(1000);
                            HwMetric::PayloadIoRead.bump(123);
                        });
                        segment.finish();
                    });
                });
                record_phase("merge", std::time::Duration::from_millis(1));
            })
            .await;
    }
    .measured(ctx.clone())
    .await;

    let profile = profiler.finish();
    assert_eq!(profile.version, PROFILE_SCHEMA_VERSION);
    assert_eq!(profile.phases.len(), 1);
    assert_eq!(profile.phases[0].name, "plan");
    assert_eq!(profile.shards.len(), 1);

    let shard = &profile.shards[0];
    assert_eq!(shard.shard_id, 7);
    assert_eq!(shard.location, ShardLocation::Local);
    assert_eq!(shard.phases[0].name, "merge");
    assert_eq!(shard.segments.len(), 1);

    let segment = &shard.segments[0];
    assert_eq!(segment.strategy, Some(SearchStrategy::FullScan));
    assert_eq!(segment.strategy_reason, Some(StrategyReason::NotIndexed));
    assert_eq!(segment.full_scan_threshold, Some(10_000));
    assert_eq!(segment.counters.vector_scores, 1000);
    assert_eq!(segment.counters.quantized_vector_scores, 0);
    assert_eq!(segment.counters.payload_io_read_bytes, 123);
    let filter = segment.filter.as_ref().unwrap();
    assert_eq!(filter.unindexed_keys, vec!["city".to_string()]);
    assert!(filter.full_payload_scan);
    assert_eq!(
        filter.estimated_cardinality,
        Some(CardinalityProfile {
            min: 0,
            exp: 500,
            max: 1000
        })
    );

    // Forks still count for the request and the collection, exactly once
    assert_eq!(ctx.hw_data()[HwMetric::Cpu], 4000);
    assert_eq!(drain.load()[HwMetric::Cpu], 4000);
    assert_eq!(drain.load()[HwMetric::PayloadIoRead], 123);

    assert!(profile.notes.iter().any(|note| note.contains("`city`")));
    assert!(
        profile
            .notes
            .iter()
            .any(|note| note.contains("no vector index"))
    );
}

#[test]
fn remote_profile_is_grafted_with_network_time() {
    let (profiler, ctx, _) = profiled(ProfileMode::Analyze);
    let remote = QueryProfile {
        version: PROFILE_SCHEMA_VERSION,
        mode: ProfileMode::Analyze,
        total_time_ms: 8.0,
        phases: vec![],
        shards: vec![ShardProfile {
            shard_id: 2,
            peer_id: Some(42),
            location: ShardLocation::Local,
            operation: "query_batch".to_string(),
            time_ms: 7.5,
            network_time_ms: None,
            phases: vec![],
            segments: vec![],
        }],
        notes: vec!["remote note".to_string()],
    };
    ctx.measure(|| {
        record_remote(
            2,
            42,
            "query_batch",
            std::time::Duration::from_millis(20),
            remote,
        )
    });
    let profile = profiler.finish();
    let shard = &profile.shards[0];
    assert_eq!(shard.location, ShardLocation::Remote);
    assert_eq!(shard.peer_id, Some(42));
    assert_eq!(shard.network_time_ms, Some(12.0));
    // Notes are derived on the receiving node only
    assert!(!profile.notes.iter().any(|note| note == "remote note"));
}

#[test]
fn explain_mode_is_visible_in_scope_and_notes() {
    let (profiler, ctx, _) = profiled(ProfileMode::Explain);
    ctx.measure(|| assert!(is_explain()));
    assert!(handoff_mode(&Handoff::Measured(ctx)) == Some(ProfileMode::Explain));
    let profile = profiler.finish();
    assert!(
        profile
            .notes
            .iter()
            .any(|note| note.starts_with("Explain mode"))
    );
}

#[test]
fn schema_roundtrips_through_json() {
    let (profiler, _ctx, _) = profiled(ProfileMode::Analyze);
    let profile = profiler.finish();
    let json = serde_json::to_value(&profile).unwrap();
    assert_eq!(json["version"], 1);
    assert_eq!(json["mode"], "analyze");
    let back: QueryProfile = serde_json::from_value(json).unwrap();
    assert_eq!(back, profile);
}
