//! Conversions between the query profile model and its gRPC messages.
//!
//! Both directions are needed: the public API returns profiles, and the internal API carries
//! the profiles of remote shards back to the receiving node. Every field is converted
//! explicitly, so a field added on one side breaks the build until added on the other.

use common::ambient::profile as model;
use tonic::Status;

use super::qdrant as grpc;

impl From<model::ProfileMode> for grpc::ProfileMode {
    fn from(value: model::ProfileMode) -> Self {
        match value {
            model::ProfileMode::Analyze => grpc::ProfileMode::ProfileAnalyze,
            model::ProfileMode::Explain => grpc::ProfileMode::ProfileExplain,
        }
    }
}

impl From<grpc::ProfileMode> for model::ProfileMode {
    fn from(value: grpc::ProfileMode) -> Self {
        match value {
            grpc::ProfileMode::ProfileAnalyze => model::ProfileMode::Analyze,
            grpc::ProfileMode::ProfileExplain => model::ProfileMode::Explain,
        }
    }
}

/// Parse the `profile` field of a request.
pub fn profile_mode_from_grpc(value: Option<i32>) -> Result<Option<model::ProfileMode>, Status> {
    value
        .map(|value| {
            grpc::ProfileMode::try_from(value)
                .map(model::ProfileMode::from)
                .map_err(|_| Status::invalid_argument(format!("unknown profile mode {value}")))
        })
        .transpose()
}

/// The `profile` field of an internal request.
pub fn profile_mode_to_grpc(value: Option<model::ProfileMode>) -> Option<i32> {
    value.map(|mode| grpc::ProfileMode::from(mode) as i32)
}

impl From<model::QueryProfile> for grpc::QueryProfile {
    fn from(value: model::QueryProfile) -> Self {
        let model::QueryProfile {
            version,
            mode,
            total_time_ms,
            phases,
            shards,
            notes,
        } = value;
        Self {
            version,
            mode: grpc::ProfileMode::from(mode) as i32,
            total_time_ms,
            phases: phases.into_iter().map(grpc::PhaseProfile::from).collect(),
            shards: shards.into_iter().map(grpc::ShardProfile::from).collect(),
            notes,
        }
    }
}

impl TryFrom<grpc::QueryProfile> for model::QueryProfile {
    type Error = Status;

    fn try_from(value: grpc::QueryProfile) -> Result<Self, Status> {
        let grpc::QueryProfile {
            version,
            mode,
            total_time_ms,
            phases,
            shards,
            notes,
        } = value;
        Ok(Self {
            version,
            mode: grpc::ProfileMode::try_from(mode)
                .map(model::ProfileMode::from)
                .map_err(|_| Status::invalid_argument("unknown profile mode"))?,
            total_time_ms,
            phases: phases.into_iter().map(model::PhaseProfile::from).collect(),
            shards: shards
                .into_iter()
                .map(model::ShardProfile::try_from)
                .collect::<Result<_, _>>()?,
            notes,
        })
    }
}

impl From<model::PhaseProfile> for grpc::PhaseProfile {
    fn from(value: model::PhaseProfile) -> Self {
        let model::PhaseProfile {
            name,
            time_ms,
            calls,
        } = value;
        Self {
            name,
            time_ms,
            calls,
        }
    }
}

impl From<grpc::PhaseProfile> for model::PhaseProfile {
    fn from(value: grpc::PhaseProfile) -> Self {
        let grpc::PhaseProfile {
            name,
            time_ms,
            calls,
        } = value;
        Self {
            name,
            time_ms,
            calls,
        }
    }
}

impl From<model::ShardProfile> for grpc::ShardProfile {
    fn from(value: model::ShardProfile) -> Self {
        let model::ShardProfile {
            shard_id,
            peer_id,
            location,
            operation,
            time_ms,
            network_time_ms,
            phases,
            segments,
        } = value;
        Self {
            shard_id,
            peer_id,
            location: match location {
                model::ShardLocation::Local => grpc::ShardLocation::ShardLocal,
                model::ShardLocation::Remote => grpc::ShardLocation::ShardRemote,
            } as i32,
            operation,
            time_ms,
            network_time_ms,
            phases: phases.into_iter().map(grpc::PhaseProfile::from).collect(),
            segments: segments
                .into_iter()
                .map(grpc::SegmentProfile::from)
                .collect(),
        }
    }
}

impl TryFrom<grpc::ShardProfile> for model::ShardProfile {
    type Error = Status;

    fn try_from(value: grpc::ShardProfile) -> Result<Self, Status> {
        let grpc::ShardProfile {
            shard_id,
            peer_id,
            location,
            operation,
            time_ms,
            network_time_ms,
            phases,
            segments,
        } = value;
        let location = match grpc::ShardLocation::try_from(location) {
            Ok(grpc::ShardLocation::ShardLocal) => model::ShardLocation::Local,
            Ok(grpc::ShardLocation::ShardRemote) => model::ShardLocation::Remote,
            Err(_) => return Err(Status::invalid_argument("unknown shard location")),
        };
        Ok(Self {
            shard_id,
            peer_id,
            location,
            operation,
            time_ms,
            network_time_ms,
            phases: phases.into_iter().map(model::PhaseProfile::from).collect(),
            segments: segments
                .into_iter()
                .map(model::SegmentProfile::try_from)
                .collect::<Result<_, _>>()?,
        })
    }
}

fn strategy_to_grpc(value: model::SearchStrategy) -> grpc::SearchStrategy {
    match value {
        model::SearchStrategy::Hnsw => grpc::SearchStrategy::StrategyHnsw,
        model::SearchStrategy::HnswFiltered => grpc::SearchStrategy::StrategyHnswFiltered,
        model::SearchStrategy::Acorn => grpc::SearchStrategy::StrategyAcorn,
        model::SearchStrategy::PayloadIndexFirst => grpc::SearchStrategy::StrategyPayloadIndexFirst,
        model::SearchStrategy::FullScan => grpc::SearchStrategy::StrategyFullScan,
        model::SearchStrategy::Exact => grpc::SearchStrategy::StrategyExact,
        model::SearchStrategy::SparseIndex => grpc::SearchStrategy::StrategySparseIndex,
        model::SearchStrategy::IndexedRead => grpc::SearchStrategy::StrategyIndexedRead,
        model::SearchStrategy::FullScanRead => grpc::SearchStrategy::StrategyFullScanRead,
        model::SearchStrategy::UnfilteredRead => grpc::SearchStrategy::StrategyUnfilteredRead,
    }
}

fn strategy_from_grpc(value: grpc::SearchStrategy) -> model::SearchStrategy {
    match value {
        grpc::SearchStrategy::StrategyHnsw => model::SearchStrategy::Hnsw,
        grpc::SearchStrategy::StrategyHnswFiltered => model::SearchStrategy::HnswFiltered,
        grpc::SearchStrategy::StrategyAcorn => model::SearchStrategy::Acorn,
        grpc::SearchStrategy::StrategyPayloadIndexFirst => model::SearchStrategy::PayloadIndexFirst,
        grpc::SearchStrategy::StrategyFullScan => model::SearchStrategy::FullScan,
        grpc::SearchStrategy::StrategyExact => model::SearchStrategy::Exact,
        grpc::SearchStrategy::StrategySparseIndex => model::SearchStrategy::SparseIndex,
        grpc::SearchStrategy::StrategyIndexedRead => model::SearchStrategy::IndexedRead,
        grpc::SearchStrategy::StrategyFullScanRead => model::SearchStrategy::FullScanRead,
        grpc::SearchStrategy::StrategyUnfilteredRead => model::SearchStrategy::UnfilteredRead,
    }
}

fn reason_to_grpc(value: model::StrategyReason) -> grpc::StrategyReason {
    match value {
        model::StrategyReason::NotIndexed => grpc::StrategyReason::ReasonNotIndexed,
        model::StrategyReason::HnswDisabled => grpc::StrategyReason::ReasonHnswDisabled,
        model::StrategyReason::ExactRequested => grpc::StrategyReason::ReasonExactRequested,
        model::StrategyReason::SmallSegment => grpc::StrategyReason::ReasonSmallSegment,
        model::StrategyReason::CardinalityBelowThreshold => {
            grpc::StrategyReason::ReasonCardinalityBelowThreshold
        }
        model::StrategyReason::CardinalityAboveThreshold => {
            grpc::StrategyReason::ReasonCardinalityAboveThreshold
        }
        model::StrategyReason::SampledCardinality => grpc::StrategyReason::ReasonSampledCardinality,
        model::StrategyReason::AcornSelectivity => grpc::StrategyReason::ReasonAcornSelectivity,
        model::StrategyReason::Unfiltered => grpc::StrategyReason::ReasonUnfiltered,
    }
}

fn reason_from_grpc(value: grpc::StrategyReason) -> model::StrategyReason {
    match value {
        grpc::StrategyReason::ReasonNotIndexed => model::StrategyReason::NotIndexed,
        grpc::StrategyReason::ReasonHnswDisabled => model::StrategyReason::HnswDisabled,
        grpc::StrategyReason::ReasonExactRequested => model::StrategyReason::ExactRequested,
        grpc::StrategyReason::ReasonSmallSegment => model::StrategyReason::SmallSegment,
        grpc::StrategyReason::ReasonCardinalityBelowThreshold => {
            model::StrategyReason::CardinalityBelowThreshold
        }
        grpc::StrategyReason::ReasonCardinalityAboveThreshold => {
            model::StrategyReason::CardinalityAboveThreshold
        }
        grpc::StrategyReason::ReasonSampledCardinality => model::StrategyReason::SampledCardinality,
        grpc::StrategyReason::ReasonAcornSelectivity => model::StrategyReason::AcornSelectivity,
        grpc::StrategyReason::ReasonUnfiltered => model::StrategyReason::Unfiltered,
    }
}

impl From<model::SegmentProfile> for grpc::SegmentProfile {
    fn from(value: model::SegmentProfile) -> Self {
        let model::SegmentProfile {
            segment_id,
            appendable,
            indexed,
            points,
            operation,
            vector_name,
            batch_size,
            limit,
            strategy,
            strategy_reason,
            full_scan_threshold,
            filter,
            time_ms,
            counters,
        } = value;
        Self {
            segment_id,
            appendable,
            indexed,
            points,
            operation,
            vector_name,
            batch_size,
            limit,
            strategy: strategy.map(|s| strategy_to_grpc(s) as i32),
            strategy_reason: strategy_reason.map(|r| reason_to_grpc(r) as i32),
            full_scan_threshold,
            filter: filter.map(grpc::FilterProfile::from),
            time_ms,
            counters: Some(grpc::SegmentCounters::from(counters)),
        }
    }
}

impl TryFrom<grpc::SegmentProfile> for model::SegmentProfile {
    type Error = Status;

    fn try_from(value: grpc::SegmentProfile) -> Result<Self, Status> {
        let grpc::SegmentProfile {
            segment_id,
            appendable,
            indexed,
            points,
            operation,
            vector_name,
            batch_size,
            limit,
            strategy,
            strategy_reason,
            full_scan_threshold,
            filter,
            time_ms,
            counters,
        } = value;
        let strategy = strategy
            .map(|s| {
                grpc::SearchStrategy::try_from(s)
                    .map(strategy_from_grpc)
                    .map_err(|_| Status::invalid_argument("unknown search strategy"))
            })
            .transpose()?;
        let strategy_reason = strategy_reason
            .map(|r| {
                grpc::StrategyReason::try_from(r)
                    .map(reason_from_grpc)
                    .map_err(|_| Status::invalid_argument("unknown strategy reason"))
            })
            .transpose()?;
        Ok(Self {
            segment_id,
            appendable,
            indexed,
            points,
            operation,
            vector_name,
            batch_size,
            limit,
            strategy,
            strategy_reason,
            full_scan_threshold,
            filter: filter.map(model::FilterProfile::from),
            time_ms,
            counters: counters
                .map(model::SegmentCounters::from)
                .unwrap_or_default(),
        })
    }
}

impl From<model::FilterProfile> for grpc::FilterProfile {
    fn from(value: model::FilterProfile) -> Self {
        let model::FilterProfile {
            estimated_cardinality,
            actual_cardinality,
            indexed_keys,
            unindexed_keys,
            full_payload_scan,
        } = value;
        Self {
            estimated_cardinality: estimated_cardinality.map(|c| {
                let model::CardinalityProfile { min, exp, max } = c;
                grpc::CardinalityProfile { min, exp, max }
            }),
            actual_cardinality,
            indexed_keys,
            unindexed_keys,
            full_payload_scan,
        }
    }
}

impl From<grpc::FilterProfile> for model::FilterProfile {
    fn from(value: grpc::FilterProfile) -> Self {
        let grpc::FilterProfile {
            estimated_cardinality,
            actual_cardinality,
            indexed_keys,
            unindexed_keys,
            full_payload_scan,
        } = value;
        Self {
            estimated_cardinality: estimated_cardinality.map(|c| {
                let grpc::CardinalityProfile { min, exp, max } = c;
                model::CardinalityProfile { min, exp, max }
            }),
            actual_cardinality,
            indexed_keys,
            unindexed_keys,
            full_payload_scan,
        }
    }
}

impl From<model::SegmentCounters> for grpc::SegmentCounters {
    fn from(value: model::SegmentCounters) -> Self {
        let model::SegmentCounters {
            vector_scores,
            quantized_vector_scores,
            vector_io_read_bytes,
            payload_io_read_bytes,
            payload_index_io_read_bytes,
            remote_io_cached_read_bytes,
            remote_io_non_cached_read_bytes,
        } = value;
        Self {
            vector_scores,
            quantized_vector_scores,
            vector_io_read_bytes,
            payload_io_read_bytes,
            payload_index_io_read_bytes,
            remote_io_cached_read_bytes,
            remote_io_non_cached_read_bytes,
        }
    }
}

impl From<grpc::SegmentCounters> for model::SegmentCounters {
    fn from(value: grpc::SegmentCounters) -> Self {
        let grpc::SegmentCounters {
            vector_scores,
            quantized_vector_scores,
            vector_io_read_bytes,
            payload_io_read_bytes,
            payload_index_io_read_bytes,
            remote_io_cached_read_bytes,
            remote_io_non_cached_read_bytes,
        } = value;
        Self {
            vector_scores,
            quantized_vector_scores,
            vector_io_read_bytes,
            payload_io_read_bytes,
            payload_index_io_read_bytes,
            remote_io_cached_read_bytes,
            remote_io_non_cached_read_bytes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_roundtrips_through_grpc() {
        let profile = model::QueryProfile {
            version: model::PROFILE_SCHEMA_VERSION,
            mode: model::ProfileMode::Analyze,
            total_time_ms: 12.5,
            phases: vec![model::PhaseProfile {
                name: "merge".to_string(),
                time_ms: 0.25,
                calls: 2,
            }],
            shards: vec![model::ShardProfile {
                shard_id: 3,
                peer_id: Some(7),
                location: model::ShardLocation::Remote,
                operation: "query_batch".to_string(),
                time_ms: 10.0,
                network_time_ms: Some(2.0),
                phases: vec![],
                segments: vec![model::SegmentProfile {
                    segment_id: "a".to_string(),
                    appendable: true,
                    indexed: false,
                    points: 100,
                    operation: "search".to_string(),
                    vector_name: Some("".to_string()),
                    batch_size: 1,
                    limit: Some(10),
                    strategy: Some(model::SearchStrategy::PayloadIndexFirst),
                    strategy_reason: Some(model::StrategyReason::CardinalityBelowThreshold),
                    full_scan_threshold: Some(1000),
                    filter: Some(model::FilterProfile {
                        estimated_cardinality: Some(model::CardinalityProfile {
                            min: 1,
                            exp: 2,
                            max: 3,
                        }),
                        actual_cardinality: Some(2),
                        indexed_keys: vec!["city".to_string()],
                        unindexed_keys: vec!["price".to_string()],
                        full_payload_scan: false,
                    }),
                    time_ms: 1.5,
                    counters: model::SegmentCounters {
                        vector_scores: 2,
                        quantized_vector_scores: 0,
                        vector_io_read_bytes: 0,
                        payload_io_read_bytes: 64,
                        payload_index_io_read_bytes: 8,
                        remote_io_cached_read_bytes: 0,
                        remote_io_non_cached_read_bytes: 0,
                    },
                }],
            }],
            notes: vec!["note".to_string()],
        };
        let grpc = grpc::QueryProfile::from(profile.clone());
        assert_eq!(model::QueryProfile::try_from(grpc).unwrap(), profile);
    }
}
