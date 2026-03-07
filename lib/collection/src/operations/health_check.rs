use super::types::{
    CollectionHealthReport, CollectionInfo, HealthRecommendation, HealthSummary,
    RecommendationCategory, RecommendationSeverity,
};

/// Analyze a collection's current state and return actionable tuning suggestions.
pub fn analyze_collection_health(
    collection_name: &str,
    info: &CollectionInfo,
) -> CollectionHealthReport {
    let mut recommendations = Vec::new();

    check_indexed_vector_ratio(info, &mut recommendations);
    check_segment_count(info, &mut recommendations);
    check_quantization(info, &mut recommendations);
    check_hnsw_ef_construct(info, &mut recommendations);
    check_wal_capacity(info, &mut recommendations);
    check_payload_indexes(info, &mut recommendations);

    let summary = HealthSummary {
        critical: recommendations
            .iter()
            .filter(|r| r.severity == RecommendationSeverity::Critical)
            .count(),
        warning: recommendations
            .iter()
            .filter(|r| r.severity == RecommendationSeverity::Warning)
            .count(),
        info: recommendations
            .iter()
            .filter(|r| r.severity == RecommendationSeverity::Info)
            .count(),
    };

    CollectionHealthReport {
        collection_name: collection_name.to_string(),
        recommendations,
        summary,
    }
}

/// Check if the indexed vector ratio is too low.
fn check_indexed_vector_ratio(info: &CollectionInfo, recs: &mut Vec<HealthRecommendation>) {
    let Some(points_count) = info.points_count else {
        return;
    };
    let Some(indexed_count) = info.indexed_vectors_count else {
        return;
    };
    if points_count == 0 {
        return;
    }

    let vectors_num = info.config.params.vectors.vectors_num();
    let total_vectors = points_count * vectors_num;
    if total_vectors == 0 {
        return;
    }

    let ratio = indexed_count as f64 / total_vectors as f64;

    if ratio < 0.5 {
        recs.push(HealthRecommendation {
            severity: RecommendationSeverity::Critical,
            category: RecommendationCategory::Indexing,
            message: format!(
                "Only {:.0}% of vectors are indexed ({indexed_count}/{total_vectors}).",
                ratio * 100.0,
            ),
            suggestion:
                "Wait for the optimizer to finish indexing, or lower `indexing_threshold` in optimizer config."
                    .to_string(),
        });
    } else if ratio < 0.8 {
        recs.push(HealthRecommendation {
            severity: RecommendationSeverity::Warning,
            category: RecommendationCategory::Indexing,
            message: format!(
                "Only {:.0}% of vectors are indexed ({indexed_count}/{total_vectors}).",
                ratio * 100.0,
            ),
            suggestion:
                "Wait for the optimizer to finish indexing, or lower `indexing_threshold` in optimizer config."
                    .to_string(),
        });
    }
}

/// Check if the segment count is too high (fragmentation).
fn check_segment_count(info: &CollectionInfo, recs: &mut Vec<HealthRecommendation>) {
    let segments = info.segments_count;

    if segments > 50 {
        recs.push(HealthRecommendation {
            severity: RecommendationSeverity::Critical,
            category: RecommendationCategory::Segmentation,
            message: format!("Collection has {segments} segments, which indicates heavy fragmentation."),
            suggestion:
                "Consider adjusting `max_segment_size` and `default_segment_number` in optimizer config, or trigger optimization."
                    .to_string(),
        });
    } else if segments > 20 {
        recs.push(HealthRecommendation {
            severity: RecommendationSeverity::Warning,
            category: RecommendationCategory::Segmentation,
            message: format!("Collection has {segments} segments, which may cause suboptimal search performance."),
            suggestion:
                "Consider adjusting optimizer config to reduce segment count, or wait for background optimization."
                    .to_string(),
        });
    }
}

/// Check if quantization should be enabled for large collections.
fn check_quantization(info: &CollectionInfo, recs: &mut Vec<HealthRecommendation>) {
    let Some(points_count) = info.points_count else {
        return;
    };
    if points_count < 100_000 {
        return;
    }

    // Check collection-level quantization
    if info.config.quantization_config.is_some() {
        return;
    }

    // Check per-vector quantization
    let has_per_vector_quantization = info
        .config
        .params
        .vectors
        .params_iter()
        .any(|(_, params)| params.quantization_config.is_some());

    if !has_per_vector_quantization {
        recs.push(HealthRecommendation {
            severity: RecommendationSeverity::Info,
            category: RecommendationCategory::Memory,
            message: format!(
                "Collection has {points_count} points without quantization enabled."
            ),
            suggestion:
                "Enable scalar or product quantization to reduce RAM usage by up to 75% and potentially improve search speed."
                    .to_string(),
        });
    }
}

/// Check if HNSW ef_construct is appropriate for the dataset size.
fn check_hnsw_ef_construct(info: &CollectionInfo, recs: &mut Vec<HealthRecommendation>) {
    let Some(points_count) = info.points_count else {
        return;
    };

    let ef_construct = info.config.hnsw_config.ef_construct;

    if points_count > 1_000_000 && ef_construct < 200 {
        recs.push(HealthRecommendation {
            severity: RecommendationSeverity::Warning,
            category: RecommendationCategory::HnswConfig,
            message: format!(
                "Collection has {points_count} points but ef_construct is only {ef_construct}."
            ),
            suggestion:
                "Consider increasing `ef_construct` to 200-512 for better recall on large datasets. Note: this requires re-indexing."
                    .to_string(),
        });
    } else if points_count > 100_000 && ef_construct < 100 {
        recs.push(HealthRecommendation {
            severity: RecommendationSeverity::Info,
            category: RecommendationCategory::HnswConfig,
            message: format!(
                "Collection has {points_count} points but ef_construct is {ef_construct}."
            ),
            suggestion:
                "Consider increasing `ef_construct` for better recall. Note: this requires re-indexing."
                    .to_string(),
        });
    }
}

/// Check if WAL capacity is too small for the collection size.
fn check_wal_capacity(info: &CollectionInfo, recs: &mut Vec<HealthRecommendation>) {
    let Some(points_count) = info.points_count else {
        return;
    };
    let Some(wal_config) = &info.config.wal_config else {
        return;
    };

    if points_count > 500_000 && wal_config.wal_capacity_mb < 32 {
        recs.push(HealthRecommendation {
            severity: RecommendationSeverity::Warning,
            category: RecommendationCategory::WalConfig,
            message: format!(
                "WAL segment capacity is {}MB for a collection with {points_count} points.",
                wal_config.wal_capacity_mb,
            ),
            suggestion:
                "Increase `wal_capacity_mb` (default is 32) to reduce WAL segment rotation overhead."
                    .to_string(),
        });
    }
}

/// Check if payload indexes are missing on large collections.
fn check_payload_indexes(info: &CollectionInfo, recs: &mut Vec<HealthRecommendation>) {
    let Some(points_count) = info.points_count else {
        return;
    };

    if points_count > 10_000 && info.payload_schema.is_empty() {
        recs.push(HealthRecommendation {
            severity: RecommendationSeverity::Info,
            category: RecommendationCategory::PayloadIndex,
            message: format!(
                "Collection has {points_count} points but no payload indexes."
            ),
            suggestion:
                "If you use filters in search queries, create payload indexes on filtered fields to improve performance."
                    .to_string(),
        });
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::num::{NonZeroU32, NonZeroU64};

    use segment::types::{Distance, HnswConfig};

    use super::*;
    use crate::config::{CollectionParams, WalConfig, default_on_disk_payload};
    use crate::operations::types::{
        CollectionConfig, CollectionInfo, CollectionStatus, OptimizersStatus, VectorParams,
        VectorsConfig,
    };

    fn default_optimizers_config() -> crate::optimizers_builder::OptimizersConfig {
        serde_json::from_str("{}").unwrap()
    }

    fn make_collection_params() -> CollectionParams {
        CollectionParams {
            vectors: VectorsConfig::Single(VectorParams {
                size: NonZeroU64::new(4).unwrap(),
                distance: Distance::Cosine,
                hnsw_config: None,
                quantization_config: None,
                on_disk: None,
                datatype: None,
                multivector_config: None,
            }),
            shard_number: NonZeroU32::new(1).unwrap(),
            sharding_method: None,
            replication_factor: NonZeroU32::new(1).unwrap(),
            write_consistency_factor: NonZeroU32::new(1).unwrap(),
            read_fan_out_factor: None,
            read_fan_out_delay_ms: None,
            on_disk_payload: default_on_disk_payload(),
            sparse_vectors: None,
        }
    }

    fn make_test_info(
        points_count: Option<usize>,
        indexed_vectors_count: Option<usize>,
        segments_count: usize,
        ef_construct: usize,
        wal_capacity_mb: usize,
    ) -> CollectionInfo {
        CollectionInfo {
            status: CollectionStatus::Green,
            optimizer_status: OptimizersStatus::Ok,
            warnings: vec![],
            indexed_vectors_count,
            points_count,
            segments_count,
            config: CollectionConfig {
                params: make_collection_params(),
                hnsw_config: HnswConfig {
                    ef_construct,
                    ..HnswConfig::default()
                },
                optimizer_config: default_optimizers_config(),
                wal_config: Some(WalConfig {
                    wal_capacity_mb,
                    wal_segments_ahead: 0,
                    wal_retain_closed: 1,
                }),
                quantization_config: None,
                strict_mode_config: None,
                metadata: None,
            },
            payload_schema: HashMap::new(),
            update_queue: None,
        }
    }

    #[test]
    fn test_healthy_collection_no_recommendations() {
        let info = make_test_info(Some(100), Some(100), 3, 200, 64);
        let report = analyze_collection_health("test", &info);
        assert!(report.recommendations.is_empty());
    }

    #[test]
    fn test_low_indexed_ratio_critical() {
        let info = make_test_info(Some(1000), Some(300), 3, 200, 64);
        let report = analyze_collection_health("test", &info);
        assert!(report.recommendations.iter().any(|r| {
            r.severity == RecommendationSeverity::Critical
                && r.category == RecommendationCategory::Indexing
        }));
    }

    #[test]
    fn test_high_segment_count_warning() {
        let info = make_test_info(Some(1000), Some(1000), 25, 200, 64);
        let report = analyze_collection_health("test", &info);
        assert!(report.recommendations.iter().any(|r| {
            r.severity == RecommendationSeverity::Warning
                && r.category == RecommendationCategory::Segmentation
        }));
    }

    #[test]
    fn test_large_collection_no_quantization() {
        let info = make_test_info(Some(200_000), Some(200_000), 3, 200, 64);
        let report = analyze_collection_health("test", &info);
        assert!(report.recommendations.iter().any(|r| {
            r.severity == RecommendationSeverity::Info
                && r.category == RecommendationCategory::Memory
        }));
    }

    #[test]
    fn test_low_ef_construct_warning() {
        let info = make_test_info(Some(2_000_000), Some(2_000_000), 3, 100, 64);
        let report = analyze_collection_health("test", &info);
        assert!(report.recommendations.iter().any(|r| {
            r.severity == RecommendationSeverity::Warning
                && r.category == RecommendationCategory::HnswConfig
        }));
    }

    #[test]
    fn test_small_wal_capacity_warning() {
        let info = make_test_info(Some(1_000_000), Some(1_000_000), 3, 200, 16);
        let report = analyze_collection_health("test", &info);
        assert!(report.recommendations.iter().any(|r| {
            r.severity == RecommendationSeverity::Warning
                && r.category == RecommendationCategory::WalConfig
        }));
    }

    #[test]
    fn test_no_payload_indexes_info() {
        let info = make_test_info(Some(50_000), Some(50_000), 3, 200, 64);
        let report = analyze_collection_health("test", &info);
        assert!(report.recommendations.iter().any(|r| {
            r.severity == RecommendationSeverity::Info
                && r.category == RecommendationCategory::PayloadIndex
        }));
    }

    #[test]
    fn test_summary_counts() {
        let info = make_test_info(Some(2_000_000), Some(500_000), 55, 100, 16);
        let report = analyze_collection_health("test", &info);
        assert_eq!(
            report.summary.critical + report.summary.warning + report.summary.info,
            report.recommendations.len()
        );
        assert!(report.summary.critical > 0);
    }
}
