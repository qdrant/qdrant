use ahash::AHashSet;
use segment::common::operation_error::OperationResult;
use segment::common::reciprocal_rank_fusion::rrf_scoring;
use segment::common::score_fusion::{ScoreFusion, score_fusion};
use segment::types::{Filter, HasIdCondition, ScoredPoint};

use crate::query::FusionInternal;

/// Fuses multiple sources of scored points with the given fusion method, applies
/// the score threshold (if any) and returns the top `limit` results.
pub fn fusion_rescore(
    sources: Vec<Vec<ScoredPoint>>,
    fusion: FusionInternal,
    score_threshold: Option<f32>,
    limit: usize,
) -> OperationResult<Vec<ScoredPoint>> {
    let fused = match fusion {
        FusionInternal::Rrf { k, ref weights } => {
            let weights_slice = weights
                .as_ref()
                .map(|w| w.iter().map(|f| f.into_inner()).collect::<Vec<_>>());
            rrf_scoring(sources, k, weights_slice.as_deref())?
        }
        FusionInternal::Dbsf => score_fusion(sources, ScoreFusion::dbsf()),
    };

    let top_fused: Vec<_> = if let Some(score_threshold) = score_threshold {
        fused
            .into_iter()
            .take_while(|point| point.score >= score_threshold)
            .take(limit)
            .collect()
    } else {
        fused.into_iter().take(limit).collect()
    };

    Ok(top_fused)
}

/// Extracts point ids from sources, and creates a filter to only include those ids.
pub fn filter_with_point_ids<I>(sources: I) -> Filter
where
    I: IntoIterator,
    I::Item: AsRef<[ScoredPoint]>,
{
    let mut point_ids = AHashSet::new();

    for source in sources {
        for point in source.as_ref() {
            point_ids.insert(point.id);
        }
    }

    // create filter for target point ids
    Filter::new_must(segment::types::Condition::HasId(HasIdCondition::from(
        point_ids,
    )))
}

#[cfg(test)]
mod tests {
    use segment::types::{Condition, PointIdType, ScoredPoint};
    use uuid::Uuid;

    use super::*;

    fn point(id: u64, score: f32) -> ScoredPoint {
        ScoredPoint {
            id: PointIdType::from(id),
            version: 0,
            score,
            payload: None,
            vector: None,
            shard_key: None,
            order_value: None,
        }
    }

    fn point_uuid(id: u128, score: f32) -> ScoredPoint {
        ScoredPoint {
            id: PointIdType::Uuid(Uuid::from_u128(id)),
            version: 0,
            score,
            payload: None,
            vector: None,
            shard_key: None,
            order_value: None,
        }
    }

    fn sorted_num_ids(points: &[ScoredPoint]) -> Vec<u64> {
        let mut ids: Vec<u64> = points
            .iter()
            .filter_map(|point| match point.id {
                PointIdType::NumId(num) => Some(num),
                PointIdType::Uuid(_) => None,
            })
            .collect();
        ids.sort_unstable();
        ids
    }

    fn filter_num_ids(filter: &Filter) -> Vec<u64> {
        let conditions = filter.must.as_ref().expect("filter must conditions");
        let Condition::HasId(has_id) = &conditions[0] else {
            panic!("expected HasId condition");
        };
        let mut ids: Vec<u64> = has_id
            .has_id
            .iter()
            .filter_map(|id| match id {
                PointIdType::NumId(num) => Some(*num),
                PointIdType::Uuid(_) => None,
            })
            .collect();
        ids.sort_unstable();
        ids
    }

    /// All ids (numeric and UUID) normalized to point-id string form, sorted.
    fn sorted_id_strings<'a>(ids: impl Iterator<Item = &'a PointIdType>) -> Vec<String> {
        let mut ids: Vec<String> = ids
            .map(|id| match id {
                PointIdType::NumId(num) => num.to_string(),
                // `PointIdType` renders UUIDs hyphenated; `Uuid`'s own
                // `Display` renders them without hyphens.
                PointIdType::Uuid(uuid) => uuid.hyphenated().to_string(),
            })
            .collect();
        ids.sort_unstable();
        ids
    }

    fn filter_with_point_ids_sorted(filter: &Filter) -> Vec<String> {
        let conditions = filter.must.as_ref().expect("filter must conditions");
        let Condition::HasId(has_id) = &conditions[0] else {
            panic!("expected HasId condition");
        };
        sorted_id_strings(has_id.has_id.iter())
    }

    #[test]
    fn fusion_rescore_rrf_combines_sources_and_limits() {
        let sources = vec![
            vec![point(1, 1.0), point(2, 0.9)],
            vec![point(1, 1.0), point(3, 0.8)],
        ];
        let fusion = FusionInternal::Rrf {
            k: 2,
            weights: None,
        };

        // Point 1 appears in both sources, so it ranks first.
        let top = fusion_rescore(sources.clone(), fusion.clone(), None, 1).unwrap();
        assert_eq!(sorted_num_ids(&top), vec![1]);

        // All points are returned when the limit is large enough.
        let all = fusion_rescore(sources, fusion, None, 10).unwrap();
        assert_eq!(sorted_num_ids(&all), vec![1, 2, 3]);
    }

    #[test]
    fn fusion_rescore_rrf_weights_change_ranking() {
        use ordered_float::OrderedFloat;

        // Source 0's only point must outrank source 1's top point: it wins on
        // weight even though source 1 has more candidates.
        let sources = vec![vec![point(3, 1.0)], vec![point(1, 1.0), point(2, 1.0)]];
        let weighted = FusionInternal::Rrf {
            k: 2,
            weights: Some(vec![OrderedFloat(5.0), OrderedFloat(1.0)]),
        };

        let result = fusion_rescore(sources, weighted, None, 10).unwrap();
        assert_eq!(
            sorted_num_ids(&result),
            vec![1, 2, 3],
            "all points must survive weighted fusion"
        );
        assert_eq!(
            result[0].id,
            PointIdType::from(3),
            "the heavier-weighted source's point must rank first",
        );
    }

    #[test]
    fn fusion_rescore_rrf_rejects_mismatched_weights() {
        let sources = vec![vec![point(1, 1.0)], vec![point(2, 1.0)]];
        let fusion = FusionInternal::Rrf {
            k: 2,
            weights: Some(vec![ordered_float::OrderedFloat(1.0)]),
        };

        let result = fusion_rescore(sources, fusion, None, 10);
        assert!(
            result.is_err(),
            "weights must match the number of sources: {result:?}",
        );
    }

    #[test]
    fn fusion_rescore_preserves_uuid_ids() {
        let uuid_a = 0x1234_5678_9abc_def0_u128;
        let uuid_b = 0xfedc_ba98_7654_3210_u128;

        let sources = vec![
            vec![point_uuid(uuid_a, 1.0), point(7, 0.9)],
            vec![point_uuid(uuid_b, 1.0), point(7, 0.8)],
        ];

        let result = fusion_rescore(
            sources,
            FusionInternal::Rrf {
                k: 2,
                weights: None,
            },
            None,
            10,
        )
        .unwrap();
        let ids = sorted_id_strings(result.iter().map(|point| &point.id));

        assert_eq!(ids.len(), 3, "uuid ids must not be dropped: {ids:?}");
        assert!(
            ids.contains(&PointIdType::from(7).to_string()),
            "mixed numeric id must survive fusion: {ids:?}",
        );
        assert!(
            ids.contains(&PointIdType::Uuid(Uuid::from_u128(uuid_a)).to_string()),
            "uuid_a must survive fusion: {ids:?}",
        );
        assert!(
            ids.contains(&PointIdType::Uuid(Uuid::from_u128(uuid_b)).to_string()),
            "uuid_b must survive fusion: {ids:?}",
        );
    }

    #[test]
    fn fusion_rescore_rrf_applies_score_threshold() {
        let sources = vec![
            vec![point(1, 1.0), point(2, 0.9)],
            vec![point(1, 1.0), point(3, 0.8)],
        ];
        let fusion = FusionInternal::Rrf {
            k: 2,
            weights: None,
        };

        // Only the point present in both sources passes the threshold.
        let result = fusion_rescore(sources, fusion, Some(0.5), 10).unwrap();
        assert_eq!(sorted_num_ids(&result), vec![1]);
    }

    #[test]
    fn fusion_rescore_dbsf_fuses_all_points() {
        let sources = vec![
            vec![point(1, 2.0), point(2, 1.0)],
            vec![point(2, 3.0), point(3, 0.5)],
        ];

        let result = fusion_rescore(sources, FusionInternal::Dbsf, None, 10).unwrap();
        assert_eq!(sorted_num_ids(&result), vec![1, 2, 3]);
    }

    #[test]
    fn filter_with_point_ids_empty_input_yields_empty_filter() {
        let filter = filter_with_point_ids(Vec::<Vec<ScoredPoint>>::new());
        assert!(filter_num_ids(&filter).is_empty());
    }

    #[test]
    fn filter_with_point_ids_deduplicates_across_sources() {
        let sources = vec![
            vec![point(1, 1.0), point(2, 1.0)],
            vec![point(2, 1.0), point(3, 1.0)],
        ];

        let filter = filter_with_point_ids(&sources);
        assert_eq!(filter_num_ids(&filter), vec![1, 2, 3]);
    }

    #[test]
    fn filter_with_point_ids_preserves_and_deduplicates_uuids() {
        let uuid_a = 0x1234_5678_9abc_def0_u128;
        let uuid_b = 0xfedc_ba98_7654_3210_u128;

        // Numeric id 2 and uuid_a are each duplicated across sources.
        let sources = vec![
            vec![point(1, 1.0), point_uuid(uuid_a, 1.0)],
            vec![point(2, 1.0), point_uuid(uuid_a, 1.0)],
            vec![point_uuid(uuid_b, 1.0), point(2, 1.0)],
        ];

        let filter = filter_with_point_ids(&sources);
        assert_eq!(
            filter_with_point_ids_sorted(&filter),
            vec![
                Uuid::from_u128(uuid_a).hyphenated().to_string(),
                Uuid::from_u128(uuid_b).hyphenated().to_string(),
                PointIdType::from(1).to_string(),
                PointIdType::from(2).to_string(),
            ],
            "uuids must be preserved and deduplicated alongside numeric ids",
        );
    }
}
