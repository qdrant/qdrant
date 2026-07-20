use ahash::AHashSet;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::data_types::vectors::NamedQuery;
use segment::types::{
    Condition, Filter, HasIdCondition, HasVectorCondition, PointIdType, ScoredPoint,
    WithPayloadInterface, WithVector,
};
use shard::query::query_enum::QueryEnum;
use shard::query::{SampleInternal, ScoringQuery, ShardQueryRequest};

use crate::read_view::{EdgeReadView, ReadSegmentHandle};
use crate::requests::SearchMatrixRequest;

#[derive(Debug, Default)]
pub struct SearchMatrixResponse {
    pub sample_ids: Vec<PointIdType>,
    pub nearests: Vec<Vec<ScoredPoint>>,
}

impl<H: ReadSegmentHandle> EdgeReadView<H> {
    pub(crate) fn search_matrix(
        &self,
        request: SearchMatrixRequest,
    ) -> OperationResult<SearchMatrixResponse> {
        let SearchMatrixRequest {
            sample_size,
            limit_per_sample,
            filter,
            using,
        } = request;
        if sample_size == 0 || limit_per_sample == 0 {
            return Ok(SearchMatrixResponse::default());
        }

        // Only sample points that actually carry the `using` vector.
        let has_vector = Filter::new_must(Condition::HasVector(HasVectorCondition::from(
            using.clone(),
        )));
        let sample_filter = Some(filter.map(|f| f.merge(&has_vector)).unwrap_or(has_vector));

        let sampling = ShardQueryRequest {
            prefetches: vec![],
            query: Some(ScoringQuery::Sample(SampleInternal::Random)),
            filter: sample_filter,
            score_threshold: None,
            limit: sample_size,
            offset: 0,
            params: None,
            with_vector: WithVector::Selector(vec![using.clone()]),
            with_payload: WithPayloadInterface::Bool(false),
        };
        let mut sampled = self.query(sampling)?;
        if sampled.len() < 2 {
            return Ok(SearchMatrixResponse::default());
        }
        sampled.truncate(sample_size);
        sampled.sort_unstable_by_key(|p| p.id);
        let sample_ids: Vec<PointIdType> = sampled.iter().map(|p| p.id).collect();

        // Restrict each nearest search to the sampled set.
        let id_filter = Filter::new_must(Condition::HasId(HasIdCondition::from(
            sample_ids.iter().copied().collect::<AHashSet<_>>(),
        )));

        let mut nearests = Vec::with_capacity(sampled.len());
        for point in &sampled {
            let vector = point
                .vector
                .as_ref()
                .and_then(|v| v.get(&using))
                .map(|v| v.to_owned())
                .ok_or_else(|| {
                    OperationError::service_error("sampled point is missing its vector")
                })?;
            let nearest = ShardQueryRequest {
                prefetches: vec![],
                query: Some(ScoringQuery::Vector(QueryEnum::Nearest(NamedQuery::new(
                    vector,
                    using.clone(),
                )))),
                filter: Some(id_filter.clone()),
                score_threshold: None,
                limit: limit_per_sample.saturating_add(1), // +1 to drop the point itself afterwards
                offset: 0,
                params: None,
                with_vector: WithVector::Bool(false),
                with_payload: WithPayloadInterface::Bool(false),
            };
            let mut scores = self.query(nearest)?;
            if let Some(pos) = scores.iter().position(|p| p.id == point.id) {
                scores.remove(pos);
            } else if scores.len() == limit_per_sample.saturating_add(1) {
                scores.pop();
            }
            nearests.push(scores);
        }

        Ok(SearchMatrixResponse {
            sample_ids,
            nearests,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EdgeShardRead;
    use crate::test_helpers::{VECTOR_NAME, point, test_config, upsert};

    #[test]
    fn matrix_returns_sample_and_nearests() {
        let dir = tempfile::tempdir().unwrap();
        let shard = crate::EdgeShard::new(dir.path(), test_config()).unwrap();
        upsert(&shard, (1..=4).map(point).collect());

        let resp = shard
            .search_matrix(SearchMatrixRequest {
                sample_size: 4,
                limit_per_sample: 2,
                filter: None,
                using: VECTOR_NAME.to_string(),
            })
            .unwrap();

        assert_eq!(resp.sample_ids.len(), 4);
        assert_eq!(resp.nearests.len(), 4);
        for (id, row) in resp.sample_ids.iter().zip(&resp.nearests) {
            assert!(row.iter().all(|p| &p.id != id));
            assert!(row.len() <= 2);
        }
    }

    #[test]
    fn matrix_empty_when_fewer_than_two_points() {
        let dir = tempfile::tempdir().unwrap();
        let shard = crate::EdgeShard::new(dir.path(), test_config()).unwrap();
        upsert(&shard, vec![point(1)]);

        let resp = shard
            .search_matrix(SearchMatrixRequest {
                sample_size: 10,
                limit_per_sample: 3,
                filter: None,
                using: VECTOR_NAME.to_string(),
            })
            .unwrap();

        assert!(resp.sample_ids.is_empty());
    }
}
