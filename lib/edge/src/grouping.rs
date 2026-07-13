use segment::common::operation_error::OperationResult;
use segment::json_path::JsonPath;
pub use shard::grouping::Group;
use shard::grouping::{GroupsAggregator, shape_candidates_query};
use shard::query::{self, ShardQueryRequest};

use crate::read_view::{EdgeReadView, ReadSegmentHandle};

/// Extra candidates fetched per requested `groups * group_size` slot so groups can
/// fill even when top hits cluster into few distinct group values.
const OVERFETCH_FACTOR: usize = 4;

/// Group the results of a base query by a payload field.
pub struct GroupRequest {
    /// Base scoring query. Its `limit`/`offset`/`with_payload`/`filter` are adjusted
    /// here; the query vector, params, prefetch and score_threshold are honoured.
    pub query: ShardQueryRequest,
    pub group_by: JsonPath,
    pub groups: usize,
    pub group_size: usize,
}

impl<H: ReadSegmentHandle> EdgeReadView<H> {
    pub(crate) fn query_groups(&self, request: GroupRequest) -> OperationResult<Vec<Group>> {
        let GroupRequest {
            mut query,
            group_by,
            groups,
            group_size,
        } = request;
        if groups == 0 || group_size == 0 {
            return Ok(vec![]);
        }

        let order = query::query_result_order(query.query.as_ref(), |vector_name| {
            self.config.get_distance(vector_name)
        })?;

        let limit = groups
            .saturating_mul(group_size)
            .saturating_mul(OVERFETCH_FACTOR);
        shape_candidates_query(&mut query, &group_by, limit, group_size);

        let points = self.query(query)?;

        let mut aggregator = GroupsAggregator::new(groups, group_size, group_by, order);
        aggregator.add_points(&points);
        Ok(aggregator.distill())
    }
}

#[cfg(test)]
mod tests {
    use segment::data_types::groups::GroupId;
    use segment::data_types::vectors::{NamedQuery, VectorInternal};
    use segment::types::{WithPayloadInterface, WithVector};
    use shard::query::ScoringQuery;
    use shard::query::query_enum::QueryEnum;

    use super::*;
    use crate::EdgeShardRead;
    use crate::test_helpers::{
        VECTOR_NAME, point_with_group, point_with_group_values, test_config, upsert,
    };

    fn base_query() -> ShardQueryRequest {
        ShardQueryRequest {
            prefetches: vec![],
            query: Some(ScoringQuery::Vector(QueryEnum::Nearest(NamedQuery::new(
                VectorInternal::from(vec![1.0]),
                VECTOR_NAME.to_string(),
            )))),
            filter: None,
            score_threshold: None,
            limit: 0,
            offset: 0,
            params: None,
            with_vector: WithVector::Bool(false),
            with_payload: WithPayloadInterface::Bool(false),
        }
    }

    #[test]
    fn groups_by_payload_field() {
        let dir = tempfile::tempdir().unwrap();
        let shard = crate::EdgeShard::new(dir.path(), test_config()).unwrap();
        upsert(
            &shard,
            vec![
                point_with_group(1, "a"),
                point_with_group(2, "b"),
                point_with_group(3, "a"),
                point_with_group(4, "b"),
            ],
        );

        let groups = shard
            .query_groups(GroupRequest {
                query: base_query(),
                group_by: "group".parse().unwrap(),
                groups: 2,
                group_size: 5,
            })
            .unwrap();

        assert_eq!(groups.len(), 2);
        // Test vectors score by dot product with [1.0], so the group holding the
        // highest-id point comes first.
        assert_eq!(groups[0].key, GroupId::from("b"));
        assert_eq!(groups[1].key, GroupId::from("a"));
        for g in &groups {
            assert_eq!(g.hits.len(), 2);
        }
    }

    #[test]
    fn groups_by_multi_valued_payload_field() {
        let dir = tempfile::tempdir().unwrap();
        let shard = crate::EdgeShard::new(dir.path(), test_config()).unwrap();
        upsert(
            &shard,
            vec![
                point_with_group_values(1, serde_json::json!(["a", "b"])),
                point_with_group(2, "a"),
                point_with_group(3, "b"),
            ],
        );

        let groups = shard
            .query_groups(GroupRequest {
                query: base_query(),
                group_by: "group".parse().unwrap(),
                groups: 2,
                group_size: 2,
            })
            .unwrap();

        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].key, GroupId::from("b"));
        assert_eq!(groups[1].key, GroupId::from("a"));
        // Point 1 carries both group values, so it must land in both groups.
        for g in &groups {
            assert_eq!(g.hits.len(), 2);
            assert!(g.hits.iter().any(|hit| hit.id == 1.into()));
        }
    }
}
