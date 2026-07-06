use std::collections::HashMap;

use segment::common::operation_error::OperationResult;
use segment::json_path::JsonPath;
use segment::types::{Condition, Filter, PayloadContainer, ScoredPoint, WithPayloadInterface};
use serde_json::Value;
use shard::query::ShardQueryRequest;

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

/// Hashable, scalar group key derived from a payload value.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum GroupKey {
    Keyword(String),
    Integer(i64),
}

impl GroupKey {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::String(s) => Some(GroupKey::Keyword(s.clone())),
            Value::Number(n) => n.as_i64().map(GroupKey::Integer),
            Value::Null | Value::Bool(_) | Value::Array(_) | Value::Object(_) => None,
        }
    }
}

pub struct PointGroup {
    pub key: GroupKey,
    pub hits: Vec<ScoredPoint>,
}

impl<H: ReadSegmentHandle> EdgeReadView<H> {
    pub(crate) fn query_groups(&self, request: GroupRequest) -> OperationResult<Vec<PointGroup>> {
        let GroupRequest {
            mut query,
            group_by,
            groups,
            group_size,
        } = request;
        if groups == 0 || group_size == 0 {
            return Ok(vec![]);
        }

        // Only points that carry the group field can be grouped.
        let present = Filter::new_must_not(Condition::IsEmpty(group_by.clone().into()));
        query.filter = Some(match query.filter.take() {
            Some(f) => f.merge(&present),
            None => present,
        });
        // The group value has to come back in the payload to bucket on it.
        query.with_payload = WithPayloadInterface::Fields(vec![group_by.clone()]);
        query.limit = groups
            .saturating_mul(group_size)
            .saturating_mul(OVERFETCH_FACTOR);
        query.offset = 0;

        let points = self.query(query)?;

        // Bucket points by group key, preserving first-seen (best-score) order.
        let mut order: Vec<GroupKey> = Vec::new();
        let mut buckets: HashMap<GroupKey, Vec<ScoredPoint>> = HashMap::new();
        for point in points {
            let Some(payload) = point.payload.as_ref() else {
                continue;
            };
            let keys: Vec<GroupKey> = payload
                .get_value_cloned(&group_by)
                .into_iter()
                .filter_map(|v| GroupKey::from_value(&v))
                .collect();
            for key in keys {
                let bucket = buckets.entry(key.clone()).or_insert_with(|| {
                    order.push(key.clone());
                    Vec::new()
                });
                if bucket.len() < group_size {
                    bucket.push(point.clone());
                }
            }
        }

        let result = order
            .into_iter()
            .take(groups)
            .map(|key| {
                let hits = buckets.remove(&key).unwrap_or_default();
                PointGroup { key, hits }
            })
            .collect();
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use segment::data_types::vectors::{NamedQuery, VectorInternal};
    use segment::types::WithVector;
    use shard::query::ScoringQuery;
    use shard::query::query_enum::QueryEnum;

    use super::*;
    use crate::EdgeShardRead;
    use crate::test_helpers::{VECTOR_NAME, point_with_group, test_config, upsert};

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

        let base = ShardQueryRequest {
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
        };

        let groups = shard
            .query_groups(GroupRequest {
                query: base,
                group_by: "group".parse().unwrap(),
                groups: 2,
                group_size: 5,
            })
            .unwrap();

        assert_eq!(groups.len(), 2);
        for g in &groups {
            assert_eq!(g.hits.len(), 2);
        }
    }
}
