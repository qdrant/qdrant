use ahash::AHashMap;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::types::{ScoredPoint, WithVector};
pub use shard::grouping::Group;
use shard::grouping::{GroupByDriver, RequestBudget};
use shard::query::{self, ShardQueryRequest};

use crate::read_view::{EdgeReadView, ReadSegmentHandle};
use crate::requests::GroupRequest;

impl<H: ReadSegmentHandle> EdgeReadView<H> {
    pub(crate) fn query_groups(&self, request: GroupRequest) -> OperationResult<Vec<Group>> {
        self.check_stopped()?;
        let GroupRequest {
            query,
            group_by,
            groups,
            group_size,
        } = request;
        let mut query = ShardQueryRequest::from(query);

        // Groups are enriched with the user-requested payload and vectors at the end,
        // so candidates are fetched bare.
        let with_payload = query.with_payload.clone();
        let with_vector = std::mem::replace(&mut query.with_vector, WithVector::Bool(false));

        let order = query::query_result_order(query.query.as_ref(), |vector_name| {
            self.config.get_distance(vector_name)
        })?;

        let mut driver = GroupByDriver::new(
            query,
            group_by,
            groups,
            group_size,
            order,
            RequestBudget::default(),
        );

        while let Some(request) = driver.next_request() {
            self.check_stopped()?;
            let points = self.query(request)?;
            driver.add_points(&points);
        }

        let mut groups = driver.distill();

        let bare_points: AHashMap<_, _> = groups
            .iter()
            .flat_map(|group| &group.hits)
            .map(|hit| {
                let point = ScoredPoint {
                    payload: None,
                    vector: None,
                    ..hit.clone()
                };
                (hit.id, point)
            })
            .collect();
        let [enriched_points] = self
            .fill_with_payload_or_vectors(
                vec![bare_points.into_values().collect()],
                with_payload,
                with_vector,
                HwMeasurementAcc::disposable_edge(),
            )?
            .try_into()
            .map_err(|unconverted: Vec<_>| {
                OperationError::service_error(format!(
                    "expected single result after filling payload/vectors, got {}",
                    unconverted.len(),
                ))
            })?;
        let enriched_points: AHashMap<_, _> = enriched_points
            .into_iter()
            .map(|point| (point.id, point))
            .collect();
        groups
            .iter_mut()
            .for_each(|group| group.hydrate_from(&enriched_points));

        Ok(groups)
    }
}

#[cfg(test)]
mod tests {
    use segment::data_types::groups::GroupId;
    use segment::data_types::vectors::{NamedQuery, VectorInternal};
    use segment::types::{Payload, WithPayloadInterface};
    use shard::query::ScoringQuery;
    use shard::query::query_enum::QueryEnum;

    use super::*;
    use crate::test_helpers::{
        VECTOR_NAME, point_with_group, point_with_group_values, test_config, upsert,
    };
    use crate::{EdgeShardRead, QueryRequest, QueryRequestBuilder};

    fn base_query() -> QueryRequest {
        QueryRequestBuilder::new(0)
            .query(ScoringQuery::Vector(QueryEnum::Nearest(NamedQuery::new(
                VectorInternal::from(vec![1.0]),
                VECTOR_NAME.to_string(),
            ))))
            .build()
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
            .query_groups(GroupRequest::new(
                base_query(),
                "group".parse().unwrap(),
                2,
                5,
            ))
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
    fn group_hits_carry_requested_payload_and_vectors() {
        let dir = tempfile::tempdir().unwrap();
        let shard = crate::EdgeShard::new(dir.path(), test_config()).unwrap();
        let mut points = vec![point_with_group(1, "a"), point_with_group(2, "b")];
        for point in &mut points {
            let extra = serde_json::json!({ "extra": 1 });
            point
                .payload
                .as_mut()
                .unwrap()
                .merge(&Payload::from(extra.as_object().unwrap().clone()));
        }
        upsert(&shard, points);

        let group = |with_payload: bool, with_vector: bool| {
            let mut query = base_query();
            query.with_payload = WithPayloadInterface::Bool(with_payload);
            query.with_vector = WithVector::Bool(with_vector);
            shard
                .query_groups(GroupRequest::new(query, "group".parse().unwrap(), 2, 1))
                .unwrap()
        };

        let groups = group(true, true);
        assert_eq!(groups.len(), 2);
        for hit in groups.iter().flat_map(|group| &group.hits) {
            let payload = hit.payload.as_ref().unwrap();
            assert!(payload.0.contains_key("group"));
            assert!(payload.0.contains_key("extra"));
            assert!(hit.vector.is_some());
        }

        let groups = group(false, false);
        assert_eq!(groups.len(), 2);
        for hit in groups.iter().flat_map(|group| &group.hits) {
            assert!(hit.payload.is_none());
            assert!(hit.vector.is_none());
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
            .query_groups(GroupRequest::new(
                base_query(),
                "group".parse().unwrap(),
                2,
                2,
            ))
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
