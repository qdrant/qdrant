use segment::json_path::JsonPath;
use segment::types::{Condition, Filter, Order, ScoredPoint};

use super::{Group, GroupsAggregator, except_on, match_on, merge_filter, shape_candidates_query};
use crate::query::ShardQueryRequest;

const MAX_GET_GROUPS_REQUESTS: usize = 5;
const MAX_GROUP_FILLING_REQUESTS: usize = 5;

/// How many backend requests a group-by run may spend in each phase.
#[derive(Copy, Clone, Debug)]
pub struct RequestBudget {
    /// Requests used to discover enough groups.
    pub collect: usize,
    /// Requests used afterwards to fill up the best groups which are not full yet.
    pub fill: usize,
}

impl Default for RequestBudget {
    fn default() -> Self {
        Self {
            collect: MAX_GET_GROUPS_REQUESTS,
            fill: MAX_GROUP_FILLING_REQUESTS,
        }
    }
}

/// Sans-IO state machine for a group-by run, shared between the server and edge so the
/// request shaping and stop conditions cannot diverge between them.
///
/// Drive it in a loop: [`next_request`](Self::next_request) yields the next backend query,
/// the caller executes it, feeds the results back with [`add_points`](Self::add_points), and
/// takes the groups with [`distill`](Self::distill) once no request is yielded. Query
/// execution stays with the caller, so the same driver serves both the async server path and
/// the sync edge path; the [`RequestBudget`] makes the effort policy explicit.
pub struct GroupByDriver {
    base_query: ShardQueryRequest,
    group_by: JsonPath,
    groups: usize,
    group_size: usize,
    /// Number of candidate points fetched per request.
    candidates_limit: usize,
    aggregator: GroupsAggregator,
    state: State,
}

#[derive(Copy, Clone)]
enum State {
    /// Discover new groups until enough of the best ones are full.
    Collecting {
        requests_left: usize,
        fill_budget: usize,
    },
    /// Fill up the best groups which are not full yet.
    Filling {
        requests_left: usize,
    },
    Done,
}

impl GroupByDriver {
    pub fn new(
        base_query: ShardQueryRequest,
        group_by: JsonPath,
        groups: usize,
        group_size: usize,
        order: Option<Order>,
        budget: RequestBudget,
    ) -> Self {
        let state = if groups == 0 || group_size == 0 {
            State::Done
        } else {
            State::Collecting {
                requests_left: budget.collect,
                fill_budget: budget.fill,
            }
        };
        Self {
            aggregator: GroupsAggregator::new(groups, group_size, group_by.clone(), order),
            base_query,
            group_by,
            groups,
            group_size,
            // Fetch enough points per request to fill all groups
            candidates_limit: groups.saturating_mul(group_size),
            state,
        }
    }

    /// The next backend query to execute, or `None` once the run is complete.
    ///
    /// Feed the response of each yielded request into [`add_points`](Self::add_points) before
    /// asking for the next one; without new results the driver yields the same request again
    /// until its budget runs out.
    pub fn next_request(&mut self) -> Option<ShardQueryRequest> {
        loop {
            match &mut self.state {
                State::Collecting {
                    requests_left,
                    fill_budget,
                } => {
                    if *requests_left == 0 {
                        self.state = State::Filling {
                            requests_left: *fill_budget,
                        };
                        continue;
                    }
                    *requests_left -= 1;

                    let mut request = self.base_query.clone();

                    // Construct filter to exclude already found groups
                    let full_groups = self.aggregator.keys_of_filled_groups();
                    if !full_groups.is_empty() {
                        let except_any = except_on(&self.group_by, &full_groups);
                        if !except_any.is_empty() {
                            merge_filter(
                                &mut request.filter,
                                Filter {
                                    must: Some(except_any),
                                    ..Default::default()
                                },
                            );
                        }
                    }

                    self.exclude_aggregated_points(&mut request);
                    shape_candidates_query(
                        &mut request,
                        &self.group_by,
                        self.candidates_limit,
                        self.group_size,
                    );
                    return Some(request);
                }
                State::Filling { requests_left } => {
                    if *requests_left == 0 {
                        self.state = State::Done;
                        continue;
                    }
                    *requests_left -= 1;

                    let mut request = self.base_query.clone();

                    // Construct filter to only include unsatisfied groups
                    let unsatisfied_groups = self.aggregator.keys_of_unfilled_best_groups();
                    let match_any = match_on(&self.group_by, &unsatisfied_groups);
                    if !match_any.is_empty() {
                        merge_filter(
                            &mut request.filter,
                            Filter {
                                must: Some(match_any),
                                ..Default::default()
                            },
                        );
                    }

                    self.exclude_aggregated_points(&mut request);
                    shape_candidates_query(
                        &mut request,
                        &self.group_by,
                        self.candidates_limit,
                        self.group_size,
                    );
                    return Some(request);
                }
                State::Done => return None,
            }
        }
    }

    /// Ingest the response of the last yielded request and advance the state machine.
    pub fn add_points(&mut self, points: &[ScoredPoint]) {
        self.aggregator.add_points(points);

        let enough_groups = self.aggregator.len_of_filled_best_groups() >= self.groups;
        match self.state {
            State::Collecting { fill_budget, .. } => {
                if enough_groups {
                    self.state = State::Done;
                } else if points.is_empty() {
                    // No more matching points; collecting again is pointless, try filling.
                    self.state = State::Filling {
                        requests_left: fill_budget,
                    };
                }
            }
            State::Filling { .. } => {
                if enough_groups || points.is_empty() {
                    self.state = State::Done;
                }
            }
            State::Done => {}
        }
    }

    /// Exclude already aggregated points
    fn exclude_aggregated_points(&self, request: &mut ShardQueryRequest) {
        let ids = self.aggregator.ids().clone();
        if !ids.is_empty() {
            merge_filter(
                &mut request.filter,
                Filter::new_must_not(Condition::HasId(ids.into())),
            );
        }
    }

    /// Consume the driver and return the best groups, sorted by their best hit.
    pub fn distill(self) -> Vec<Group> {
        self.aggregator.distill()
    }
}

#[cfg(test)]
mod tests {
    use common::types::ScoreType;
    use segment::data_types::groups::GroupId;
    use segment::payload_json;
    use segment::types::{WithPayloadInterface, WithVector};

    use super::*;

    const GROUPS: usize = 2;
    const GROUP_SIZE: usize = 2;

    fn base_query() -> ShardQueryRequest {
        ShardQueryRequest {
            prefetches: vec![],
            query: None,
            filter: None,
            score_threshold: None,
            limit: 0,
            offset: 0,
            params: None,
            with_vector: WithVector::Bool(false),
            with_payload: WithPayloadInterface::Bool(false),
        }
    }

    fn driver(budget: RequestBudget) -> GroupByDriver {
        GroupByDriver::new(
            base_query(),
            "g".parse().unwrap(),
            GROUPS,
            GROUP_SIZE,
            Some(Order::LargeBetter),
            budget,
        )
    }

    fn point(id: u64, score: ScoreType, group: &str) -> ScoredPoint {
        ScoredPoint {
            id: id.into(),
            version: 0,
            score,
            payload: Some(payload_json! { "g": group }),
            vector: None,
            metadata: None,
            shard_key: None,
            order_value: None,
        }
    }

    #[test]
    fn single_request_budget_yields_one_shaped_request() {
        let mut driver = driver(RequestBudget {
            collect: 1,
            fill: 0,
        });

        let request = driver.next_request().unwrap();
        assert_eq!(request.limit, GROUPS * GROUP_SIZE);
        assert_eq!(request.offset, 0);
        // The is-empty restriction on the group_by field must be in place.
        assert!(request.filter.is_some());
        assert_eq!(
            request.with_payload,
            WithPayloadInterface::Fields(vec!["g".parse().unwrap()])
        );

        // One incomplete group left after the response, but the budget is spent.
        driver.add_points(&[point(1, 1.0, "a")]);
        assert!(driver.next_request().is_none());

        let groups = driver.distill();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].key, GroupId::from("a"));
    }

    #[test]
    fn stops_early_when_enough_groups_are_filled() {
        let mut driver = driver(RequestBudget {
            collect: 5,
            fill: 5,
        });

        assert!(driver.next_request().is_some());
        driver.add_points(&[
            point(1, 4.0, "a"),
            point(2, 3.0, "a"),
            point(3, 2.0, "b"),
            point(4, 1.0, "b"),
        ]);

        // Both requested groups are full; no further requests are spent.
        assert!(driver.next_request().is_none());

        let groups = driver.distill();
        assert_eq!(groups.len(), GROUPS);
        assert_eq!(groups[0].key, GroupId::from("a"));
        assert_eq!(groups[1].key, GroupId::from("b"));
        for group in &groups {
            assert_eq!(group.hits.len(), GROUP_SIZE);
        }
    }

    #[test]
    fn moves_to_filling_and_finishes_on_empty_responses() {
        let mut driver = driver(RequestBudget {
            collect: 5,
            fill: 5,
        });

        // Collect round: one hit per group, none full.
        let collect_request = driver.next_request().unwrap();
        driver.add_points(&[point(1, 2.0, "a"), point(2, 1.0, "b")]);

        // Next collect round excludes the aggregated points, so its filter grows.
        let second_request = driver.next_request().unwrap();
        assert_ne!(collect_request.filter, second_request.filter);

        // Nothing new to collect: the driver switches to filling.
        driver.add_points(&[]);
        assert!(driver.next_request().is_some());

        // Filling also comes up empty: the run is over.
        driver.add_points(&[]);
        assert!(driver.next_request().is_none());

        let groups = driver.distill();
        assert_eq!(groups.len(), 2);
    }

    #[test]
    fn zero_groups_or_group_size_finishes_immediately() {
        let budget = RequestBudget {
            collect: 5,
            fill: 5,
        };
        for (groups, group_size) in [(0, GROUP_SIZE), (GROUPS, 0)] {
            let mut driver = GroupByDriver::new(
                base_query(),
                "g".parse().unwrap(),
                groups,
                group_size,
                Some(Order::LargeBetter),
                budget,
            );
            assert!(driver.next_request().is_none());
            assert!(driver.distill().is_empty());
        }
    }
}
