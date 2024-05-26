use std::cmp::max;
use std::collections::{HashMap, HashSet};

use common::fixed_length_priority_queue::FixedLengthPriorityQueue;
use common::types::ScoreType;
use segment::types::{PointIdType, ScoredPoint, SeqNumberType};

use crate::operations::types::CollectionResult;

pub struct SearchResultAggregator {
    queue: FixedLengthPriorityQueue<ScoredPoint>,
    seen: HashSet<PointIdType>, // Point ids seen
}

impl SearchResultAggregator {
    pub fn new(limit: usize) -> CollectionResult<Self> {
        Ok(SearchResultAggregator {
            queue: FixedLengthPriorityQueue::new(limit)?,
            seen: HashSet::new(),
        })
    }

    pub fn push(&mut self, point: ScoredPoint) {
        let point_id = point.id;
        if !self.seen.contains(&point_id) {
            self.seen.insert(point_id);
            self.queue.push(point);
        }
    }

    pub fn into_vec(self) -> Vec<ScoredPoint> {
        self.queue.into_vec()
    }

    pub fn lowest(&self) -> Option<&ScoredPoint> {
        self.queue.top()
    }
}

pub struct BatchResultAggregator {
    // result aggregators for each batched request
    batch_aggregators: Vec<SearchResultAggregator>,
    // Store max version for each point id to exclude outdated points from the result
    point_versions: HashMap<PointIdType, SeqNumberType>,
}

impl BatchResultAggregator {
    pub fn new(tops: impl Iterator<Item = usize>) -> CollectionResult<Self> {
        let mut merged_results_per_batch = vec![];
        for top in tops {
            merged_results_per_batch.push(SearchResultAggregator::new(top)?);
        }

        Ok(BatchResultAggregator {
            batch_aggregators: merged_results_per_batch,
            point_versions: HashMap::new(),
        })
    }

    pub fn update_point_versions(&mut self, search_results: &Vec<Vec<Vec<ScoredPoint>>>) {
        for segment_result in search_results {
            for segment_batch_result in segment_result {
                for point in segment_batch_result {
                    let point_id = point.id;
                    let point_version =
                        self.point_versions.entry(point_id).or_insert(point.version);
                    *point_version = max(*point_version, point.version);
                }
            }
        }
    }

    /// Updates the specific batch result aggregator with the new points
    /// Point must be:
    /// - not seen before
    /// - not outdated (not less than the version stored in point_versions)
    ///
    /// WARN: Must be called after `update_point_versions`, so that `point_versions` is up to date
    pub fn update_batch_results(
        &mut self,
        batch_id: usize,
        search_results: impl Iterator<Item = ScoredPoint>,
    ) {
        let aggregator = &mut self.batch_aggregators[batch_id];
        for scored_point in search_results {
            debug_assert!(self.point_versions.contains_key(&scored_point.id));
            let point_max_version = self
                .point_versions
                .get(&scored_point.id)
                .copied()
                .unwrap_or(0);
            if scored_point.version >= point_max_version {
                aggregator.push(scored_point);
            }
        }
    }

    /// Return lowest acceptable score for given batch id
    pub fn batch_lowest_scores(&self, batch_id: usize) -> Option<ScoreType> {
        let batch_scores = &self.batch_aggregators[batch_id];
        batch_scores.lowest().map(|x| x.score)
    }

    pub fn into_topk(self) -> Vec<Vec<ScoredPoint>> {
        self.batch_aggregators
            .into_iter()
            .map(|x| x.into_vec())
            .collect()
    }
}
