use std::collections::HashMap;

use issues::{Issue, Solution};
use tokio::sync::RwLockReadGuard;

use crate::collection::Collection;
use crate::operations::shard_selector_internal::ShardSelectorInternal;
use crate::shards::CollectionId;

pub struct TooManyCollections;

impl Issue for TooManyCollections {
    fn code(&self) -> String {
        "/TOO_MANY_COLLECTIONS".to_string()
    }

    fn description(&self) -> String {
        "It looks like you have too many collections.\nIf your architecture creates collections programmatically, it's probably better to restructure your solution into a fixed number of them.\n\nLearn more here: https://qdrant.tech/documentation/guides/multiple-partitions/\nOr, for some more complex cases: https://qdrant.tech/documentation/guides/distributed_deployment/#user-defined-sharding".to_string()
    }

    fn solution(&self) -> Solution {
        Solution::Refactor(
            "Restructure your usage of Qdrant into a fixed number of collections".to_string(),
        )
    }
}

impl TooManyCollections {
    /// Defines how many collections are considered too many. Below this number, the issue is not submitted
    const MANY_COLLECTIONS: usize = 30;

    /// Defines how many points are considered too few as an average per collection
    const FEW_POINTS: usize = 300;

    /// Defines how concentrated the data is, in relation to the amount of collections.
    /// This metric is calculated with the average number of points per collection divided by the amount of collections
    pub const DENSITY_THRESHOLD: f64 = Self::FEW_POINTS as f64 / Self::MANY_COLLECTIONS as f64;

    pub async fn has_too_many_collections(
        collections_read_guard: RwLockReadGuard<'_, HashMap<CollectionId, Collection>>,
    ) -> bool {
        let counts_futures = collections_read_guard.values().map(|collection| async {
            collection
                .info(&ShardSelectorInternal::All)
                .await
                .ok()
                .and_then(|info| info.points_count)
                .unwrap_or(0)
        });
        let collections_count = collections_read_guard.len().max(1);

        let avg_collection_size = futures::future::join_all(counts_futures)
            .await
            .iter()
            .sum::<usize>()
            / collections_count;

        drop(collections_read_guard);

        let has_many_collections = collections_count > Self::MANY_COLLECTIONS;

        let data_density = avg_collection_size as f64 / collections_count as f64;
        let has_low_density = data_density < Self::DENSITY_THRESHOLD;

        has_many_collections && has_low_density
    }

    /// Checks the points density (avg_points / collections) in the collections and submits an issue if it's too low
    pub async fn submit_if_too_many_collections(
        collections_read_guard: RwLockReadGuard<'_, HashMap<CollectionId, Collection>>,
    ) -> bool {
        Self::has_too_many_collections(collections_read_guard).await
            && issues::submit(TooManyCollections)
    }

    /// Checks the points density (avg_points / collections) in the collections and solves the issue if it's acceptable
    pub async fn solve_if_sane_amount_of_collections(
        collections_read_guard: RwLockReadGuard<'_, HashMap<CollectionId, Collection>>,
    ) -> bool {
        !Self::has_too_many_collections(collections_read_guard).await
            && issues::solve(TooManyCollections.code())
    }
}
