use std::time::Duration;

use futures::Future;
use itertools::Itertools;
use tokio::sync::RwLockReadGuard;

use super::group_by::{group_by, GroupRequest};
use crate::collection::Collection;
use crate::lookup::lookup_ids;
use crate::lookup::types::PseudoId;
use crate::operations::consistency_params::ReadConsistency;
use crate::operations::types::{CollectionError, CollectionResult, PointGroup};
use crate::shards::shard::ShardId;

/// Builds on top of the group_by function to add lookup and possibly other features
pub struct GroupBy<'a, F, Fut>
where
    F: Fn(String) -> Fut + Clone,
    Fut: Future<Output = Option<RwLockReadGuard<'a, Collection>>>,
{
    group_by: GroupRequest,
    collection: &'a Collection,
    /// `Fn` to get a collection having its name. Obligatory for recommend and lookup
    collection_by_name: F,
    read_consistency: Option<ReadConsistency>,
    shard_selection: Option<ShardId>,
    timeout: Option<Duration>,
}

impl<'a, F, Fut> GroupBy<'a, F, Fut>
where
    F: Fn(String) -> Fut + Clone,
    Fut: Future<Output = Option<RwLockReadGuard<'a, Collection>>>,
{
    /// Creates a basic GroupBy builder
    pub fn new(group_by: GroupRequest, collection: &'a Collection, collection_by_name: F) -> Self {
        Self {
            group_by,
            collection,
            collection_by_name,
            read_consistency: None,
            shard_selection: None,
            timeout: None,
        }
    }

    pub fn set_read_consistency(mut self, read_consistency: Option<ReadConsistency>) -> Self {
        self.read_consistency = read_consistency;
        self
    }

    pub fn set_shard_selection(mut self, shard_selection: Option<ShardId>) -> Self {
        self.shard_selection = shard_selection;
        self
    }

    pub fn set_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.timeout = timeout;
        self
    }

    /// Runs the group by operation, optionally with a timeout.
    pub async fn execute(self) -> CollectionResult<Vec<PointGroup>> {
        if let Some(timeout) = self.timeout {
            tokio::time::timeout(timeout, self.run())
                .await
                .map_err(|_| {
                    log::debug!("GroupBy timeout reached: {} seconds", timeout.as_secs());
                    CollectionError::timeout(timeout.as_secs() as usize, "GroupBy")
                })?
        } else {
            self.run().await
        }
    }

    /// Does the actual grouping
    async fn run(self) -> CollectionResult<Vec<PointGroup>> {
        let mut groups = group_by(
            self.group_by.clone(),
            self.collection,
            self.collection_by_name.clone(),
            self.read_consistency,
            self.shard_selection,
            self.timeout,
        )
        .await?;

        if let Some(lookup) = self.group_by.with_lookup {
            let mut lookups = {
                let pseudo_ids = groups
                    .iter()
                    .map(|group| group.id.clone())
                    .map_into()
                    .collect();

                lookup_ids(
                    lookup,
                    pseudo_ids,
                    self.collection_by_name,
                    self.read_consistency,
                    self.shard_selection,
                )
                .await?
            };

            // Put the lookups in their respective groups
            groups.iter_mut().for_each(|group| {
                group.lookup = lookups.remove(&PseudoId::from(group.id.clone()));
            });
        }

        Ok(groups)
    }
}
