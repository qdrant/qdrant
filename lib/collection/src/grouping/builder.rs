use futures::Future;
use itertools::Itertools;
use tokio::sync::RwLockReadGuard;

use super::group_by::{group_by, GroupRequest};
use crate::collection::Collection;
use crate::lookup::lookup_ids;
use crate::lookup::types::PseudoId;
use crate::operations::consistency_params::ReadConsistency;
use crate::operations::types::{CollectionResult, PointGroup};
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
        }
    }

    pub fn with_read_consistency(mut self, read_consistency: ReadConsistency) -> Self {
        self.read_consistency = Some(read_consistency);
        self
    }

    pub fn with_shard_selection(mut self, shard_selection: ShardId) -> Self {
        self.shard_selection = Some(shard_selection);
        self
    }

    pub async fn execute(self) -> CollectionResult<Vec<PointGroup>> {
        let mut groups = group_by(
            self.group_by.clone(),
            self.collection,
            self.collection_by_name.clone(),
            self.read_consistency,
            self.shard_selection,
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
