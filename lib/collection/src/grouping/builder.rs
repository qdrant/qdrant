#![allow(dead_code)] // TODO: remove

use futures::Future;
use itertools::Itertools;
use segment::data_types::groups::PseudoId;
use tokio::sync::RwLockReadGuard;

use super::group_by::{group_by, GroupRequest};
use crate::collection::Collection;
use crate::lookup::{lookup_ids, Lookup, LookupRequest};
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
    lookup: Option<LookupRequest>,
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
            lookup: None,
            read_consistency: None,
            shard_selection: None,
        }
    }

    pub fn with_lookup(mut self, lookup: LookupRequest) -> Self {
        self.lookup = Some(lookup);
        self
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
            self.group_by,
            self.collection,
            self.collection_by_name.clone(),
            self.read_consistency,
            self.shard_selection,
        )
        .await?;

        if let Some(lookup) = self.lookup {
            let mut lookups = {
                let pseudo_ids = groups
                    .iter()
                    .cloned()
                    .map(|group| group.id)
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
            for group in &mut groups {
                if let Some(lookup) = lookups.remove(&PseudoId::from(group.id.clone())) {
                    group.lookup = Some(lookup);
                } else {
                    // We want to explicitly say that there is no lookup for this group id,
                    // this is to differentiate between not finding a lookup and not requesting a lookup
                    group.lookup = Some(Lookup::None);
                }
            }
        }

        Ok(groups)
    }
}
