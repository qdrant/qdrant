use std::collections::HashSet;
use std::ops::DerefMut as _;
use std::sync::Arc;
use std::time::{Duration, Instant};

use collection::shards::replica_set::ReplicaState;
use collection::shards::shard::ShardId;
use collection::shards::CollectionId;
use storage::content_manager::toc::TableOfContent;
use tokio::sync::RwLock;

pub struct Ready {
    toc: Arc<TableOfContent>,
    consensus_sync_instant: Instant,
    initial_unhelathy_shards: RwLock<Option<HashSet<Replica>>>,
}

impl Ready {
    pub fn new(toc: Arc<TableOfContent>) -> Self {
        Self::with_consensus_sync_timeout(toc, Duration::from_secs(10))
    }

    pub fn with_consensus_sync_timeout(
        toc: Arc<TableOfContent>,
        consensus_sync_timeout: Duration,
    ) -> Self {
        Self {
            toc,
            consensus_sync_instant: Instant::now() + consensus_sync_timeout,
            initial_unhelathy_shards: Default::default(),
        }
    }

    pub async fn ready(&self) -> bool {
        if Instant::now() < self.consensus_sync_instant {
            return false;
        }

        let is_first_run = self.initial_unhelathy_shards.read().await.is_some();

        let this_peer_id = self.toc.this_peer_id;
        let collections = self.toc.all_collections().await;

        let mut shards = Vec::new();

        for collection in &collections {
            let state = match self.toc.get_collection(collection).await {
                Ok(collection) => collection.state().await,
                Err(_) => continue,
            };

            for (&shard, info) in state.shards.iter() {
                if let Some(&state) = info.replicas.get(&this_peer_id) {
                    shards.push((collection.clone(), shard, state));
                }
            }
        }

        let mut initial_unhealthy_shards = self.initial_unhelathy_shards.write().await;

        match initial_unhealthy_shards.deref_mut() {
            None => {
                let unhealthy_shards =
                    shards.into_iter().filter_map(|(collection, shard, state)| {
                        if !matches!(state, ReplicaState::Active | ReplicaState::Listener) {
                            Some(Replica::new(collection, shard))
                        } else {
                            None
                        }
                    });

                initial_unhealthy_shards
                    .insert(unhealthy_shards.collect())
                    .is_empty()
            }

            Some(initial_unhealthy_shards) => {
                for (collection, shard, state) in shards {
                    let replica = Replica::new(collection, shard);

                    if matches!(state, ReplicaState::Active | ReplicaState::Listener) {
                        initial_unhealthy_shards.remove(&replica);
                    } else if is_first_run {
                        initial_unhealthy_shards.insert(replica);
                    }
                }

                initial_unhealthy_shards.is_empty()
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Replica {
    collection: CollectionId,
    shard: ShardId,
}

impl Replica {
    pub fn new(collection: CollectionId, shard: ShardId) -> Self {
        Self { collection, shard }
    }
}
