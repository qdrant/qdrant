use std::sync::Arc;
use std::time::{Duration, Instant};

use collection::shards::replica_set::ReplicaState;
use storage::content_manager::toc::TableOfContent;

pub struct Ready {
    toc: Arc<TableOfContent>,
    consensus_sync_instant: Instant,
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
        }
    }

    pub async fn ready(&self) -> bool {
        if Instant::now() < self.consensus_sync_instant {
            return false;
        }

        let this_peer_id = self.toc.this_peer_id;
        let collections = self.toc.all_collections().await;

        for collection in &collections {
            let state = match self.toc.get_collection(&collection).await {
                Ok(collection) => collection.state().await,
                Err(_) => continue,
            };

            let ready = state
                .shards
                .iter()
                .filter_map(|(_, state)| state.replicas.get(&this_peer_id).copied())
                .all(|state| matches!(state, ReplicaState::Active | ReplicaState::Listener)); // TODO: `ReplicaState::Initializing`?

            if !ready {
                return false;
            }
        }

        true
    }
}
