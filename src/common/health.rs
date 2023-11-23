use std::collections::HashSet;
use std::sync::atomic::{self, AtomicBool};
use std::sync::Arc;
use std::time::{Duration, Instant};

use collection::shards::replica_set::ReplicaState;
use collection::shards::shard::ShardId;
use collection::shards::CollectionId;
use storage::content_manager::toc::TableOfContent;
use tokio::sync::RwLock;
use tokio::time;

pub struct Ready {
    toc: Arc<TableOfContent>,
    consensus_warmup_timeout: Duration,
    startup_timestamp: Instant,
    is_initialized: AtomicBool,
    unhealthy_shards: RwLock<HashSet<Shard>>,
}

impl Ready {
    pub fn new(toc: Arc<TableOfContent>) -> Self {
        Self::with_consensus_warmup_timeout(toc, Duration::from_secs(10))
    }

    pub fn with_consensus_warmup_timeout(
        toc: Arc<TableOfContent>,
        consensus_warmup_timeout: Duration,
    ) -> Self {
        Self {
            toc,
            consensus_warmup_timeout,
            startup_timestamp: Instant::now(),
            is_initialized: Default::default(),
            unhealthy_shards: Default::default(),
        }
    }

    pub async fn initialize(self: Arc<Self>) {
        let consensus_warmup_timestamp = self.startup_timestamp + self.consensus_warmup_timeout;
        time::sleep_until(consensus_warmup_timestamp.into()).await;

        if self.is_initialized.load(atomic::Ordering::Relaxed) {
            return;
        }

        self.initialize_unhealthy(filter(self.shards().await, false))
            .await;
    }

    pub async fn ready(&self) -> bool {
        if self.startup_timestamp.elapsed() < self.consensus_warmup_timeout {
            return false;
        }

        let is_initialized = self.is_initialized.load(atomic::Ordering::Relaxed);

        if is_initialized && self.unhealthy_shards.read().await.is_empty() {
            return true;
        }

        let shards = self.shards().await;

        if !is_initialized {
            self.initialize_unhealthy(filter(shards, false)).await
        } else {
            self.remove_healthy(filter(shards, true)).await
        }
    }

    async fn shards(&self) -> Vec<(Shard, bool)> {
        let this_peer_id = self.toc.this_peer_id;
        let collections = self.toc.all_collections().await;

        let mut shards = Vec::new();

        for collection in &collections {
            let state = match self.toc.get_collection(collection).await {
                Ok(collection) => collection.state().await,
                Err(_) => continue,
            };

            for (&shard, info) in state.shards.iter() {
                let Some(&state) = info.replicas.get(&this_peer_id) else {
                    continue;
                };

                shards.push((Shard::new(collection, shard), is_ready(state)));
            }
        }

        shards
    }

    async fn initialize_unhealthy(&self, shards: impl IntoIterator<Item = Shard>) -> bool {
        let mut unhealthy_shards = self.unhealthy_shards.write().await;

        unhealthy_shards.extend(shards);
        self.is_initialized.store(true, atomic::Ordering::Relaxed);

        unhealthy_shards.is_empty()
    }

    async fn remove_healthy(&self, shards: impl IntoIterator<Item = Shard>) -> bool {
        let mut unhealthy_shards = self.unhealthy_shards.write().await;

        for shard in shards {
            unhealthy_shards.remove(&shard);
        }

        unhealthy_shards.is_empty()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct Shard {
    collection: CollectionId,
    shard: ShardId,
}

impl Shard {
    pub fn new(collection: impl Into<CollectionId>, shard: ShardId) -> Self {
        Self {
            collection: collection.into(),
            shard,
        }
    }
}

fn is_ready(state: ReplicaState) -> bool {
    matches!(state, ReplicaState::Active | ReplicaState::Listener) // TODO: `ReplicaState::Initializing`?
}

fn filter(
    shards: impl IntoIterator<Item = (Shard, bool)>,
    filter: bool,
) -> impl Iterator<Item = Shard> {
    shards.into_iter().filter_map(move |(shard, is_ready)| {
        if is_ready == filter {
            Some(shard)
        } else {
            None
        }
    })
}
