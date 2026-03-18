use std::sync::{Arc, Weak};

use tokio::sync;

use super::ShardHolder;

#[derive(Clone)]
pub struct SharedShardHolder(Arc<sync::RwLock<ShardHolder>>);

impl SharedShardHolder {
    pub fn new(shard_holder: ShardHolder) -> Self {
        Self(Arc::new(sync::RwLock::new(shard_holder)))
    }

    pub async fn read(&self) -> sync::RwLockReadGuard<'_, ShardHolder> {
        self.0.read().await
    }

    pub async fn read_owned(self) -> sync::OwnedRwLockReadGuard<ShardHolder> {
        self.0.read_owned().await
    }

    pub async fn write(&self) -> sync::RwLockWriteGuard<'_, ShardHolder> {
        self.0.write().await
    }

    pub async fn write_owned(self) -> sync::OwnedRwLockWriteGuard<ShardHolder> {
        self.0.write_owned().await
    }

    pub fn downgrade(&self) -> WeakShardHolder {
        WeakShardHolder(Arc::downgrade(&self.0))
    }
}

#[derive(Clone)]
pub struct WeakShardHolder(Weak<sync::RwLock<ShardHolder>>);

impl WeakShardHolder {
    pub fn upgrade(&self) -> Option<SharedShardHolder> {
        self.0.upgrade().map(SharedShardHolder)
    }
}
