use api::rest::ShardKeySelector;
use segment::types::ShardKey;

use crate::shards::shard::ShardId;

#[derive(Debug, Clone, PartialEq)]
pub enum ShardSelectorInternal {
    /// No shard key specified
    Empty,
    /// All apply to all keys
    All,
    /// Select one shard key
    ShardKey(ShardKey),
    /// Select multiple shard keys
    ShardKeys(Vec<ShardKey>),
    /// ShardId
    ShardId(ShardId),
}

impl ShardSelectorInternal {
    pub fn is_shard_id(&self) -> bool {
        matches!(self, ShardSelectorInternal::ShardId(_))
    }
}

impl From<Option<ShardKey>> for ShardSelectorInternal {
    fn from(key: Option<ShardKey>) -> Self {
        match key {
            None => ShardSelectorInternal::Empty,
            Some(key) => ShardSelectorInternal::ShardKey(key),
        }
    }
}

impl From<Vec<ShardKey>> for ShardSelectorInternal {
    fn from(keys: Vec<ShardKey>) -> Self {
        ShardSelectorInternal::ShardKeys(keys)
    }
}

impl From<ShardKeySelector> for ShardSelectorInternal {
    fn from(selector: ShardKeySelector) -> Self {
        match selector {
            ShardKeySelector::ShardKey(key) => ShardSelectorInternal::ShardKey(key),
            ShardKeySelector::ShardKeys(keys) => ShardSelectorInternal::ShardKeys(keys),
        }
    }
}

impl From<Option<ShardKeySelector>> for ShardSelectorInternal {
    fn from(selector: Option<ShardKeySelector>) -> Self {
        match selector {
            None => ShardSelectorInternal::Empty,
            Some(selector) => selector.into(),
        }
    }
}
