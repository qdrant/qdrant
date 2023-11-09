use collection::operations::shard_key_selector::ShardKeySelector;
use segment::types::ShardKey;

#[derive(Debug, Clone)]
pub enum ShardKeySelectorInternal {
    /// No shard key specified
    Empty,
    /// All apply to all keys
    All,
    /// Select one shard key
    ShardKey(ShardKey),
    /// Select multiple shard keys
    ShardKeys(Vec<ShardKey>),
}

impl From<Option<ShardKey>> for ShardKeySelectorInternal {
    fn from(key: Option<ShardKey>) -> Self {
        match key {
            None => ShardKeySelectorInternal::Empty,
            Some(key) => ShardKeySelectorInternal::ShardKey(key),
        }
    }
}

impl From<Vec<ShardKey>> for ShardKeySelectorInternal {
    fn from(keys: Vec<ShardKey>) -> Self {
        ShardKeySelectorInternal::ShardKeys(keys)
    }
}

impl From<ShardKeySelector> for ShardKeySelectorInternal {
    fn from(selector: ShardKeySelector) -> Self {
        match selector {
            ShardKeySelector::ShardKey(key) => ShardKeySelectorInternal::ShardKey(key),
            ShardKeySelector::ShardKeys(keys) => ShardKeySelectorInternal::ShardKeys(keys),
        }
    }
}

impl From<Option<ShardKeySelector>> for ShardKeySelectorInternal {
    fn from(selector: Option<ShardKeySelector>) -> Self {
        match selector {
            None => ShardKeySelectorInternal::Empty,
            Some(selector) => selector.into(),
        }
    }
}
