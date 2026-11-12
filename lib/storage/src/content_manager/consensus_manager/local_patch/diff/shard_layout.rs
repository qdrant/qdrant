//! Shard layout invariants that shard routing relies on

use std::collections::HashMap;
use std::fmt;

use collection::collection_state::State;
use collection::config::ShardingMethod;
use collection::shards::shard::ShardId;
use segment::types::ShardKey;

/// Shard layout inconsistency, that breaks routing of a collection
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Violation {
    /// Collection with auto sharding has a shard key, its shards are removed from the default ring
    ShardKeyWithAutoSharding { shard_key: ShardKey },
    /// Shard key mapping references a shard that doesn't exist
    MissingShard {
        shard_key: ShardKey,
        shard_id: ShardId,
    },
    /// Shard is mapped to more than one shard key, only one of them routes to it
    ShardWithManyKeys { shard_id: ShardId },
    /// Shard of a collection with custom sharding has no shard key, so it's not routed to
    ShardWithoutKey { shard_id: ShardId },
}

/// Find all shard layout inconsistencies of a collection
pub(super) fn violations(state: &State) -> Vec<Violation> {
    let State {
        config,
        shards,
        resharding: _,
        transfers: _,
        shards_key_mapping,
        payload_index_schema: _,
    } = state;

    let sharding_method = config.params.sharding_method.unwrap_or_default();

    let mut violations = Vec::new();
    let mut key_count_by_shard: HashMap<ShardId, usize> = HashMap::new();

    for (shard_key, shard_ids) in shards_key_mapping.iter() {
        match sharding_method {
            ShardingMethod::Auto => violations.push(Violation::ShardKeyWithAutoSharding {
                shard_key: shard_key.clone(),
            }),
            ShardingMethod::Custom => (),
        }

        for &shard_id in shard_ids {
            *key_count_by_shard.entry(shard_id).or_default() += 1;

            if !shards.contains_key(&shard_id) {
                violations.push(Violation::MissingShard {
                    shard_key: shard_key.clone(),
                    shard_id,
                });
            }
        }
    }

    for (&shard_id, &key_count) in &key_count_by_shard {
        if key_count > 1 {
            violations.push(Violation::ShardWithManyKeys { shard_id });
        }
    }

    match sharding_method {
        ShardingMethod::Auto => (),
        ShardingMethod::Custom => {
            for &shard_id in shards.keys() {
                if !key_count_by_shard.contains_key(&shard_id) {
                    violations.push(Violation::ShardWithoutKey { shard_id });
                }
            }
        }
    }

    violations
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ShardKeyWithAutoSharding { shard_key } => write!(
                f,
                "shard key mapping has shard key {shard_key}, \
                 but collection uses auto sharding",
            ),
            Self::MissingShard {
                shard_key,
                shard_id,
            } => write!(
                f,
                "shard key mapping references shard {shard_id} of shard key {shard_key}, \
                 which doesn't exist",
            ),
            Self::ShardWithManyKeys { shard_id } => write!(
                f,
                "shard key mapping maps shard {shard_id} to more than one shard key",
            ),
            Self::ShardWithoutKey { shard_id } => write!(
                f,
                "shard {shard_id} has no shard key, but collection uses custom sharding",
            ),
        }
    }
}
