//! Shard layout invariants that shard routing relies on

use std::collections::HashSet;

use collection::collection_state::State;
use collection::config::ShardingMethod;
use itertools::Itertools as _;

/// Validate shard layout of a collection, and return all problems found
pub(super) fn validate(state: &State) -> Vec<String> {
    let State {
        config,
        shards,
        resharding,
        transfers: _,
        shards_key_mapping,
        payload_index_schema: _,
    } = state;

    let sharding_method = config.params.sharding_method.unwrap_or_default();

    let mut problems = Vec::new();

    for (shard_key, shard_ids) in shards_key_mapping.iter() {
        match sharding_method {
            // Shards with a shard key are removed from the default hash ring
            ShardingMethod::Auto => problems.push(format!(
                "shard key mapping has shard key {shard_key}, but collection uses auto sharding",
            )),
            ShardingMethod::Custom => (),
        }

        for shard_id in shard_ids {
            if !shards.contains_key(shard_id) {
                problems.push(format!(
                    "shard key mapping references shard {shard_id} of shard key {shard_key}, \
                     which doesn't exist",
                ));
            }
        }
    }

    // Only one of the shard keys routes to the shard
    for shard_id in shards_key_mapping.iter_shard_ids().duplicates() {
        problems.push(format!(
            "shard key mapping maps shard {shard_id} to more than one shard key",
        ));
    }

    match sharding_method {
        ShardingMethod::Auto => (),
        // Shards without a shard key are not routed to
        ShardingMethod::Custom => {
            let mapped_shards: HashSet<_> = shards_key_mapping.iter_shard_ids().collect();

            for shard_id in shards.keys().filter(|id| !mapped_shards.contains(id)) {
                problems.push(format!(
                    "shard {shard_id} has no shard key, but collection uses custom sharding",
                ));
            }
        }
    }

    // `ShardHolder::rebuild_rings` panics, if resharding shard key has no hash ring
    if let Some(resharding) = resharding {
        let has_ring = match (sharding_method, &resharding.shard_key) {
            (ShardingMethod::Auto, None) => true,
            (ShardingMethod::Custom, Some(shard_key)) => shards_key_mapping
                .get(shard_key)
                .is_some_and(|shard_ids| shard_ids.iter().any(|id| shards.contains_key(id))),
            (ShardingMethod::Auto, Some(_)) | (ShardingMethod::Custom, None) => false,
        };

        if !has_ring {
            let shard_key = match &resharding.shard_key {
                Some(shard_key) => format!("shard key {shard_key}"),
                None => "no shard key".to_string(),
            };

            problems.push(format!(
                "resharding is in progress for {shard_key}, which has no hash ring",
            ));
        }
    }

    problems
}
