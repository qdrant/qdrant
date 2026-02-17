# Migration Plan: Auto to Custom Sharding

## Overview

This document outlines the plan to migrate a collection from `ShardingMethod::Auto` to `ShardingMethod::Custom`. This is a **one-way migration** that promotes all existing shards into a "default" shard key.

## Architecture Context

### Current State (Auto Sharding)
- Single hash ring with `None` as the shard key
- Shards are numbered sequentially: `0..shard_number-1`
- No shard key mapping stored (`shard_key_mapping.json` is empty)
- Hash ring structure: `HashMap<Option<ShardKey>, HashRingRouter>` with only `None` entry

### Target State (Custom Sharding)
- Hash ring per shard key
- All existing shards assigned to a default shard key (e.g., `ShardKey::Keyword("default")`)
- Shard key mapping file populated with default key -> all shard IDs
- Hash ring structure: `HashMap<Option<ShardKey>, HashRingRouter>` with entries per shard key

## Migration Strategy

### Phase 1: Pre-Migration Validation

**Location**: Dispatcher layer (`lib/storage/src/dispatcher.rs`)

1. **Validate Collection State**
   - Ensure collection exists and is in `Auto` mode
   - Verify no active resharding operations
   - Ensure no ongoing shard transfers
   - Check that all shards are in stable state (Active/Partial, not Initializing/Recovery)

2. **Validate Cluster State**
   - Ensure consensus is available (not single-node mode)
   - Verify cluster quorum is healthy
   - Check that all peers are reachable

**Implementation**:
```rust
// New validation function in dispatcher
async fn validate_auto_to_custom_migration(
    &self,
    collection_name: &str,
) -> Result<(), StorageError> {
    // Check collection exists and is Auto
    // Check no resharding active
    // Check no transfers in progress
    // Check cluster health
}
```

### Phase 2: Consensus Operation

**Location**: `lib/storage/src/content_manager/collection_meta_ops.rs`

1. **Create New Consensus Operation**
   - Add `PromoteToCustomSharding` variant to `CollectionMetaOperations`
   - Include collection name and optional default shard key (defaults to `"default"`)

2. **Propose Operation Through Consensus**
   - Dispatcher submits operation to consensus
   - Operation is replicated across all peers
   - All peers apply the operation atomically

**Implementation**:
```rust
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq, Hash, Clone)]
pub enum CollectionMetaOperations {
    // ... existing variants
    PromoteToCustomSharding {
        collection_name: String,
        default_shard_key: Option<ShardKey>, // Defaults to "default"
    },
}
```

### Phase 3: TOC Layer - Local Collection Update

**Location**: `lib/storage/src/content_manager/toc/collection_meta_ops.rs`

1. **Handle Operation in TOC**
   - Extract collection from TOC
   - Call collection-level migration method
   - Return success/failure to consensus

**Implementation**:
```rust
// In perform_collection_meta_op
CollectionMetaOperations::PromoteToCustomSharding { 
    collection_name, 
    default_shard_key 
} => {
    let collection = self.get_collection_unchecked(&collection_name).await?;
    collection.promote_to_custom_sharding(default_shard_key).await?;
    Ok(true)
}
```

### Phase 4: Collection Layer - State Migration

**Location**: `lib/collection/src/collection/mod.rs` or new file `lib/collection/src/collection/sharding_migration.rs`

1. **Update Collection Config**
   - Change `sharding_method` from `Auto` to `Custom`
   - Save config to disk
   - Update in-memory config

2. **Create Shard Key Mapping**
   - Get all current shard IDs (from `shard_holder`)
   - Create mapping: `default_shard_key -> HashSet<all_shard_ids>`
   - Update `ShardHolder` with new mapping

3. **Update ShardHolder**
   - Call `set_shard_key_mappings()` with new mapping
   - Update `sharding_method` field
   - Rebuild hash rings (will create hash ring per shard key)

4. **Update Replica Sets**
   - For each shard, update its replica set with the new shard key
   - Ensure replica sets are aware of their shard key

**Implementation**:
```rust
impl Collection {
    pub async fn promote_to_custom_sharding(
        &self,
        default_shard_key: Option<ShardKey>,
    ) -> CollectionResult<()> {
        // 1. Validate current state
        let state = self.state().await;
        if state.config.params.sharding_method.unwrap_or_default() != ShardingMethod::Auto {
            return Err(CollectionError::bad_request(
                "Collection is not in Auto sharding mode"
            ));
        }

        // 2. Get default shard key
        let default_key = default_shard_key
            .unwrap_or_else(|| ShardKey::Keyword("default".into()));

        // 3. Get all shard IDs
        let mut shard_holder = self.shards_holder.write().await;
        let all_shard_ids: HashSet<ShardId> = shard_holder
            .get_shards()
            .map(|(id, _)| id)
            .collect();

        // 4. Create shard key mapping
        let mut shard_key_mapping = ShardKeyMapping::default();
        shard_key_mapping.insert(default_key.clone(), all_shard_ids.clone());

        // 5. Update shard holder
        shard_holder.set_shard_key_mappings(shard_key_mapping.clone())?;
        
        // 6. Update replica sets with shard key
        for shard_id in &all_shard_ids {
            if let Some(replica_set) = shard_holder.get_shard_mut(*shard_id) {
                replica_set.update_shard_key(Some(default_key.clone())).await?;
            }
        }

        // 7. Update collection config
        {
            let mut config = self.collection_config.write().await;
            config.params.sharding_method = Some(ShardingMethod::Custom);
        }
        self.collection_config.read().await.save(&self.path)?;

        // 8. Rebuild hash rings (happens automatically when sharding_method changes)
        // The shard_holder will rebuild rings on next access, but we can trigger it explicitly
        drop(shard_holder);
        
        Ok(())
    }
}
```

### Phase 5: ShardHolder Updates

**Location**: `lib/collection/src/shards/shard_holder/mod.rs`

1. **Update Sharding Method**
   - Add method to change `sharding_method` field
   - Ensure `rebuild_rings()` handles the transition correctly

2. **Rebuild Hash Rings**
   - When `sharding_method` changes from Auto to Custom:
     - Remove the `None` hash ring
     - Create hash rings for each shard key in mapping
     - Add all shards to their respective hash rings

**Implementation**:
```rust
impl ShardHolder {
    pub fn set_sharding_method(&mut self, method: ShardingMethod) {
        self.sharding_method = method;
        // Rings will be rebuilt on next access or explicitly
    }

    // rebuild_rings() already handles both Auto and Custom correctly
    // Just need to ensure it's called after migration
}
```

### Phase 6: Replica Set Updates

**Location**: `lib/collection/src/shards/replica_set/mod.rs`

1. **Update Shard Key in Replica Sets**
   - Add method to update shard key for existing replica sets
   - Ensure all replicas (local and remote) are aware of the shard key

**Implementation**:
```rust
impl ShardReplicaSet {
    pub async fn update_shard_key(
        &mut self,
        shard_key: Option<ShardKey>,
    ) -> CollectionResult<()> {
        self.shard_key = shard_key.clone();
        // Update local shard if present
        if let Some(local_shard) = &mut self.local {
            // Local shard may need to be updated if it stores shard key
        }
        // Remote replicas will be updated through consensus state
        Ok(())
    }
}
```

### Phase 7: Config Compatibility

**Location**: `lib/collection/src/config.rs`

1. **Update Compatibility Check**
   - Modify `check_compatible()` to allow Auto -> Custom migration
   - Still prevent Custom -> Auto (one-way migration)
   - Add validation for the migration scenario

**Implementation**:
```rust
impl CollectionParams {
    pub fn check_compatible(&self, other: &CollectionParams) -> CollectionResult<()> {
        // ... existing checks ...

        let this_sharding_method = self.sharding_method.unwrap_or_default();
        let other_sharding_method = other.sharding_method.unwrap_or_default();

        // Allow Auto -> Custom migration
        if this_sharding_method == ShardingMethod::Auto 
            && other_sharding_method == ShardingMethod::Custom {
            // This is allowed - migration path
            return Ok(());
        }

        // Prevent Custom -> Auto (one-way migration)
        if this_sharding_method == ShardingMethod::Custom 
            && other_sharding_method == ShardingMethod::Auto {
            return Err(CollectionError::bad_input(
                "Cannot migrate from Custom to Auto sharding (one-way migration)"
            ));
        }

        // Same method - always compatible
        if this_sharding_method == other_sharding_method {
            return Ok(());
        }

        // Any other change is incompatible
        Err(CollectionError::bad_input(format!(
            "sharding method is incompatible: \
             origin sharding method: {this_sharding_method:?}, \
             while other sharding method: {other_sharding_method:?}",
        )))
    }
}
```

## State Persistence

### Files Modified During Migration

1. **`collection_config.json`**
   - `params.sharding_method` changed from `Auto` to `Custom`

2. **`shard_key_mapping.json`**
   - Created/updated with: `{"default": [0, 1, 2, ...]}` (all shard IDs)

3. **Consensus State**
   - New operation logged in Raft log
   - State snapshot includes updated shard key mapping

### Persistence Order

1. Consensus operation is proposed and committed
2. All peers apply the operation
3. Each peer updates local collection config
4. Each peer updates local shard key mapping file
5. Each peer rebuilds hash rings in memory

## Error Handling & Rollback

### Error Scenarios

1. **Partial Application**
   - If migration fails mid-way, consensus ensures all-or-nothing
   - Failed operation is not committed, so state remains consistent

2. **Peer Failure During Migration**
   - Failed peer will recover from consensus snapshot
   - Snapshot includes the migration operation
   - Peer applies migration on recovery

3. **Validation Failures**
   - Pre-migration validation catches issues early
   - Operation is rejected before consensus proposal

### Rollback Strategy

**Note**: This is a one-way migration. Rollback would require:
1. Manual intervention to change config back to Auto
2. Clearing shard key mapping
3. Rebuilding hash rings

**Recommendation**: Do not implement automatic rollback. Document manual rollback procedure separately.

## Testing Strategy

### Unit Tests

1. **ShardHolder Migration**
   - Test `set_shard_key_mappings()` with default key
   - Test `rebuild_rings()` after method change

2. **Config Compatibility**
   - Test `check_compatible()` allows Auto -> Custom
   - Test `check_compatible()` rejects Custom -> Auto

3. **Collection Migration**
   - Test `promote_to_custom_sharding()` updates all components
   - Test error handling for invalid states

### Integration Tests

1. **Single Node Migration**
   - Create collection with Auto sharding
   - Add points to collection
   - Migrate to Custom sharding
   - Verify all points are still accessible
   - Verify shard key mapping is correct

2. **Cluster Migration**
   - Create collection with Auto sharding in cluster
   - Migrate through consensus
   - Verify all peers have consistent state
   - Verify queries work correctly after migration

3. **Failure Scenarios**
   - Test migration with peer failure
   - Test migration with ongoing transfers
   - Test migration with resharding active (should fail)

## API Design

### REST API

```rust
POST /collections/{collection_name}/sharding/promote_to_custom
{
    "default_shard_key": "default"  // Optional, defaults to "default"
}
```

### gRPC API

```protobuf
message PromoteToCustomSharding {
    string collection_name = 1;
    optional ShardKey default_shard_key = 2;
}

rpc PromoteToCustomSharding(PromoteToCustomSharding) returns (OperationResponse);
```

## Implementation Checklist

- [ ] Add `PromoteToCustomSharding` to `CollectionMetaOperations`
- [ ] Implement validation in Dispatcher
- [ ] Implement TOC handler for migration operation
- [ ] Implement `promote_to_custom_sharding()` in Collection
- [ ] Update `ShardHolder` to support method change
- [ ] Update `ShardReplicaSet` to support shard key updates
- [ ] Update `check_compatible()` to allow Auto -> Custom
- [ ] Add REST API endpoint
- [ ] Add gRPC API endpoint
- [ ] Write unit tests
- [ ] Write integration tests
- [ ] Update documentation
- [ ] Add migration logging and metrics

## Performance Considerations

1. **Migration is Fast**
   - No data movement required
   - Only metadata updates (config, mapping)
   - Hash ring rebuild is O(shards)

2. **No Downtime**
   - Migration is atomic through consensus
   - Queries continue to work during migration
   - Only brief lock during state update

3. **Memory Impact**
   - Minimal - just updating in-memory structures
   - Hash ring rebuild is lightweight

## Security Considerations

1. **Authorization**
   - Migration should require admin/collection write permissions
   - Validate user has permission to modify collection

2. **Validation**
   - Ensure collection exists
   - Ensure collection is in correct state
   - Prevent concurrent migrations

## Future Enhancements

1. **Custom Default Shard Key**
   - Allow user to specify custom default shard key name
   - Validate shard key format

2. **Migration Status Tracking**
   - Track migration progress
   - Provide status endpoint

3. **Metrics**
   - Track migration operations
   - Monitor migration success/failure rates
