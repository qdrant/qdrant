# Consensus Lag Bug Reproduction Test

This test reproduces a bug where consensus lag on some machines causes creating a new collection to disrupt existing collections.

## Bug Description

When consensus lags behind on some nodes and a new collection is created, it can cause errors in existing collections:

- `Replica X of shard Y has state Some(Dead), but expected Some(Active)`
- `Cannot deactivate the last active replica X of shard Y`

## Test Setup

1. Start a 3-node cluster using `run_cluster.sh`:
   ```bash
   ./scripts/run_cluster.sh 3
   ```

2. Wait for all nodes to be ready (check the terminal windows)

## Running the Test

### Option 1: Python Script (Recommended)

```bash
python3 scripts/test_consensus_lag_bug.py
```

### Option 2: Bash Script

```bash
./scripts/test_consensus_lag_bug.sh
```

## What the Test Does

1. **Creates an existing collection** (`existing_collection`) with 3 shards and replication factor 3
2. **Introduces lag** by proposing `TestSlowDown` operations targeting node 2 (peer ID 102)
3. **Creates a new collection** (`new_collection`) while lag exists
4. **Monitors** the existing collection for disruptions

## Expected Behavior (Bug Reproduction)

If the bug is reproduced, you should see in the logs:

```
WARN storage::content_manager::consensus_manager: Failed to apply collection meta operation entry with user error: Wrong input: Replica 3267267825477813 of shard 0 has state Some(Dead), but expected Some(Active)
WARN storage::content_manager::consensus_manager: Failed to apply collection meta operation entry with user error: Wrong input: Cannot deactivate the last active replica 3267267825477813 of shard 2
```

## How TestSlowDown Works

The `TestSlowDown` operation:
- Is proposed via consensus (so it's applied on all nodes)
- When applied on the target peer, it sleeps for the specified duration
- This delays the application of subsequent consensus operations on that peer
- Creates a lag where the peer falls behind in applying operations

## Understanding the Bug

The bug occurs when:
1. Consensus operations are committed in a certain order
2. Some nodes lag behind in applying these operations
3. New operations are proposed that depend on the current state
4. The lagging node applies operations based on stale state, causing conflicts

For example:
- Operation A: Mark replica X as Dead (for existing collection)
- Operation B: Initialize replica Y (for new collection) - proposed based on current state
- On lagging node: Operation B is applied before Operation A completes
- Result: Operation B expects replica X to be Active, but it's already Dead

## Troubleshooting

If the test doesn't reproduce the bug:
1. Increase the `TestSlowDown` duration (currently 20 seconds)
2. Try creating multiple collections in quick succession
3. Check that all nodes are actually lagging (monitor logs)
4. Verify that the cluster has proper replication (3 nodes, replication factor 3)

## Next Steps

Once the bug is reproduced:
1. Investigate why operations from different collections interfere
2. Check if operations should be isolated per collection
3. Consider adding validation to prevent state conflicts
4. Review the consensus application logic for race conditions
