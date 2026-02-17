#!/usr/bin/env python3
"""
Test script to reproduce consensus lag bug.

This script simulates the scenario where:
1. An existing collection has replicas
2. Consensus lags behind on some nodes (simulated with TestSlowDown)
3. A new collection is created
4. The lag causes operations to be applied in a way that disrupts existing collections
"""

import json
import time
import requests
import sys
from typing import List, Optional

BASE_URL = "http://127.0.0.1:6333"
NODE2_URL = "http://127.0.0.2:6333"
NODE3_URL = "http://127.0.0.3:6333"
LAGGING_PEER_ID = 102  # Node 2


def log(msg: str):
    print(f"[TEST] {msg}")


def warn(msg: str):
    print(f"[WARN] {msg}")


def error(msg: str):
    print(f"[ERROR] {msg}")


def wait_for_cluster(max_retries: int = 30):
    """Wait for cluster to be ready."""
    log("Waiting for cluster to be ready...")
    for i in range(max_retries):
        try:
            resp = requests.get(f"{BASE_URL}/readyz", timeout=1)
            if resp.status_code == 200:
                log("Cluster is ready")
                return True
        except:
            pass
        time.sleep(1)
    error("Cluster is not ready")
    return False


def get_peer_ids() -> List[int]:
    """Get list of peer IDs in the cluster."""
    try:
        resp = requests.get(f"{BASE_URL}/cluster")
        resp.raise_for_status()
        data = resp.json()
        peers = data.get("raft_info", {}).get("peers", [])
        return sorted([p.get("id") for p in peers if p.get("id")])
    except Exception as e:
        error(f"Failed to get peer IDs: {e}")
        return []


def create_collection(name: str, shards: int = 1, replication_factor: int = 1) -> bool:
    """Create a collection."""
    log(f"Creating collection: {name} (shards={shards}, replication_factor={replication_factor})")
    
    payload = {
        "vectors": {
            "size": 128,
            "distance": "Cosine"
        },
        "shard_number": shards,
        "replication_factor": replication_factor
    }
    
    try:
        resp = requests.put(
            f"{BASE_URL}/collections/{name}",
            json=payload,
            timeout=30
        )
        resp.raise_for_status()
        time.sleep(3)  # Wait for collection to be ready
        log(f"Collection {name} created successfully")
        return True
    except Exception as e:
        error(f"Failed to create collection {name}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            try:
                error(f"Response: {e.response.text}")
            except:
                pass
        return False


def propose_slowdown(collection_name: str, peer_id: Optional[int] = None, duration: float = 5.0) -> bool:
    """Propose TestSlowDown operation to consensus."""
    log(f"Proposing TestSlowDown: collection={collection_name}, peer_id={peer_id}, duration={duration}s")
    
    payload = {
        "test_slow_down": {
            "duration": duration
        }
    }
    
    if peer_id is not None:
        payload["test_slow_down"]["peer_id"] = peer_id
    
    try:
        resp = requests.post(
            f"{BASE_URL}/collections/{collection_name}/cluster?wait=true",
            json=payload,
            timeout=60
        )
        
        if resp.status_code != 200:
            warn(f"TestSlowDown proposal returned status {resp.status_code}: {resp.text}")
            return False
        
        log("TestSlowDown proposed successfully")
        return True
    except Exception as e:
        warn(f"Failed to propose TestSlowDown: {e}")
        return False


def check_collection_cluster_info(name: str):
    """Get cluster info for a collection."""
    try:
        resp = requests.get(f"{BASE_URL}/collections/{name}/cluster")
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        error(f"Failed to get cluster info for {name}: {e}")
        return None


def check_replica_states(name: str):
    """Check replica states and report issues."""
    log(f"Checking replica states for collection: {name}")
    
    info = check_collection_cluster_info(name)
    if not info:
        return
    
    shards = info.get("shards", [])
    
    for shard in shards:
        shard_id = shard.get("shard_id")
        replicas = shard.get("replicas", [])
        active_count = sum(1 for r in replicas if r.get("state") == "Active")
        dead_count = sum(1 for r in replicas if r.get("state") == "Dead")
        
        log(f"  Shard {shard_id}: {len(replicas)} replicas ({active_count} Active, {dead_count} Dead)")
        
        if dead_count > 0:
            warn(f"  Found Dead replicas in shard {shard_id}:")
            for r in replicas:
                if r.get("state") == "Dead":
                    warn(f"    Peer {r.get('peer_id')}: {r.get('state')}")
        
        if active_count == 0 and len(replicas) > 0:
            error(f"  WARNING: Shard {shard_id} has no Active replicas!")


def main():
    """Main test scenario."""
    log("=== Starting consensus lag bug reproduction test ===")
    
    # Wait for cluster
    if not wait_for_cluster():
        sys.exit(1)
    
    # Get peer IDs
    peer_ids = get_peer_ids()
    log(f"Cluster peer IDs: {peer_ids}")
    
    if len(peer_ids) < 3:
        error(f"Need at least 3 nodes for this test. Found: {len(peer_ids)}")
        sys.exit(1)
    
    # Step 1: Create existing collection
    log("\n=== Step 1: Create existing collection with replication ===")
    if not create_collection("existing_collection", shards=3, replication_factor=3):
        error("Failed to create existing collection")
        sys.exit(1)
    
    log("Initial state of existing_collection:")
    check_replica_states("existing_collection")
    
    # Step 2: Introduce lag by proposing multiple TestSlowDown operations
    log("\n=== Step 2: Introduce lag by proposing TestSlowDown on node 2 ===")
    log("This will cause node 2 to lag behind in applying consensus operations")
    
    # Propose multiple slowdowns to create a queue of delayed operations
    # Each slowdown will delay the node when it's applied
    log("Proposing first TestSlowDown (will delay node 2 when applied)...")
    propose_slowdown("existing_collection", peer_id=LAGGING_PEER_ID, duration=10.0)
    
    # Immediately propose another slowdown - this will queue up
    # The lagging node will apply these sequentially, creating lag
    log("Proposing second TestSlowDown to create operation queue...")
    propose_slowdown("existing_collection", peer_id=LAGGING_PEER_ID, duration=10.0)
    
    # Give it a moment for operations to start propagating
    time.sleep(1)
    
    # Step 3: Create new collection while lag exists
    log("\n=== Step 3: Create new collection while lag exists ===")
    log("This should trigger InitializeReplica operations")
    log("The lagging node will apply these operations after the slowdowns, potentially with stale state")
    
    # Create new collection - this will trigger operations that might conflict
    # if the lagging node has stale state when it applies them
    log("Creating new collection (operations will queue behind slowdowns on lagging node)...")
    success = create_collection("new_collection", shards=3, replication_factor=3)
    
    # Step 4: Check for disruptions
    log("\n=== Step 4: Check if existing collection is disrupted ===")
    
    # Check multiple times to catch transient issues
    for i in range(3):
        log(f"Check iteration {i+1}:")
        check_replica_states("existing_collection")
        time.sleep(2)
    
    # Step 5: Final check
    log("\n=== Step 5: Final state check ===")
    log("Checking final state of both collections:")
    check_replica_states("existing_collection")
    if success:
        check_replica_states("new_collection")
    
    log("\n=== Test completed ===")
    log("Check the logs for errors like:")
    log("  - 'Replica X of shard Y has state Some(Dead), but expected Some(Active)'")
    log("  - 'Cannot deactivate the last active replica X of shard Y'")
    log("")
    log("If you see these errors, the bug has been reproduced!")


if __name__ == "__main__":
    main()
