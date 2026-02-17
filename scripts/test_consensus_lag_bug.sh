#!/bin/bash

# Test script to reproduce consensus lag bug
# This script simulates consensus lag and verifies that creating a new collection
# disrupts existing collections when some nodes lag behind in applying consensus operations

set -e

SCRIPT_DIR="${BASH_SOURCE[0]%/*}"
BASE_URL="http://127.0.0.1:6335"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log() {
    echo -e "${GREEN}[TEST]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Wait for cluster to be ready
wait_for_cluster() {
    log "Waiting for cluster to be ready..."
    for i in {1..30}; do
        if curl -s "$BASE_URL/readyz" > /dev/null 2>&1; then
            log "Cluster is ready"
            return 0
        fi
        sleep 1
    done
    error "Cluster is not ready"
    return 1
}

# Get cluster status and peer IDs
get_peer_ids() {
    curl -s "$BASE_URL/cluster" | jq -r '.raft_info.peers[]?.id // empty' | sort -n
}

# Create a collection
create_collection() {
    local name=$1
    local shards=${2:-1}
    local replication_factor=${3:-1}
    
    log "Creating collection: $name (shards=$shards, replication_factor=$replication_factor)"
    
    local response=$(curl -s -X PUT "$BASE_URL/collections/$name" \
        -H "Content-Type: application/json" \
        -d "{
            \"vectors\": {
                \"size\": 128,
                \"distance\": \"Cosine\"
            },
            \"shard_number\": $shards,
            \"replication_factor\": $replication_factor
        }")
    
    if echo "$response" | jq -e '.error' > /dev/null 2>&1; then
        error "Failed to create collection $name: $(echo "$response" | jq -r '.error.message // .error')"
        return 1
    fi
    
    # Wait for collection to be ready
    sleep 3
    log "Collection $name created successfully"
}

# Propose TestSlowDown operation via cluster API
# Note: TestSlowDown requires a collection name, but affects all operations on the target peer
propose_slowdown() {
    local collection_name=$1
    local peer_id=${2:-null}
    local duration=${3:-5.0}
    
    log "Proposing TestSlowDown: collection=$collection_name, peer_id=$peer_id, duration=${duration}s"
    
    local body
    if [ "$peer_id" = "null" ]; then
        body="{\"test_slow_down\": {\"duration\": $duration}}"
    else
        body="{\"test_slow_down\": {\"peer_id\": $peer_id, \"duration\": $duration}}"
    fi
    
    local response=$(curl -s -X POST "$BASE_URL/collections/$collection_name/cluster?wait=true" \
        -H "Content-Type: application/json" \
        -d "$body")
    
    if echo "$response" | jq -e '.error' > /dev/null 2>&1; then
        warn "TestSlowDown proposal returned error: $(echo "$response" | jq -r '.error.message // .error')"
    else
        log "TestSlowDown proposed successfully"
    fi
}

# Check collection cluster info
check_collection_cluster_info() {
    local name=$1
    curl -s "$BASE_URL/collections/$name/cluster" | jq '.'
}

# Check for replica state issues
check_replica_states() {
    local name=$1
    log "Checking replica states for collection: $name"
    
    local cluster_info=$(check_collection_cluster_info "$name")
    
    # Check each shard
    echo "$cluster_info" | jq -r '.shards[] | "Shard \(.shard_id): \(.replicas | length) replicas"'
    
    # Check for Dead replicas
    local dead_replicas=$(echo "$cluster_info" | jq '[.shards[].replicas[] | select(.state == "Dead")] | length')
    if [ "$dead_replicas" -gt 0 ]; then
        warn "Found $dead_replicas Dead replica(s) in collection $name"
        echo "$cluster_info" | jq '[.shards[].replicas[] | select(.state == "Dead")] | {shard_id, peer_id, state}'
    fi
    
    # Check for issues with last active replica
    echo "$cluster_info" | jq -r '.shards[] | 
        "Shard \(.shard_id): Active replicas: \(.replicas | map(select(.state == "Active")) | length)"'
}

# Main test scenario
main() {
    log "=== Starting consensus lag bug reproduction test ==="
    
    # Wait for cluster
    wait_for_cluster
    
    # Get peer IDs
    local peer_ids=($(get_peer_ids))
    log "Cluster peer IDs: ${peer_ids[*]}"
    
    if [ ${#peer_ids[@]} -lt 3 ]; then
        error "Need at least 3 nodes for this test. Found: ${#peer_ids[@]}"
        exit 1
    fi
    
    # Use peer 102 (node 2) as the lagging node
    local lagging_peer=102
    
    log "=== Step 1: Create existing collection with replication ==="
    create_collection "existing_collection" 3 3
    
    # Check initial state
    log "Initial state of existing_collection:"
    check_replica_states "existing_collection"
    
    log "=== Step 2: Introduce lag by proposing TestSlowDown on node 2 ==="
    # Propose a slowdown that will delay operations on the lagging node
    # This will cause node 2 to lag behind in applying consensus operations
    propose_slowdown "existing_collection" "$lagging_peer" 15.0
    
    log "=== Step 3: Create new collection while lag exists ==="
    log "This should trigger InitializeReplica operations that might conflict with existing collection state"
    
    # Create new collection - this will trigger operations that might conflict
    # if the lagging node has stale state
    (
        log "Creating new collection in background..."
        create_collection "new_collection" 3 3
    ) &
    
    local create_pid=$!
    
    # Wait a bit for operations to start propagating
    sleep 2
    
    log "=== Step 4: Check if existing collection is disrupted ==="
    # While new collection is being created, check if existing collection gets disrupted
    for i in {1..5}; do
        log "Check iteration $i:"
        check_replica_states "existing_collection"
        sleep 2
    done
    
    # Wait for new collection creation to complete
    log "Waiting for new collection creation to complete..."
    wait $create_pid || {
        error "New collection creation failed or had errors"
    }
    
    log "=== Step 5: Final state check ==="
    log "Checking final state of both collections:"
    check_replica_states "existing_collection"
    check_replica_states "new_collection"
    
    log "=== Test completed ==="
    log "Check the logs for errors like:"
    log "  - 'Replica X of shard Y has state Some(Dead), but expected Some(Active)'"
    log "  - 'Cannot deactivate the last active replica X of shard Y'"
    log ""
    log "If you see these errors, the bug has been reproduced!"
}

# Run main function
main "$@"
