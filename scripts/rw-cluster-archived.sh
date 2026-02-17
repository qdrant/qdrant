#!/bin/bash
set -e


SCRIPT_DIR="${BASH_SOURCE[0]%/*}"

# This script sets up a Qdrant cluster (read or write) with a specified number of nodes

# Function to print usage information
usage() {
    echo "Usage: $0 [read|write|stop] <number_of_nodes|cluster_type> [rm]"
    echo ""
    echo "Commands:"
    echo "  read|write <nodes>  Create a read or write cluster with specified number of nodes"
    echo "  stop <cluster_type> Stop all nodes in the specified cluster (read or write)"
    echo ""
    echo "Options:"
    echo "  number_of_nodes     Number of nodes to create in the cluster (must be >= 1)"
    echo "  cluster_type        Type of cluster to stop (read or write)"
    echo "  rm                  Optional: Remove all storage for the specified cluster type before starting"
    echo ""
    echo "Examples:"
    echo "  $0 read 3           # Create a read cluster with 3 nodes"
    echo "  $0 write 2          # Create a write cluster with 2 nodes" 
    echo "  $0 read 3 rm        # Remove storage and create a read cluster with 3 nodes"
    echo "  $0 stop read        # Stop all nodes in the read cluster"
    exit 1
}

# Check for minimum required arguments
if [ $# -lt 1 ]; then
    usage
fi

# Handle stop command
if [[ "$1" == "stop" ]]; then
    if [ $# -lt 2 ]; then
        echo "Error: Please specify which cluster type to stop (read or write)"
        usage
    fi
    
    MODE=$2
    if [[ "$MODE" != "read" && "$MODE" != "write" ]]; then
        echo "Error: Cluster type must be 'read' or 'write'"
        usage
    fi
    
    echo "Stopping $MODE cluster..."
    
    # Find and kill all processes for the specified cluster type
    for pid_file in ${MODE}_node_*.pid; do
        if [ -f "$pid_file" ]; then
            pid=$(cat "$pid_file")
            echo "Stopping node with PID $pid..."
            kill $pid 2>/dev/null || echo "Process $pid already stopped"
            rm "$pid_file"
        fi
    done
    
    echo "$MODE cluster stopped!"
    exit 0
fi

# For create commands, need at least 2 arguments
if [ $# -lt 2 ]; then
    usage
fi

# Validate mode argument
MODE=$1
if [[ $MODE != "read" && $MODE != "write" ]]; then
    echo "Error: First argument must be 'read', 'write', or 'stop'"
    usage
fi

# Validate node count
NODE_COUNT=$2
if [[ ! $NODE_COUNT =~ ^[0-9]+$ || $NODE_COUNT -lt 1 ]]; then
    echo "Error: Number of nodes must be a positive integer"
    usage
fi

# Function to start a node in the background and capture its PID
start_node() {
    local node_id=$1
    local is_first=$2
    
    if [ "$is_first" = true ]; then
        echo "Starting $MODE node $node_id (bootstrap node)..."
    else
        echo "Starting $MODE node $node_id..."
    fi
    
    # Use nohup to keep the process running after the script exits
    nohup ./rw.sh $MODE $node_id > ${MODE}_node_${node_id}.log 2>&1 &
    echo $! > ${MODE}_node_${node_id}.pid
    echo "Node $node_id started with PID $(cat ${MODE}_node_${node_id}.pid)"
    
    # Give the bootstrap node a moment to initialize before starting other nodes
    if [ "$is_first" = true ]; then
        echo "Waiting for bootstrap node to initialize..."
        sleep 5
    fi
}

# Check for optional rm argument
RM_OPTION=""
if [[ $# -eq 3 && $3 == "rm" ]]; then
    RM_OPTION="rm"
    # Remove existing storage first
    echo "Removing existing $MODE cluster storage..."
    $SCRIPT_DIR/rw.sh $MODE 1 rm
fi

# Create directory for PIDs and logs if it doesn't exist
mkdir -p cluster_logs

# Start the nodes
echo "Setting up $MODE cluster with $NODE_COUNT nodes..."

# Start the bootstrap node (node 1) first
start_node 1 true

# Start the remaining nodes
for (( i=2; i<=$NODE_COUNT; i++ )); do
    start_node $i false
    # Add a small delay between node starts to prevent race conditions
    sleep 2
done

echo ""
echo "$MODE cluster with $NODE_COUNT nodes is now running!"
echo "To stop the cluster, run: $0 stop $MODE"
echo "To view logs, check the ${MODE}_node_*.log files"
echo ""
