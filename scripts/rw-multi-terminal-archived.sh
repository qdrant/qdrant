#!/bin/bash
# Multi-terminal RW Server Manager
# This script manages multiple rw.sh instances across different terminals
set -e

# Array to store child process PIDs
declare -a CHILD_PIDS=()
SCRIPT_DIR="${BASH_SOURCE[0]%/*}"

# Function to cleanup all child processes
cleanup() {
    echo ""
    echo "Shutting down all child processes..."
    
    # Kill all child processes
    for pid in "${CHILD_PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            echo "Killing process $pid..."
            kill -TERM "$pid" 2>/dev/null || true
            # Give process time to terminate gracefully
            sleep 0.5
            # Force kill if still running
            if kill -0 "$pid" 2>/dev/null; then
                kill -KILL "$pid" 2>/dev/null || true
            fi
        fi
    done
    
    # Also kill any rw.sh processes that might be running
    pkill -f "$SCRIPT_DIR/rw.sh" 2>/dev/null || true
    
    echo "All child processes terminated."
    exit 0
}

# Set up signal handlers
trap cleanup SIGINT SIGTERM EXIT

# Function to detect available terminal emulator
detect_terminal() {
    if command -v gnome-terminal >/dev/null 2>&1; then
        echo "gnome-terminal"
    elif command -v xterm >/dev/null 2>&1; then
        echo "xterm"
    elif command -v konsole >/dev/null 2>&1; then
        echo "konsole"
    elif command -v xfce4-terminal >/dev/null 2>&1; then
        echo "xfce4-terminal"
    elif command -v alacritty >/dev/null 2>&1; then
        echo "alacritty"
    elif command -v kitty >/dev/null 2>&1; then
        echo "kitty"
    else
        echo "none"
    fi
}

# Function to open command in new terminal
open_terminal() {
    local cmd="$1"
    local title="$2"
    local terminal="alacritty" # $(detect_terminal)
    local pid
    
    case "$terminal" in
        "gnome-terminal")
            gnome-terminal --title="$title" -- bash -c "$cmd; echo 'Press Enter to close...'; read" &
            pid=$!
            ;;
        "xterm")
            xterm -title "$title" -e bash -c "$cmd; echo 'Press Enter to close...'; read" &
            pid=$!
            ;;
        "konsole")
            konsole --title "$title" -e bash -c "$cmd; echo 'Press Enter to close...'; read" &
            pid=$!
            ;;
        "xfce4-terminal")
            xfce4-terminal --title="$title" -e "bash -c '$cmd; echo Press Enter to close...; read'" &
            pid=$!
            ;;
        "alacritty")
            alacritty --title "$title" -e bash -c "$cmd; echo 'Press Enter to close...'; read" &
            pid=$!
            ;;
        "kitty")
            kitty --title "$title" bash -c "$cmd; echo 'Press Enter to close...'; read" &
            pid=$!
            ;;
        *)
            echo "No supported terminal emulator found!"
            echo "Please install one of: gnome-terminal, xterm, konsole, xfce4-terminal, alacritty, or kitty"
            exit 1
            ;;
    esac
    
    # Store the PID for cleanup
    CHILD_PIDS+=("$pid")
    echo "Started terminal with PID: $pid"
}

# Main execution
main() {
    # Check if rw.sh exists
    if [[ ! -f "$SCRIPT_DIR/rw.sh" ]]; then
        echo "Error: rw.sh not found in $SCRIPT_DIR"
        exit 1
    fi
    
    # Make rw.sh executable if it isn't
    if [[ ! -x "$SCRIPT_DIR/rw.sh" ]]; then
        chmod +x $SCRIPT_DIR/rw.sh
    fi
    
    echo "Starting RW Server cluster..."
    echo "This will open 7 terminal windows:"
    echo "- 1 cleanup terminal"
    echo "- 6 server terminals (read/write modes with node IDs 1-3)"
    echo "- 1 bfb upsert terminal"
    echo ""
    echo "Press Ctrl+C to stop all processes and terminals."
    echo ""
    
    # Step 1: Cleanup in a new terminal
    echo "Opening cleanup terminal..."
    open_terminal "$SCRIPT_DIR/rw.sh read rm && $SCRIPT_DIR/rw.sh write rm" "RW Cleanup"
    
    # Give cleanup time to complete
    sleep 2
    
    # Step 2: Start servers in separate terminals
    modes=("read" "write")
    
    for mode in "${modes[@]}"; do
        for node_id in {1..3}; do
            echo "Opening terminal for $mode mode, node $node_id..."
            open_terminal "$SCRIPT_DIR/rw.sh $mode $node_id" "RW Server ($mode-$node_id)"
            sleep 0.1  # Small delay between terminal launches
        done
    done
    
    # Step 3: Open bfb terminal
    echo "Opening bfb upsert terminal..."
    open_terminal "echo sleeping for 10s && sleep 10 && bfb --replication-factor 3 --shards 1 --indexing-threshold 0 -n 100000 -p 8 -t 8 --uri http://127.0.0.1:6334" "bfb upsert"
    
    echo ""
    echo "All terminals launched!"
    echo "- Cleanup completed in first terminal"
    echo "- 6 server instances running in separate terminals"
    echo "- BFB upsert terminal started"
    echo "- Each terminal will stay open until you press Enter"
    echo ""
    echo "Main process is running. Press Ctrl+C to stop all servers and terminals."
    
    # Keep the main process running and wait for signals
    while true; do
        sleep 1
        
        # Check if any child processes have died and remove them from tracking
        for i in "${!CHILD_PIDS[@]}"; do
            if ! kill -0 "${CHILD_PIDS[i]}" 2>/dev/null; then
                unset 'CHILD_PIDS[i]'
            fi
        done
        
        # Rebuild array to remove gaps
        CHILD_PIDS=("${CHILD_PIDS[@]}")
        
        # If all children are dead, exit
        if [ ${#CHILD_PIDS[@]} -eq 0 ]; then
            echo "All child processes have terminated. Exiting."
            break
        fi
    done
}

# Run main function
main "$@"
