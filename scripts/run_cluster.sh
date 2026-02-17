#!/bin/bash

# Multi-terminal Qdrant Cluster Manager
# This script manages multiple run.sh instances across different terminals

set -e
SCRIPT_DIR="${BASH_SOURCE[0]%/*}"

# Default number of nodes
NUM_NODES=${1:-3}

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
    local terminal=$(detect_terminal)

    case "$terminal" in
        "gnome-terminal")
            gnome-terminal --title="$title" -- bash -c "$cmd; echo 'Press Enter to close...'; read"
            ;;
        "xterm")
            xterm -title "$title" -e bash -c "$cmd; echo 'Press Enter to close...'; read" &
            ;;
        "konsole")
            konsole --title "$title" -e bash -c "$cmd; echo 'Press Enter to close...'; read" &
            ;;
        "xfce4-terminal")
            xfce4-terminal --title="$title" -e "bash -c '$cmd; echo Press Enter to close...; read'" &
            ;;
        "alacritty")
            alacritty --title "$title" -e bash -c "$cmd; echo 'Press Enter to close...'; read" &
            ;;
        "kitty")
            kitty --title "$title" bash -c "$cmd; echo 'Press Enter to close...'; read" &
            ;;
        *)
            echo "No supported terminal emulator found!"
            echo "Please install one of: gnome-terminal, xterm, konsole, xfce4-terminal, alacritty, or kitty"
            exit 1
            ;;
    esac
}

# Main execution
main() {
    # Check if run.sh exists
    if [[ ! -f "$SCRIPT_DIR/run.sh" ]]; then
        echo "Error: run.sh not found in $SCRIPT_DIR"
        exit 1
    fi

    # Make run.sh executable if it isn't
    if [[ ! -x "$SCRIPT_DIR/run.sh" ]]; then
        chmod +x "$SCRIPT_DIR/run.sh"
    fi

    # Validate number of nodes
    if ! [[ "$NUM_NODES" =~ ^[0-9]+$ ]] || [ "$NUM_NODES" -lt 1 ]; then
        echo "Error: Number of nodes must be a positive integer"
        echo "Usage: $0 [NUM_NODES]"
        exit 1
    fi

    echo "Starting Qdrant cluster with $NUM_NODES nodes..."
    echo "This will open $NUM_NODES terminal windows"
    echo ""

    # Step 1: Cleanup in a new terminal (optional)
    echo "Opening cleanup terminal..."
    open_terminal "$SCRIPT_DIR/run.sh rm" "Qdrant Cleanup"

    # Give cleanup time to complete
    sleep 2

    # Step 2: Start bootstrap node (ID=1) first
    echo "Opening terminal for bootstrap node (ID=1)..."
    open_terminal "$SCRIPT_DIR/run.sh 1" "Qdrant Node 1 (Bootstrap)"

    # Wait until bootstrap node is running with timeout of 10s
    timeout 10s bash -c 'while ! curl -s http://127.0.0.1:6333/readyz > /dev/null; do sleep 1; done' || {
        echo "Error: Bootstrap node is not running"
        exit 1
    }

    # Step 3: Start remaining nodes in separate terminals
    for node_id in $(seq 2 $NUM_NODES); do
        echo "Opening terminal for node $node_id..."
        open_terminal "$SCRIPT_DIR/run.sh $node_id" "Qdrant Node $node_id"
        sleep 0.1  # Small delay between terminal launches
    done

    echo ""
    echo "All terminals launched!"
    echo "- Cleanup completed in first terminal"
    echo "- Bootstrap node (ID=1) running on http://127.0.0.1:6335"
    echo "- $((NUM_NODES - 1)) additional nodes running"
    echo "- Each terminal will stay open until you press Enter"
    echo ""
    echo "To stop all servers, close the terminal windows or press Ctrl+C in each."
    echo ""
    echo "Node endpoints:"
    for node_id in $(seq 1 $NUM_NODES); do
        if [ $node_id -eq 1 ]; then
            echo "  Node $node_id: http://127.0.0.1:6335 (Bootstrap)"
        else
            echo "  Node $node_id: http://127.0.0.$node_id:6335"
        fi
    done
}

# Run main function
main "$@"
