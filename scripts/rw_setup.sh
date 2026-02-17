#!/bin/bash

# Multi-terminal RW Server Manager
# This script manages multiple rw.sh instances across different terminals

set -e
SCRIPT_DIR="${BASH_SOURCE[0]%/*}"

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
    # Check if rw.sh exists
    if [[ ! -f "$SCRIPT_DIR/rw.sh" ]]; then
        echo "Error: rw.sh not found in current directory"
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
    # ToDo: Add bfb continous upsert script
    echo "Opening cleanup terminal..."
    open_terminal "echo sleeping for 10s && sleep 10 && bfb --replication-factor 3 --shards 2 --indexing-threshold 0 -n 100k -p 8 -t 8 --uri http://127.0.0.1:6334 && bfb --skip-create -n 10M --max-id 100k" "bfb upsert"

    echo ""
    echo "All terminals launched!"
    echo "- Cleanup completed in first terminal"
    echo "- 6 server instances running in separate terminals"
    echo "- BFB upsert"
    echo "- Each terminal will stay open until you press Enter"
    echo ""
    echo "To stop all servers, close the terminal windows or press Ctrl+C in each."
}

# Run main function
main "$@"
