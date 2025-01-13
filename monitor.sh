#!/bin/bash

# Create a log file
LOG_FILE="resource_usage.log"

# Function to log CPU and RAM usage
log_usage() {
  while true; do
    # Get current CPU and memory usage
    CPU_USAGE=$(top -bn1 | grep "Cpu(s)" | sed "s/.*, *\([0-9.]*\)%* id.*/\1/" | awk '{print 100 - $1}')
    MEM_USAGE=$(free -m | awk '/Mem:/ {printf "%.2f", $3/$2 * 100.0}')

    # Append the usage to the log file
    echo "$(date): CPU Usage: $CPU_USAGE%, RAM Usage: $MEM_USAGE%" >> $LOG_FILE
    sleep 1  # Log every 5 seconds
  done
}

# Start logging in the background
log_usage &
MONITOR_PID=$!
