#!/bin/bash

# Create a log file
LOG_FILE="resource_usage.log"

# Function to log CPU, RAM, and Disk usage
log_usage() {
  while true; do
    # Get the number of CPU cores
    CPU_CORES=$(nproc)

    # Get current overall CPU usage
    CPU_USAGE=$(top -bn1 | grep "Cpu(s)" | sed "s/.*, *\([0-9.]*\)%* id.*/\1/" | awk '{print 100 - $1}')

    # Get current RAM usage (absolute and percentage)
    RAM_INFO=$(free -m | awk '/Mem:/ {printf "%.2f MB (%.2f%%)", $3, $3/$2 * 100.0}')

    # Get total disk usage
    DISK_USAGE=$(df -h / | awk 'NR==2 {print $3 "/" $2 " (" $5 ")"}')

    # Append the usage to the log file
    echo "$(date): CPU Usage: $CPU_USAGE%, CPU Cores: $CPU_CORES, RAM Usage: $RAM_INFO, Disk Usage: $DISK_USAGE" >> $LOG_FILE
    sleep 5  # Log every 5 seconds
  done
}

# Start logging in the background
log_usage &
MONITOR_PID=$!
echo "Monitoring PID: $MONITOR_PID"
