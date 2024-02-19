#!/usr/bin/env bash

# Clean old RocksDB log files, always leave the last two.
#
# On invocation, the script will prompt whether to run in dry mode.
#
# Script MUST be run in the ./storage directory of Qdrant.

# Number of newest old logs to keep
KEEP_N=2
DRY=1
CLEAN_COUNT=0

set -e

function main {
    # Ensure script is run in storage directory
    if [[ "$(basename "$(pwd)")" != "storage" ]]; then
        echo Error: script must be run in ./storage directory
        echo Currently in "$(pwd)"
        exit 1
    fi

    echo This script removes old and obsolete log files to clean up disk space.
    echo It is potentionally dangerous. Always make sure you have a backup before running this.
    echo Qdrant must be stopped before running this script.

    # Ask for dry run
    yes_or_no "Dry run?" || DRY=0

    # Confirmation
    if [[ $DRY == 0 ]]; then
        yes_or_no "Are you sure you want to continue?" || (echo "Aborted"; exit 1)
    fi

    # Go through collections/shards/segments to clean
    for collection in $(find collections/* -maxdepth 0 -type d); do
        echo "Cleaning collection: $collection"
        for shard in $(find $collection/* -maxdepth 0 -type d); do
            for segment in $(find $shard/segments/* -maxdepth 0 -type d); do
                clean_segment "$segment"
            done
        done
    done

    echo Cleaned $CLEAN_COUNT old log files
}

function yes_or_no {
    while true; do
        read -p "$* [y/n]: " yn
        case $yn in
            [Yy]*) return 0 ;;
            [Nn]*) return 1 ;;
        esac
    done
}

function clean_segment {
    remove_oldest_logs_in "$segment"

    if [[ -d "$segment/payload_index" ]]; then
        remove_oldest_logs_in "$segment/payload_index"
    fi
}

function remove_oldest_logs_in {
    FILES=$(find "$1" -maxdepth 1 -name 'LOG.old.*' | sort -n | head -n -${KEEP_N})
    CLEAN_COUNT=$((CLEAN_COUNT + $(echo "$FILES" | wc -w)))
    remove $FILES
}

function remove {
    if [[ -z "$*" ]]; then
        return
    fi

    echo "+ rm -f" $*
    if [[ $DRY == 0 ]]; then
        rm -f $*
    fi
}

main
