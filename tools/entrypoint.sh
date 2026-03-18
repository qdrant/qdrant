#!/usr/bin/env bash
# Script to run qdrant in docker container and handle contingencies, like OOM.
# The functioning logic is as follows:
# - If recovery mode is allowed, we check if qdrant was killed during initialization or not.
#   - If it was killed during initialization, we remove run qdrant in recovery mode
#   - If it was killed after initialization, do nothing and restart container
# - If recovery mode is not allowed, we just restart container

_term () {
  kill -TERM "$QDRANT_PID" 2>/dev/null
}

trap _term SIGTERM

_interrupt () {
  kill -INT "$QDRANT_PID" 2>/dev/null
}

trap _interrupt SIGINT

./qdrant $@ &

# Get PID for the traps
QDRANT_PID=$!
wait $QDRANT_PID

EXIT_CODE=$?

QDRANT_ALLOW_RECOVERY_MODE=${QDRANT_ALLOW_RECOVERY_MODE:-false}

# Check that recovery mode is allowed
if [ "$QDRANT_ALLOW_RECOVERY_MODE" != true ]; then
    exit $EXIT_CODE
fi

# Check that qdrant was killed (exit code 137)
# Ideally, we want to catch only OOM, but it's not possible to distinguish it from random kill signal
if [ $EXIT_CODE != 137 ]; then
    exit $EXIT_CODE
fi

QDRANT_INIT_FILE_PATH=${QDRANT_INIT_FILE_PATH:-'.qdrant-initialized'}
RECOVERY_MESSAGE="Qdrant was killed during initialization. Most likely it's Out-of-Memory.
Please check memory consumption, increase memory limit or remove some collections and restart"

# Check that qdrant was initialized
# Qdrant creates QDRANT_INIT_FILE_PATH file after initialization
# So if it doesn't exist, qdrant was killed during initialization
if [ ! -f "$QDRANT_INIT_FILE_PATH" ]; then
    # Run qdrant in recovery mode.
    # No collection operations are allowed in recovery mode except for removing collections
    QDRANT__STORAGE__RECOVERY_MODE="$RECOVERY_MESSAGE" ./qdrant $@ &
    # Get PID for the traps
    QDRANT_PID=$!
    wait $QDRANT_PID
    exit $?
fi

exit $EXIT_CODE
