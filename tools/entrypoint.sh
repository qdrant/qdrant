#!/usr/bin/env sh
# Script to run qdrant in docker container and handle contingencies, like OOM.
# The functioning logic is as follows:
# - If recovery mode is allowed, we check if qdrant was killed during initialization or not.
#   - If it was killed during initialization, we remove run qdrant in recovery mode
#   - If it was killed after initialization, do nothing and restart container
# - If recovery mode is not allowed, we just restart container

./qdrant $@

EXIT_CODE=$?

QDRANT_ALLOW_RECOVERY_MODE=${QDRANT_ALLOW_RECOVERY_MODE:-false}

# Check that recovery mode is allowed
if [ "$QDRANT_ALLOW_RECOVERY_MODE" = true ]; then

  IS_INITIALIZED_FILE='.qdrant-initialized'

  RECOVERY_MESSAGE="Qdrant was killed during initialization. Most likely it's Out-of-Memory. \
  Please check memory consumption, increase memory limit or remove some collections and restart"

  # Check that qdrant was killed (exit code 137)
  # Ideally, we want to catch only OOM, but it's not possible to distinguish it from random kill signal
  if [ $EXIT_CODE -eq 137 ]; then
    # Check that qdrant was initialized
    # Qdrant creates IS_INITIALIZED_FILE file after initialization
    # So if it doesn't exist, qdrant was killed during initialization
    if [ ! -f "$IS_INITIALIZED_FILE" ]; then
      QDRANT__STORAGE__RECOVERY_MODE="$RECOVERY_MESSAGE" ./qdrant $@
      exit $?
    fi
  fi
fi

exit $EXIT_CODE