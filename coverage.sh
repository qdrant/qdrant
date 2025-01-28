#!/bin/bash
REPORT_DIR="target/llvm-cov/crate-reports"

WORKSPACE_CRATES=$(cargo metadata --format-version 1 | jq -r '.workspace_members[]' | awk -F '/' '{print $NF}' | cut -d '#' -f 1)

mkdir -p "$REPORT_DIR"

LCOV_COMMAND_ARGS=""

for CRATE in $WORKSPACE_CRATES; do
    if [ "$CRATE" == "qdrant" ]; then
        continue
    fi

    echo "Testing crate with coverage: $CRATE"
    cargo llvm-cov --no-clean nextest --profile ci --jobs=1 -p "$CRATE" --lcov --output-path "$REPORT_DIR/$CRATE.info"

    LCOV_COMMAND_ARGS="${LCOV_COMMAND_ARGS} -a $REPORT_DIR/$CRATE.info"
done

lcov $LCOV_COMMAND_ARGS --output-file lcov.info
