#!/bin/bash

RUN_PER_PACKAGE=${RUN_PER_PACKAGE:-"false"}

if [ "$RUN_PER_PACKAGE" == "false" ]; then
    # Run for the entire workspace in one shot. This assumes we have enough memory
    cargo llvm-cov nextest --profile ci --workspace --html
    exit 0
fi

PACKAGES=($(cargo metadata --format-version 1 | jq -r '.workspace_members[] | split("/") | .[-1] | split("#")[0]' | sort))
REPORT_DIR="target/llvm-cov/package-reports"

echo "Workspace packages: ${PACKAGES[*]}"

mkdir -p "$REPORT_DIR"

LCOV_COMMAND_ARGS=""

for PACKAGE in "${PACKAGES[@]}"; do
    echo "Testing PACKAGE with coverage: $PACKAGE"
    # Profile "ci" is configured in .config/nextest.toml
    cargo llvm-cov --no-clean nextest --profile ci -p "$PACKAGE" --lcov --output-path "$REPORT_DIR/$PACKAGE.info"

    LCOV_COMMAND_ARGS="${LCOV_COMMAND_ARGS} -a $REPORT_DIR/$PACKAGE.info"
done

if [ -n "$LCOV_COMMAND_ARGS" ]; then
    lcov $LCOV_COMMAND_ARGS --output-file lcov.info
fi
