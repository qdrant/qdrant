#!/bin/bash

# Usage: tools/coverage.sh
#
# For running on low RAM machines like Github CI, Use `RUN_PER_PACKAGE=true tools/coverage.sh`
#
# If using locally, occasionally run `cargo llvm-cov clean` to avoid bloating `target/llvm-cov-target` dir with .profraw files

RUN_PER_PACKAGE=${RUN_PER_PACKAGE:-"false"}

if [ "$RUN_PER_PACKAGE" == "false" ]; then
    # Run for the entire workspace in one shot. This assumes that the machine has enough memory
    cargo llvm-cov --no-clean nextest --profile ci --workspace --lcov --output-path "lcov.info"
    cargo llvm-cov report --html
    exit 0
fi

PACKAGES=($(cargo metadata --format-version 1 | jq -r '.workspace_members[] | split("/") | .[-1] | split("#")[0]' | sort))
REPORT_DIR="/tmp/llvm-cov-reports"

echo "Workspace packages: ${PACKAGES[*]}"

mkdir -p "$REPORT_DIR"

LCOV_COMMAND_ARGS=""

for PACKAGE in "${PACKAGES[@]}"; do
    echo "Testing package with coverage: $PACKAGE"
    PACKAGE_REPORT_PATH="$REPORT_DIR/$PACKAGE.info"

    # Profile "ci" is configured in .config/nextest.toml
    cargo llvm-cov --no-clean nextest --profile ci -p "$PACKAGE" --lcov --output-path "$PACKAGE_REPORT_PATH"
    echo "Testing completed for package $PACKAGE. Cleaning artifacts"
    cargo llvm-cov clean -p "$PACKAGE"

    if [ -e "$PACKAGE_REPORT_PATH" ]; then # If file exists
        LCOV_COMMAND_ARGS="${LCOV_COMMAND_ARGS} -a $PACKAGE_REPORT_PATH"
    fi
done

if [ -n "$LCOV_COMMAND_ARGS" ]; then
    lcov $LCOV_COMMAND_ARGS --output-file lcov.info
fi
