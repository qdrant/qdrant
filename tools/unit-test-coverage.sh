#!/bin/bash

# Usage: tools/coverage.sh
#
# For running on low RAM machines like Github CI, Use `RUN_PER_PACKAGE=true tools/coverage.sh`
#
# If using locally, occasionally run `cargo llvm-cov clean` to avoid bloating `target/llvm-cov-target` dir with .profraw files

RUN_PER_PACKAGE=${RUN_PER_PACKAGE:-"false"}
OUTPATH_PATH="unit-test-coverage.lcov"

if [ "$RUN_PER_PACKAGE" == "false" ]; then
    # Run for the entire workspace in one shot. This assumes that the machine has enough memory
    cargo nextest run --no-run --profile ci --workspace --target-dir target/llvm-cov-target --verbose # only build, don't run

    # export LLVM_PROFILE_FILE_NAME="target/llvm-cov-target/qdrant-unit-%m.profraw" # Merge all profraw files into ones
    # RUSTFLAGS="-C instrument-coverage" cargo build --features "service_debug data-consistency_check" --locked --target-dir target/llvm-cov-target
    # Before cargo run tests/nextest will need to build the project with LLVM coverage instrumentation
    # However, surprisingly llvm-cov passes LLVM_PROFILE_FILE_NAME with %p by default and that generates thousands of small files during build phase
    # Best solution is that llvm-cov should avoid passing this env var in the build step (or introduce a dedicated build step without this)
    # But in the meantime, I'm using this workaround of using %m to merge all profraw files into one (includes data from build + test phase)
    cargo llvm-cov --no-clean nextest --profile ci --workspace --verbose
    cargo llvm-cov report --lcov --output-path "$OUTPATH_PATH"
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
    lcov $LCOV_COMMAND_ARGS --output-file "$OUTPATH_PATH"
fi
