#!/bin/bash

WORKSPACE_CRATES=($(cargo metadata --format-version 1 | jq -r '.workspace_members[] | split("/") | .[-1] | split("#")[0]' | sort))
EXPECTED_WORKSPACE_CRATES=("api" "blob_store" "cancel" "collection" "common" "dataset" "gpu" "io" "issues" "memory" "qdrant" "quantization" "segment" "sparse" "storage")

if [ "${WORKSPACE_CRATES[*]}" != "${EXPECTED_WORKSPACE_CRATES[*]}" ]; then
    echo "Workspace crates have changed. Please update coverage.sh to update the expected and whitelisted ones."
    exit 1
fi

WHITELISTED_WORKSPACE_CRATES=("api" "common" "segment" "blob_store")
REPORT_DIR="target/llvm-cov/crate-reports"

echo "All workspace crates: ${WORKSPACE_CRATES[*]}"
echo "Whitelisted workspace crates: ${WHITELISTED_WORKSPACE_CRATES[*]}"

mkdir -p "$REPORT_DIR"

LCOV_COMMAND_ARGS=""

for CRATE in "${WHITELISTED_WORKSPACE_CRATES[@]}"; do
    if [ "$CRATE" == "qdrant" ]; then
        continue
    fi

    echo "Testing crate with coverage: $CRATE"
    # Profile "ci" is configured in .config/nextest.toml
    cargo llvm-cov --no-clean nextest --profile ci -p "$CRATE" --lcov --output-path "$REPORT_DIR/$CRATE.info"

    LCOV_COMMAND_ARGS="${LCOV_COMMAND_ARGS} -a $REPORT_DIR/$CRATE.info"
done

if [ -n "$LCOV_COMMAND_ARGS" ]; then
    lcov $LCOV_COMMAND_ARGS --output-file lcov.info
fi
