#!/bin/bash

# Run pytest in a loop until it fails
counter=0
# export TEST_QDRANT_IMAGE_TAG=v1.12.6
# CMD="pytest -s tests/consensus_tests/test_tenant_promotion.py -x -vv" #  -k updates
# CMD="pytest -s -x -vv -k resharding_abort_live_lock"
CMD="uv --project tests run pytest tests/consensus_tests/test_resharding.py -k test_resharding_transfer"
while $CMD; do
    counter=$((counter + 1))
    echo "==========Test passed $counter times. Running again...========="
    rm -rf consensus_test_logs
done


echo "Test failed. Exiting."
