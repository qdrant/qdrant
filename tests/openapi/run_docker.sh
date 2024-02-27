#!/bin/sh

set -ex

clear_after_tests()
{
  rm -rf .hypothesis .pytest_cache
}


# This script is supposed to be executed from the docker image

cd "$(dirname "$0")"

trap clear_after_tests EXIT

for i in $(seq 1 100); do
    pytest -s ./openapi_integration/test_snapshot.py
done
