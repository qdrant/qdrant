#!/bin/sh

set -ex

clear_after_tests()
{
  rm -rf .hypothesis .pytest_cache
}


# This script is supposed to be executed from the docker image

cd "$(dirname "$0")"

trap clear_after_tests EXIT

pytest openapi_integration/test_snapshot.py -s
pytest openapi_integration/test_snapshot.py -s
pytest openapi_integration/test_snapshot.py -s
pytest openapi_integration/test_snapshot.py -s
pytest openapi_integration/test_snapshot.py -s
pytest openapi_integration/test_snapshot.py -s
pytest openapi_integration/test_snapshot.py -s
