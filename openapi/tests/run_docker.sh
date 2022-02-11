#!/bin/sh

set -ex

# This script is supposed to be executed from the docker image

cd "$(dirname "$0")"

pytest

rm -rf .hypothesis .pytest_cache
