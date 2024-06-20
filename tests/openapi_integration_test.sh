#!/usr/bin/env bash

set -ex

# Ensure current path is project root
cd "$(dirname "$0")/../"

poetry -C tests run pytest tests/openapi
