#!/usr/bin/env bash

set -e

# Ensure current path is project root
cd "$(dirname "$0")/../"


docker run --rm \
  -v $(pwd)/docs/grpc:/out \
  -v $(pwd)/src/tonic/proto:/protos \
  pseudomuto/protoc-gen-doc --doc_opt=markdown,docs.md

# <https://github.com/pseudomuto/protoc-gen-doc/issues/383>
sudo chown -R "$USER:$(id -g -n)" $(pwd)/docs/grpc
