#!/usr/bin/env bash


DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"


docker run \
  -e QDRANT_HOST=host.docker.internal:6334 \
  --add-host host.docker.internal:host-gateway \
  -v "${PWD}"/lib/api/src/grpc/proto:/proto \
  -v "${PWD}"/benches/service:/code \
  --rm \
  -i loadimpact/k6 \
  run - <"$DIR/grpc-search.js"


