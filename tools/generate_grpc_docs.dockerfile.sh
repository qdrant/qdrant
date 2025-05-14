#! /bin/bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

project_dir="$script_dir/.."

docker build -f "$script_dir/generate_grpc_docs.dockerfile" -t grpc-doc-gen "$project_dir"
docker run --rm -v "$project_dir/docs/grpc":/out grpc-doc-gen
