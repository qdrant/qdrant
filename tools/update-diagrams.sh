#!/usr/bin/env bash

# Finds all diagrams in documentation and updates renders
# Use this script if you updated one of the *.mmd files.

# Ensure current path is project root
cd "$(dirname "$0")/../" || exit

find . -name '*.mmd' \
  | xargs -I {} docker run --rm -v $PWD:/data -u $UID ghcr.io/mermaid-js/mermaid-cli/mermaid-cli:latest -i /data/{} -o /data/{}.png

