#!/usr/bin/env bash


DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"


docker run --network=host --rm -i loadimpact/k6 run - <"$DIR/collection_stress.js"


