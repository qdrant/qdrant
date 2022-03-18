#!/usr/bin/env bash


DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"


docker run --network=host --rm -i loadimpact/k6 run -u 10 -i 500000 --duration 30m - <"$DIR/collection_stress.js"


