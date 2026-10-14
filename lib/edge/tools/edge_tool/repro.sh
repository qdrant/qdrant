#!/bin/bash

DATA_DIR=./edge-data-repro

rm -rf $DATA_DIR

export ACCESS_KEY=${AWS_ACCESS_KEY_ID:-""}
export SECRET_KEY=${AWS_SECRET_ACCESS_KEY:-""}

cargo run -p edge-tool -- create --dense 4 --indexing-threshold-kb 1 $DATA_DIR
cargo run -p edge-tool -- upsert --num 1000 $DATA_DIR
cargo run -p edge-tool -- optimize $DATA_DIR

cargo run -p edge-tool -- upload \
    --clean \
    --aws \
    --bucket qdrant-benchmark-snapshots \
    --endpoint https://storage.googleapis.com \
    --region auto \
    --access-key $ACCESS_KEY \
    --secret-key $SECRET_KEY \
    $DATA_DIR serverless/upsert-bug-repro
