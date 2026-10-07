#!/bin/bash

DATA_DIR=./edge-data-repro

rm -rf $DATA_DIR

cargo run -p edge-tool -- create --dense 4 --indexing-threshold-kb 1 $DATA_DIR
cargo run -p edge-tool -- upsert --num 1000 $DATA_DIR
cargo run -p edge-tool -- optimize $DATA_DIR

cargo run -p edge-tool -- upload \
    --clean \
    --aws \
    --bucket test-bucket \
    --endpoint http://localhost:9000 \
    --region   us-east-1 \
    --access-key  rustfsadmin \
    --secret-key  rustfsadmin \
    $DATA_DIR serverless/upsert-bug-repro
