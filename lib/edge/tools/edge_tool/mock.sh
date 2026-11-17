#!/bin/bash

rm -rf edge-data

export ACCESS_KEY=${AWS_ACCESS_KEY_ID:-""}
export SECRET_KEY=${AWS_SECRET_ACCESS_KEY:-""}

cargo run -p edge-tool -- create --dense emb:1024 --sparse=bm25 \
  --quantization turbo4 --on-disk-payload \
  --indexing-threshold-kb 100 \
  --payload-index text:text --payload-index num:integer --payload-index color:keyword \
  ./edge-data

cargo run -p edge-tool -- upsert --num 1000 --start-id 0 ./edge-data

cargo run -p edge-tool -- optimize edge-data/

cargo run -p edge-tool -- upload \
    --clean \
    --aws \
    --bucket qdrant-benchmark-snapshots \
    --endpoint https://storage.googleapis.com \
    --region auto \
    --access-key $ACCESS_KEY \
    --secret-key $SECRET_KEY \
    edge-data serverless/upload-test
