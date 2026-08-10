# edge-tool

Create, seed, optimize, and upload minimal local Qdrant edge collections — for benchmarking and
manual testing of the edge write/read/optimize paths without a running Qdrant server.

`create` and `upsert` write through [`edge::EdgeShard`](../../src/edge_shard/mod.rs) directly (the
full read+write local shard, not the read-only or update-only edge variants used by
`edge-shard-query`/`edge-shard-update`), so the resulting directory is a real, self-contained edge
collection: readable by `edge-shard-query`, promotable to object storage with `upload`, or opened
by a real Qdrant edge deployment.

## Subcommands

```sh
cargo run -p edge-tool -- create --dense 1024 --sparse --quantization turbo4 ./collection
cargo run -p edge-tool -- upsert -n 1000 ./collection
cargo run -p edge-tool -- optimize ./collection
cargo run -p edge-tool -- upload --bucket my-bucket ./collection my_collection/0
```

### `create` — build a minimal collection on disk

```sh
edge-tool create [OPTIONS] <PATH>
```

- `--dense <SIZE>` — add a dense vector. A single bare `--dense 1024` creates one vector named
  `dense`; with more than one dense vector, every one must be named: `--dense text:768 --dense
  image:512`. Repeatable.
- `--distance <cosine|euclid|dot|manhattan>` — distance metric for every dense vector (default
  `cosine`).
- `--sparse [NAME]` — add a sparse vector. Bare `--sparse` creates one vector named `sparse`;
  `--sparse NAME` names it. Repeatable for multiple sparse vectors.
- `--quantization <PRESET>` — quantize every dense vector. One of `scalar`, `binary`,
  `product-x4`, `product-x8`, `product-x16`, `product-x32`, `product-x64`, `turbo1`, `turbo1.5`,
  `turbo2`, `turbo4`.
- `--payload-index NAME:TYPE` — create a payload index. `TYPE` is one of `keyword`, `integer`,
  `float`, `text`, `geo`. Comma-separated and/or repeatable, e.g. `--payload-index
  city:keyword,age:integer`.
- `--segments <N>` — target number of segments for the optimizer (omit to derive from CPU count).
- `--on-disk-payload` — store payload on disk (mmap) instead of RAM.

`PATH` must not already contain segment data.

### `upsert` — seed the collection with random points

```sh
edge-tool upsert -n 1000 <PATH>
```

Generates `-n`/`--num` random points shaped to match the collection's *live* schema — every
dense/sparse vector in its config, one random value per payload field currently indexed (read from
`EdgeShard::info().payload_schema`, so it reflects reality even if the shard was not created by
this tool). Point ids are sequential and default to starting at the collection's current
(approximate) point count, so repeated `upsert` calls append rather than overwrite; override with
`--start-id`. `--seed` controls the RNG (default `42`).

### `optimize` — run the shard optimizers

```sh
edge-tool optimize <PATH>
```

Calls `EdgeShard::optimize()` once, which itself loops (merge/indexing/vacuum) until no further
optimization plan is produced. Logs the segment/point counts before and after.

### `upload` — push the collection to object storage

```sh
edge-tool upload [OPTIONS] <SOURCE> <DESTINATION>
```

Recursively uploads every file under the local `SOURCE` directory to `DESTINATION` (a key prefix
inside the bucket, e.g. `my_collection/0`), preserving the relative directory structure — so the
result is byte-for-byte the same layout `edge-shard-query`/`edge-shard-update` expect via
`--prefix`. Every file is streamed through `object_store`'s multipart upload API (even small ones,
as a single final part), so upload memory use stays bounded regardless of segment size.

- `--aws` (default) — AWS S3 or an S3-compatible store (MinIO, RustFS, ...).
- `--gcs` — Google Cloud Storage.
- `--bucket` [`BLOB_BUCKET`] — required.
- `--endpoint` [`S3_ENDPOINT`] — custom S3 endpoint (MinIO/RustFS/LocalStack; omit for real AWS).
- `--region` [`S3_REGION`] — required for real AWS, optional for S3-compatible endpoints.
- `--access-key` [`S3_ACCESS_KEY`] / `--secret-key` [`S3_SECRET_KEY`] — must be given together; if
  both are omitted, the AWS default credential chain is used.
- `--session-token` [`S3_SESSION_TOKEN`], `--s3-express` [`S3_EXPRESS`].
- `--gcs-service-account-path` [`GCS_SERVICE_ACCOUNT_PATH`] / `--gcs-service-account-key`
  [`GCS_SERVICE_ACCOUNT_KEY`] — GCS credentials; the path form takes precedence, ADC is used if
  neither is set.
- `--concurrency <N>` — files uploaded in parallel (default `8`).

## Example — end to end against a local S3-compatible store

Using [`../s3_proxy`](../s3_proxy) to serve a local directory as S3:

```sh
../s3_proxy/s3_proxy.sh up

cargo run -p edge-tool -- create --dense 128 --sparse --quantization turbo4 \
    --payload-index city:keyword,age:integer ./collection
cargo run -p edge-tool -- upsert -n 1000 ./collection
cargo run -p edge-tool -- optimize ./collection
cargo run -p edge-tool -- upload \
    --endpoint http://localhost:9000 --bucket test-bucket --region us-east-1 \
    --access-key test --secret-key test \
    ./collection my_collection/0

# Read it back with no Qdrant server involved:
cargo run -p edge-shard-query -- \
    --backend aws --endpoint http://localhost:9000 --bucket test-bucket --region us-east-1 \
    --access-key test --secret-key test --prefix my_collection/0 \
    scroll --limit 10
```
