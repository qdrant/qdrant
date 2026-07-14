---
name: edge-shard-query
description: Use the `edge-shard-query` CLI to run scroll / dense search / sparse search against a read-only Qdrant edge shard, reading segments directly from S3, GCS, or a live Qdrant peer over gRPC — no Qdrant server required. Also covers the live-reload mode for watching a leader's writes propagate. Use when asked to query, inspect, debug, or live-watch an edge shard on object storage.
---

# edge-shard-query

An experimental binary that opens a `ReadOnlyEdgeShard` **directly over object storage** (S3/S3-compatible or GCS) — or over a running Qdrant peer's `StorageRead` gRPC service — and runs a single read request against it.

There is no Qdrant server in the loop for the object-storage backends. The tool reads segment files itself, through a local disk cache, so it is the fastest way to answer "what does the data in this bucket actually contain?" and to watch a leader's writes land.

## Running it

```sh
cargo run -p edge-shard-query -- <CONNECTION FLAGS> <SUBCOMMAND> <SUBCOMMAND FLAGS>
```

Connection flags must come **before** the subcommand; request flags come after it. For repeated use, build once (`cargo build -p edge-shard-query --release`) and call `target/release/edge-shard-query` directly.

Logging is on at `info` by default; raise it with `RUST_LOG=debug`.

## Choosing a backend

`--backend` selects where segments are read from. It defaults to `aws`.

| Backend | What it reads | Shard addressed by |
| --- | --- | --- |
| `aws` | AWS S3 or any S3-compatible store (MinIO, RustFS, LocalStack) | `--bucket` + `--prefix` |
| `gcs` | Google Cloud Storage | `--bucket` + `--prefix` |
| `uio-grpc` | A running Qdrant peer's `StorageRead` gRPC service | `--collection` + `--shard-id` |

### `--prefix` (object storage only)

The key prefix inside the bucket that points at the **edge-shard root** — the directory containing `edge_config.json` and `segments/`. Empty means the bucket root. A shard for collection `foo`, shard `0` typically lives at `foo/0`.

An `edge_config.json` is *not* required: segments are discovered from the leader's segment manifest, and the shard config is derived from the segments themselves. Leave `--prefix` empty for `uio-grpc`, which addresses the shard by collection and shard id instead.

### Connection flags

All of these accept an environment variable as a fallback, shown in brackets.

**AWS / S3-compatible**
- `--bucket` [`BLOB_BUCKET`] — bucket name, no scheme prefix. Required.
- `--endpoint` [`S3_ENDPOINT`] — custom S3 endpoint. Set for MinIO/RustFS/LocalStack; **omit for real AWS**.
- `--region` [`S3_REGION`] — e.g. `us-east-1`. Required for real AWS, optional for S3-compatible endpoints.
- `--access-key` [`S3_ACCESS_KEY`] / `--secret-key` [`S3_SECRET_KEY`] — must be given **together**. If both are omitted, the AWS default credential chain is used.
- `--session-token` [`S3_SESSION_TOKEN`] — for short-lived credentials.
- `--s3-express` [`S3_EXPRESS`] — for S3 Express One Zone directory buckets (named `*--x-s3`).

**GCS**
- `--bucket` [`BLOB_BUCKET`] — required.
- `--gcs-service-account-path` [`GCS_SERVICE_ACCOUNT_PATH`] — path to a service-account JSON key file.
- `--gcs-service-account-key` [`GCS_SERVICE_ACCOUNT_KEY`] — the key contents inline. The *path* form takes precedence if both are set; if neither is set, application default credentials (ADC) are used.

**uio-grpc**
- `--endpoint` — the peer's public gRPC URL, e.g. `http://localhost:6334`. Required.
- `--collection` [`QDRANT_COLLECTION`] — required.
- `--shard-id` — defaults to `0`.
- `--api-key` [`QDRANT_API_KEY`] — omit for an unauthenticated peer.

> The peer must run with `QDRANT__FEATURE_FLAGS__WRITE_SEGMENT_MANIFEST=true`, otherwise the shard has no segment manifest and discovery fails.

### Tuning flags (all backends)

- `--cache-dir` — local mirror directory for the segment disk cache. Each remote block is fetched once and served locally afterwards. Defaults to a stable subdirectory of the system temp dir, so **the cache persists across runs** — delete it to force a cold read.
- `--search-threads` — size of the shard's search thread pool (used to read segments in parallel at open, and to run searches). `0` derives it from the CPU count.
- `--no-load-profile` — by default the shard is opened *for the specific request*, warming only the segment components that request will touch and leaving the rest cold. This flag disables that and warms everything per the persisted segment configs, like a long-lived deployment would. Use it when benchmarking steady-state behaviour rather than cold start.

## Subcommands

### `scroll` — paginate over points

- `--offset <ID>` — start id, to resume a previous page. Accepts an integer or a UUID string.
- `--order-by <FIELD>` — order by a payload field instead of by id.

Prints the records plus a `next_page_offset` line to feed back into `--offset`.

### `search` — dense nearest-neighbour search

- `--vector` — the query vector. Accepts a JSON array (`[0.1, 0.2, ...]`), a comma-separated list (`0.1,0.2,...`), or `@path` / `@-` to read either form from a file or stdin. **If omitted, a random vector is used**, with its dimension read from the shard config once the shard is open — handy for a quick smoke test.
- `--using <NAME>` — which named vector to search. Omit for the default/unnamed vector.
- `--offset <N>` — results to skip before collecting `--limit`. Default `0`.
- `--score-threshold <F>` — only return results scoring at least this.
- `--hnsw-ef <N>` — HNSW beam width. Larger is more accurate and slower.
- `--exact` — search exhaustively, bypassing HNSW. Slow but exact; useful to check recall.

### `search-sparse` — sparse nearest-neighbour search

- `--vector` — **required**. Accepts a JSON object (`{"indices": [12, 700], "values": [0.4, 0.9]}`), a comma-separated list of `index:value` pairs (`12:0.4,700:0.9`), or `@path` / `@-`. Indices need not be sorted.
- `--using <NAME>` — sparse vectors are named, so this is **usually required**. Omit only if the shard stores its sparse vector under the default (empty) name.
- `--offset`, `--score-threshold`, `--exact` — as for `search` (`--exact` bypasses the sparse index).

### Filtering and output (every subcommand)

- `--filter <JSON>` — an arbitrary payload filter in Qdrant's filter DSL (the `filter` field of a REST request). Curl `--data` style: a literal JSON string, `@path` to read from a file, or `@-` to read from stdin. Any condition the DSL can express works here, so there are no per-condition flags.
- `--filter-key` / `--filter-value` — shortcut for the common "field equals value" case. Must be given together, and are mutually exclusive with `--filter`. The value is parsed as an integer or boolean when it looks like one, otherwise as a string.
- `--limit <N>` — max points to return. Default `10`.
- `--with-vectors` — include vectors in the output.

Results print as one JSON object per line (`id`, `payload`, `vector`, plus `score` and `version` for searches).

## Live reload — watching a leader's writes

Both flags keep the tool running after the first answer. Each iteration refreshes the shard from the backend, re-runs the **same** request, and prints a diff against the previous results:

```
+ {...}   point appeared
- {...}   point disappeared
~ {old} -> {new}   same id, changed content
```

A pure reordering of unchanged rows prints nothing (`no changes`). This is the fastest way to watch a leader writing into the same bucket, and to catch staleness or consistency bugs in the follower read path.

- `--live-reload <SECONDS>` — refresh on a timer (minimum 1).
- `--live-reload-key` — refresh when you press Enter instead. Better when stepping through a debugger on the leader. Not compatible with `@-` (stdin) request arguments, since it reads stdin itself.

The two are mutually exclusive. A failed refresh is logged and retried on the next trigger — the shard keeps serving its previous state rather than dying.

## Examples

Scroll with a JSON filter, against a local MinIO/RustFS:

```sh
cargo run -p edge-shard-query -- \
    --backend  aws \
    --endpoint http://localhost:9000 \
    --bucket   test-bucket \
    --region   us-east-1 \
    --access-key rustfsadmin \
    --secret-key rustfsadmin \
    --prefix   collection/0 \
    scroll \
    --filter '{"must":[{"key":"city","match":{"value":"London"}}]}' \
    --limit  20
```

Dense search on GCS, query vector from a file:

```sh
cargo run -p edge-shard-query -- \
    --backend gcs \
    --bucket  my-bucket \
    --gcs-service-account-path /path/to/key.json \
    --prefix  collection/0 \
    search \
    --vector @query.json \
    --limit  5
```

Sparse search (note `--using`, since sparse vectors are named):

```sh
cargo run -p edge-shard-query -- \
    --backend  aws \
    --endpoint http://localhost:9000 \
    --bucket   test-bucket \
    --prefix   collection/0 \
    search-sparse \
    --using  text \
    --vector '{"indices": [12, 700, 5301], "values": [0.4, 0.9, 0.2]}' \
    --limit  5
```

Read straight from a running Qdrant peer — no object storage at all:

```sh
cargo run -p edge-shard-query -- \
    --backend    uio-grpc \
    --endpoint   http://localhost:6334 \
    --collection my_collection \
    --shard-id   0 \
    scroll \
    --limit 10
```

Watch a leader's writes propagate, re-running a filtered search every 2 seconds:

```sh
cargo run -p edge-shard-query -- \
    --backend  aws \
    --endpoint http://localhost:9000 \
    --bucket   test-bucket \
    --prefix   collection/0 \
    --live-reload 2 \
    search \
    --filter-key city --filter-value London \
    --limit 20
```

## Troubleshooting

- **Nothing found / shard opens with 0 segments.** Check `--prefix` actually points at the shard root (the level holding `segments/`), and that the leader wrote a segment manifest. For `uio-grpc`, the peer needs `QDRANT__FEATURE_FLAGS__WRITE_SEGMENT_MANIFEST=true`.
- **Stale results.** The disk cache persists across runs by default. Delete `--cache-dir` (or the default temp subdirectory) to force a cold fetch.
- **`--vector` rejected on `search-sparse`.** Sparse takes `{"indices": [...], "values": [...]}` or `12:0.4,700:0.9` — not a plain float array.
- **Vector name not in shard config.** `--using` must name a vector that exists in the shard; with no `--using`, the default/unnamed vector must exist.
- **Credentials.** `--access-key` and `--secret-key` must be supplied together, or both omitted to fall back to the AWS default credential chain.
