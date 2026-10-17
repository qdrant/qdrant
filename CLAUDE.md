# Qdrant

Vector search engine written in Rust. Cargo workspace: the server binary is in `src/`,
the library crates are in `lib/*`. User docs: https://qdrant.tech/documentation/.
REST API spec: `docs/redoc/master/openapi.json`. gRPC API: `lib/api/src/grpc/proto/`.

## Code map

A request goes through the layers top to bottom:

- `src/actix/api/` (REST), `src/tonic/api/` (gRPC): handlers, auth, request conversion.
  `src/settings.rs` reads the config (defaults in `config/config.yaml`), and
  `src/consensus.rs` runs the Raft loop.
- `lib/storage`: cluster-wide state. `dispatcher.rs` sends metadata operations through
  consensus. `content_manager/toc/` (TableOfContent) owns all collections.
  `content_manager/consensus/` is Raft. `content_manager/snapshots/` handles
  collection snapshots. `rbac/` is JWT access control.
- `lib/collection`: one collection. `operations/` has the request and operation types.
  `hash_ring.rs` routes points to shards, `shards/shard_holder/` holds the shards,
  `shards/replica_set/` handles replication, and `shards/transfer/` plus `wal_delta.rs`
  handle shard transfers. `update_handler.rs` and `update_workers/` run the WAL and
  optimizers. Built on top of search: `grouping/`, `lookup/`, `recommendations.rs`, `discovery.rs`.
- `lib/shard`: one local shard. `segment_holder/` holds its segments. `optimizers/`
  has merge, indexing, vacuum, and config-mismatch. `proxy_segment/` takes writes while a
  segment is being optimized. `query/` runs universal queries. `wal.rs` is the shard WAL.
- `lib/segment`: one segment, the unit of storage and indexing. `types.rs` has the core
  config and filter types. `entry/` has the segment traits (`SegmentEntry` and its supertraits). `segment_constructor/`
  builds segments, and `spaces/` has the distance metrics with SIMD. Also
  `payload_storage/`, `json_path/`, `index/sparse_index/`, `index/plain_vector_index/`,
  and `index/query_optimization/` (filter planning).
- Supporting crates: `sparse`, `bm25`, `posting_list`, `wal`, `gpu` (GPU HNSW build),
  `api` (REST/gRPC types).
- `lib/edge` (Qdrant Edge): an embeddable single shard built on `lib/shard` and
  `lib/segment`, with no server or consensus. The public API is whatever `lib.rs`
  re-exports; everything else is internal. `config/` has `EdgeConfig`. Shard variants for
  serverless setups are behind the `serverless` feature. The published crate
  `qdrant-edge` is assembled from these sources by `publish/amalgamate.py`.

## Build, lint, test

- Format: `cargo +nightly fmt --all` (needs nightly).
- Lint: `cargo clippy --workspace --all-targets -- -D warnings`. When editing code behind
  `#[cfg(feature = ...)]`, add that feature, e.g. `--features staging`. Avoid
  `--all-features` locally: it builds the GPU backend, which is slow to compile.
- Avoid `--release` locally: fat LTO makes it very slow to build. For benchmarks and
  profiling use `--profile perf`, which is release without LTO.
- CI also enforces ast-grep rules: `sg scan --config tools/ast-grep/sgconfig.yml`.
- Rust tests: `cargo test -p <crate>` is much faster than `--workspace`.
- Python tests need `uv`, and most need a running server:
  `uv --project tests run pytest tests/openapi`, `tests/consensus_tests` (spawns its own cluster).
- More detail (Docker, profiling, benchmarks): `docs/DEVELOPMENT.md`.

## API changes

REST needs all four steps, and CI checks that they agree:
1. Endpoints and models in Rust.
2. Specs in `openapi/*ytt.yaml`.
3. Schema definitions in `src/schema_generator.rs`.
4. Run `./tools/generate_openapi_models.sh`. Never edit `openapi.json` by hand.

gRPC: edit `lib/api/src/grpc/proto/*.proto`, then `cargo build -p api` regenerates the code.

Qdrant Edge has its own API. If it exposes the feature you're changing, update all three
interfaces: Rust (`lib/edge/src`), Python (`lib/edge/python`), and FFI (`lib/edge/ffi`).
Update the examples too: `lib/edge/publish/examples` (Rust) and `lib/edge/python/examples`.
`lib/edge/publish/qdrant-edge` is generated; don't edit it.

A new endpoint must also be added in the same PR to: `EXPECTED_NUMBER_OF_APIS` in
`tests/openapi_consistency_check.sh`, the `ACTION_ACCESS` table in
`tests/consensus_tests/auth_tests/test_jwt_access.py`, and `REST_ENDPOINT_WHITELIST` /
`GRPC_ENDPOINT_WHITELIST` in `src/common/metrics/requests.rs`.

## Contributing

- PRs target `dev`. `master` is release-only.
- AI-assisted PRs follow `docs/CONTRIBUTING.md#contributing-with-ai`. Mark each commit
  `[AI]` or `[manual]` in the PR description, and include the original prompt. Don't
  generate the PR description or replies to review comments, and don't add
  AI-generated walkthroughs or diagrams.

## Code conventions

Full rules with examples: `.github/review-rules.md`. Tests and benchmarks are exempt.

- Program defensively, so the compiler catches broken assumptions:
  - Don't fill structs with `..Default::default()`. Set every field so new fields don't get defaults silently.
  - If a conversion can fail, implement `TryFrom`, not a `From` that falls back to a default.
  - Use enums instead of `bool` parameters.
  - Mark results that must not be ignored with `#[must_use]`.
  - Prefer `SomeType::from(x)` over `x.into()`.
  - Match enums exhaustively instead of using a `_` arm.
  - Ignore struct fields explicitly (`field: _`) instead of using `..`.
- Don't use the `transmute_*` helpers in new code; use `bytemuck` or `zerocopy`.
