# API layer (REST, gRPC, `api` crate)

Code navigation for HTTP (Actix-web) and gRPC (Tonic) servers in the main binary and the shared [`api`](../../../lib/api/) library crate.

---

## Overview

Qdrant exposes **REST** (Actix-web) and **gRPC** (Tonic) APIs. The [`api`](../../../lib/api/src/lib.rs) crate defines shared types, validation, and conversions between wire formats and internal engine types. The **main binary** (`src/main.rs` and friends) wires routes and services to [`Dispatcher`](../../../lib/storage/src/dispatcher/mod.rs) and [`TableOfContent`](../../../lib/storage/src/content_manager/toc/mod.rs) (ToC): handlers obtain an `Auth` context, resolve collections through the dispatcher/ToC, and delegate to the collection and segment layers.

---

## API crate (`lib/api/`)

Crate root [`lib.rs`](../../../lib/api/src/lib.rs) declares three top-level modules:

| Module | Role |
|--------|------|
| [`conversions`](../../../lib/api/src/conversions/mod.rs) | Shared conversion helpers split into [`json`](../../../lib/api/src/conversions/json.rs), [`vectors`](../../../lib/api/src/conversions/vectors.rs), and [`inference`](../../../lib/api/src/conversions/inference.rs). Used heavily from gRPC conversion code (e.g. JSON ↔ proto). |
| [`grpc`](../../../lib/api/src/grpc/mod.rs) | Proto-generated types, gRPC validation, transport helpers, and gRPC↔REST/internal conversions. |
| [`rest`](../../../lib/api/src/rest/mod.rs) | REST DTOs (`schema`), API models, validation, and REST-specific conversions. |

Public re-exports: gRPC surface is re-exported from the `grpc` module (`pub use qdrant::*`, descriptor set for reflection, `api_crate_version()`).

### REST types (`rest/schema.rs`)

[`rest/schema.rs`](../../../lib/api/src/rest/schema.rs) holds request/response **DTOs** for the HTTP API: serde `Deserialize`/`Serialize`, [`schemars::JsonSchema`](https://docs.rs/schemars) for OpenAPI/schema generation, and [`validator`](https://docs.rs/validator) where applicable. Types compose [`segment`](../../../lib/segment/) primitives (e.g. `Filter`, `Payload`, vector types) with API-facing enums such as [`Vector`](../../../lib/api/src/rest/schema.rs) and [`VectorStruct`](../../../lib/api/src/rest/schema.rs) (dense, sparse, multi-dense, and inference-oriented variants). Companion modules under `rest/`: [`models`](../../../lib/api/src/rest/models.rs), [`validate`](../../../lib/api/src/rest/validate.rs).

### gRPC types (`grpc/`)

- **[`qdrant`](../../../lib/api/src/grpc/qdrant.rs)** (generated): messages and `tonic` service traits for Collections, Points, Snapshots, Qdrant, internal services, etc.
- **[`grpc_health_v1`](../../../lib/api/src/grpc/grpc.health.v1.rs)**: standard gRPC health protocol.
- **[`conversions.rs`](../../../lib/api/src/grpc/conversions.rs)**: large mapping layer between proto types, REST schema types, and `segment`/`collection` internals (including filters, indexes, search/update payloads, errors as [`tonic::Status`](https://docs.rs/tonic)).
- Support modules: [`validate`](../../../lib/api/src/grpc/validate.rs), [`ops`](../../../lib/api/src/grpc/ops.rs), channel pools, etc.

### Conversions

- **[`rest/conversions.rs`](../../../lib/api/src/rest/conversions.rs)**: REST ↔ internal vector and scoring types (e.g. `VectorInternal` / `VectorStructInternal` ↔ `VectorOutput` / `VectorStructOutput`), inference usage models ↔ gRPC, and similar bounded transforms used by HTTP handlers.
- **[`grpc/conversions.rs`](../../../lib/api/src/grpc/conversions.rs)**: Proto ↔ REST (`rest::schema`) and ↔ engine types; uses [`crate::conversions::json`](../../../lib/api/src/conversions/json.rs) for structured JSON-like fields in protobuf.
- **[`conversions/`](../../../lib/api/src/conversions/mod.rs)**: Shared building blocks so REST and gRPC paths do not duplicate JSON/vector/inference logic.

---

## REST server (`src/actix/`)

[`src/actix/mod.rs`](../../../src/actix/mod.rs) builds the Actix [`App`](https://docs.rs/actix-web): shared [`web::Data`](https://docs.rs/actix-web) includes `Dispatcher`, telemetry, HTTP client, debugger state, health checker, service settings, and audit config.

**Middleware order** (last `wrap` runs first): compression → optional **API key** [`AuthTransform`](../../../src/actix/auth.rs) with a whitelist for `/`, `/healthz`, `/readyz`, `/livez`, and Web UI paths → `NormalizePath::trim` → optional **CORS** → **Logger** (excluding hot paths: `/`, `/metrics`, `/telemetry`, health URLs) → **Actix telemetry** transform.

**Validation**: `actix-web-validator` configs for path, query, and JSON (JSON body size capped by `max_request_size_mb`).

**Route configuration** (each `config_*` lives under [`src/actix/api/`](../../../src/actix/api/)): collections, snapshots, updates, cluster, service, search, recommend, discover, query, facet, shards, issues, debugger, profiler, local shard, audit; plus scroll, count, get point(s). Optional static **Web UI** service. TLS uses Rustls when enabled ([`certificate_helpers`](../../../src/actix/certificate_helpers.rs)).

---

## gRPC server (`src/tonic/`)

[`src/tonic/mod.rs`](../../../src/tonic/mod.rs) registers public services on the configured gRPC port:

- **[`QdrantService`](../../../src/tonic/mod.rs)**: `health_check` returning [`VersionInfo`](../../../lib/api/src/rest/models.rs) as proto.
- **[`HealthService`](../../../src/tonic/mod.rs)**: `grpc.health.v1` serving status.
- **[`CollectionsService`](../../../src/tonic/api/collections_api.rs)**, **[`PointsService`](../../../src/tonic/api/points_api.rs)** (wrapped for telemetry), **[`SnapshotsService`](../../../src/tonic/api/snapshots_api.rs)** (telemetry wrapper): user-facing RPCs backed by `Dispatcher`.
- **Reflection** via embedded [`QDRANT_DESCRIPTOR_SET`](../../../lib/api/src/grpc/mod.rs) with an explicit allowlist of service names.

**Stack**: optional TLS → **Tower** layers — [`LoggingMiddlewareLayer`](../../../src/tonic/logging.rs), [`TonicTelemetryLayer`](../../../src/tonic/tonic_telemetry.rs), optional [`AuthLayer`](../../../src/tonic/auth.rs) when API keys are configured. Responses use gzip compression and large decode limits where configured.

**Internal / P2P** — [`init_internal`](../../../src/tonic/mod.rs) binds a separate listener with [`PointsInternalService`](../../../src/tonic/api/points_internal_api.rs), [`CollectionsInternalService`](../../../src/tonic/api/collections_internal_api.rs), [`QdrantInternalService`](../../../src/tonic/api/qdrant_internal_api.rs), [`ShardSnapshotsService`](../../../src/tonic/api/snapshots_api.rs), and [`RaftService`](../../../src/tonic/api/raft_api.rs) (consensus), using `TableOfContent` and consensus state directly. No API-key layer on this stack; TLS optional for intra-cluster.

---

## Request flow

1. **REST**: Actix handler parses/validates JSON → builds internal request types (often via `api::rest` and `collection::operations`) → obtains [`Auth`](../../../lib/storage/src/rbac/auth.rs) from request extensions / middleware → calls **Dispatcher** / **ToC** methods (e.g. collection updates, search) with `CollectionPass` where applicable.
2. **gRPC**: Tonic decodes proto → service impl converts via `api::grpc::conversions` → same **Dispatcher** / **ToC** path with `Auth`.
3. **Dispatcher/ToC** resolves the collection, shard routing, and replication; **Collection** runs the operation across **segments** (local shard) or remote shards (see [`collection`](./collection.md), [`segment`](./segment.md)).

---

## Cross-References

| Topic | Where |
|--------|--------|
| RBAC and audit on API paths | [`rbac-and-auth.md`](./rbac-and-auth.md), [`lib/storage/src/rbac/`](../../../lib/storage/src/rbac/mod.rs) |
| Dispatcher / ToC | [`lib/storage/src/dispatcher/`](../../../lib/storage/src/dispatcher/mod.rs), [`content_manager/toc/`](../../../lib/storage/src/content_manager/toc/mod.rs) |
| Collection orchestration | [`collection.md`](./collection.md) |
| Proto sources | [`lib/api/src/grpc/proto/`](../../../lib/api/src/grpc/proto/qdrant.proto) (`.proto` files; build generates `qdrant.rs`, etc.) |
| Settings (ports, TLS, limits) | [`src/settings.rs`](../../../src/settings.rs) |
