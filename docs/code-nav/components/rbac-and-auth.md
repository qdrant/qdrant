# RBAC and audit (`lib/storage`)

Code navigation for **role-style access** at collection granularity, the [`Auth`](../../../lib/storage/src/rbac/auth.rs) wrapper that pairs checks with **audit logging**, and the **structured JSON audit** pipeline in [`audit.rs`](../../../lib/storage/src/audit.rs).

---

## Overview

**RBAC** in Qdrant controls what a caller may do, primarily **per collection** (with a global read/manage mode). The [`Auth`](../../../lib/storage/src/rbac/auth.rs) struct carries the resolved [`Access`](../../../lib/storage/src/rbac/mod.rs), identity metadata (`subject`, `remote`), and [`AuthType`](../../../lib/storage/src/rbac/mod.rs). Methods such as [`check_global_access`](../../../lib/storage/src/rbac/auth.rs), [`check_collection_access`](../../../lib/storage/src/rbac/auth.rs), [`check_point_op`](../../../lib/storage/src/rbac/ops_checks.rs), and [`check_collection_meta_operation`](../../../lib/storage/src/rbac/ops_checks.rs) delegate to the inner `Access` and, when audit is enabled, emit one **JSON line** per check via [`audit_log`](../../../lib/storage/src/audit.rs). **Internal** cluster traffic uses `AuthType::Internal` and is **not** audit-logged.

---

## Location

| Path | Role |
|------|------|
| [`lib/storage/src/rbac/mod.rs`](../../../lib/storage/src/rbac/mod.rs) | `Access`, `AuthType`, modes, `AccessRequirements`, `CollectionPass`, validation |
| [`lib/storage/src/rbac/auth.rs`](../../../lib/storage/src/rbac/auth.rs) | `Auth` struct and audited check helpers |
| [`lib/storage/src/rbac/ops_checks.rs`](../../../lib/storage/src/rbac/ops_checks.rs) | `Access::check_point_op`, `Access::check_collection_meta_operation`, `Auth` wrappers, `CheckableCollectionOperation` |
| [`lib/storage/src/rbac/auditable_operation.rs`](../../../lib/storage/src/rbac/auditable_operation.rs) | Auditable operation naming/metadata used with meta-ops |
| [`lib/storage/src/audit.rs`](../../../lib/storage/src/audit.rs) | `AuditConfig`, `AuditEvent`, rolling file logger |

---

## Access model

**[`Access`](../../../lib/storage/src/rbac/mod.rs)** (serde-friendly, untagged enum):

- **`Global(GlobalAccessMode)`** — applies to all collections: **`r`** ([`Read`](../../../lib/storage/src/rbac/mod.rs)) or **`m`** ([`Manage`](../../../lib/storage/src/rbac/mod.rs)).
- **`Collection(CollectionAccessList)`** — list of [`CollectionAccess`](../../../lib/storage/src/rbac/mod.rs) entries, each naming one **`collection`** and a [`CollectionAccessMode`](../../../lib/storage/src/rbac/mod.rs):
  - **`r`** — read-only.
  - **`rw`** — read/write including “extras” (e.g. snapshots, payload indexes) when requirements ask for `extras`.
  - **`prw`** ([`PointsReadWrite`](../../../lib/storage/src/rbac/mod.rs)) — point read/write only; operations requiring **extras** or **manage** are forbidden for that collection.

**[`AccessRequirements`](../../../lib/storage/src/rbac/mod.rs)** (builder-style flags):

- **`write`** — mutating data operations.
- **`manage`** — cluster/collection-admin style operations (implies strong global or denied for restricted collection modes).
- **`extras`** — non-point features such as snapshots and payload index changes (see checks in [`CollectionAccessView::meets_requirements`](../../../lib/storage/src/rbac/mod.rs)).

Successful checks return **`CollectionPass`** (or **`CollectionMultipass`** for global-only flows) so downstream code holds a typed proof of access to a collection name.

**[`check_collection_meta_operation`](../../../lib/storage/src/rbac/ops_checks.rs)** on `Access` maps [`CollectionMetaOperations`](../../../lib/storage/src/content_manager/collection_meta_ops.rs) to requirements: create/update/delete collection, aliases, resharding, shard transfer/replica state, shard keys → **global manage**; create/drop payload index → **collection write + extras**; nop → no check.

---

## Auth struct

[`Auth`](../../../lib/storage/src/rbac/auth.rs) fields: **`access`**, optional **`subject`** (e.g. JWT subject), optional **`remote`** (client IP string), **`auth_type`** ([`Jwt`](../../../lib/storage/src/rbac/mod.rs) | `ApiKey` | `None` | `Internal`), optional **`tracing_id`**.

- **`new` / `new_internal`** — construct for requests or for service-internal full access (`Internal` skips audit).
- **`unlogged_access`** — borrows `&Access` **without** audit (discouraged for general use).
- **`access(method)`** — borrows `&Access` and emits an audit entry with result **ok** (see implementation).
- **`check_global_access`**, **`check_collection_access`** — call the underlying `Access` methods and **`emit_audit`** with the method name and optional collection.

**Point and meta operations** ([`ops_checks.rs`](../../../lib/storage/src/rbac/ops_checks.rs)):

- **`Auth::check_point_op`** — runs `Access::check_point_op` via `unlogged_access`, then **`emit_audit(method, Some(collection), &result)`**. Used from ToC/point operation paths (search, scroll, recommend, etc.).
- **`Auth::check_collection_meta_operation`** — wraps `Access::check_collection_meta_operation`; audit uses **`operation.operation_name()`** and **no** collection field in the event (collection name is not passed as a separate argument to `emit_audit` here).

---

## Audit logging

**[`AuditConfig`](../../../lib/storage/src/audit.rs)** (serde): **`enabled`**, **`dir`** (default `./storage/audit`), **`rotation`** — **`daily`** or **`hourly`**, **`max_log_files`** (default **7**, older rotated files removed), **`trust_forwarded_headers`** — when true, client IP for audit may follow `X-Forwarded-For` (only safe behind a trusted proxy).

**[`init_audit_logger`](../../../lib/storage/src/audit.rs)** installs a global singleton; returns a **`WorkerGuard`** that must stay alive for the process lifetime (non-blocking writer shutdown). **`audit_log`** is a no-op if disabled. **`is_audit_enabled`** / **`audit_trust_forwarded_headers`** read global state.

**[`AuditEvent`](../../../lib/storage/src/audit.rs)** (JSON one line per event): **`timestamp`** (UTC ISO-8601), **`method`** (handler/check name), **`auth_type`**, optional **`subject`**, **`remote`**, **`collection`** (for collection-scoped checks), **`tracing_id`**, **`result`** (`ok` | `denied`), optional **`error`** message on denial.

**Rolling files**: [`tracing_appender::rolling::RollingFileAppender`](https://docs.rs/tracing-appender) with prefix **`audit`**, suffix **`log`**, rotation per config; writes go through a **non-blocking** writer with per-event JSON serialization and newline for atomic line-oriented logs.

**Tracing ID extraction** (for populating `Auth`): [`extract_tracing_id`](../../../lib/storage/src/audit.rs) checks headers in order **`x-request-id`**, **`x-tracing-id`**, **`traceparent`**, truncating to [`MAX_TRACING_ID_LEN`](../../../lib/storage/src/audit.rs) (256).

---

## Cross-References

| Topic | Where |
|--------|--------|
| REST API key / JWT wiring | [`src/actix/auth.rs`](../../../src/actix/auth.rs), [`src/common/auth.rs`](../../../src/common/auth.rs) |
| gRPC auth layer | [`src/tonic/auth.rs`](../../../src/tonic/auth.rs) |
| Dispatcher / ToC using `Auth` | [`lib/storage/src/dispatcher/`](../../../lib/storage/src/dispatcher/mod.rs), [`content_manager/toc/`](../../../lib/storage/src/content_manager/toc/mod.rs) (e.g. [`point_ops.rs`](../../../lib/storage/src/content_manager/toc/point_ops.rs)) |
| HTTP API layer overview | [`api-layer.md`](./api-layer.md) |
| Storage errors (e.g. forbidden) | [`lib/storage/src/content_manager/errors.rs`](../../../lib/storage/src/content_manager/errors.rs) |
