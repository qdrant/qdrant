# Changelog

## 0.8.0

First release of the Qdrant Edge SDK for Dart & Flutter — on-device vector
search with no server and no network, powered by the shared `qdrant-edge-ffi`
Rust crate (the same crate the Swift and Kotlin SDKs bind) through UniFFI.

- **Full shard API**: load/persist, upsert, search, and query (nearest,
  RRF/DBSF fusion, MMR, formula, order-by, sample), retrieve / scroll / count /
  facet / groups, the filter-condition set, config setters, and snapshot
  manifest.
- **Curated public surface** via `package:qdrant_edge/qdrant_edge.dart` — the
  UniFFI plumbing stays out of the semver contract behind an explicit `show`
  list.
- **Native engine as a Native Asset**: the build hook resolves a per-platform,
  SHA256-pinned prebuilt cdylib (Linux x86_64/arm64, Windows x86_64, macOS
  arm64, iOS arm64 device/simulator, Android arm64/x86_64), downloading it from
  the release when no local build is available — so consumers need no Rust
  toolchain.
- Built `--no-default-features` (no `search_matrix`), matching the Swift and
  Kotlin mobile SDKs.
