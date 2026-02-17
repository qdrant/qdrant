# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Qdrant is a vector similarity search engine written in Rust. It provides REST and gRPC APIs for managing collections of vectors with payloads, supporting distributed deployment via Raft consensus.

## Build Commands

```bash
# Development build
cargo build

# Build workspace (with tests)
cargo build --workspace --tests --locked

# Release build (LTO enabled, slow)
cargo build --release

# Format check (requires nightly)
cargo +nightly fmt --all -- --check

# Lint
cargo clippy --workspace --all-targets
```

## Testing

```bash
# Run all tests (primary method, uses nextest)
cargo nextest run --workspace --features rocksdb --profile ci --locked

# Run a specific test by name
cargo nextest run --workspace --features rocksdb --profile ci -E 'test(test_name_here)'

# Run tests in a specific package
cargo nextest run --package segment --features rocksdb --profile ci

# Integration tests (requires built binary)
./tests/integration-tests.sh              # single-peer
./tests/integration-tests.sh distributed  # multi-peer

# Python consensus tests
pytest tests/consensus_tests

# Python e2e tests (requires Docker)
pytest -n 4 tests/e2e_tests -v -m "not longrunning"
```

Key feature flags for testing: `rocksdb`, `service_debug`, `data-consistency-check`, `staging`.

## Architecture

### Layered Crate Structure (top-down)

```
src/          → Binary entry point, REST (Actix) and gRPC (Tonic) handlers, consensus
lib/storage   → TableOfContent (collection registry), Dispatcher, consensus integration
lib/collection→ Collection management, operations, shard orchestration, WAL
lib/shard     → LocalShard, ReplicaSet, shard transfers, peer-to-peer replication
lib/segment   → Vector storage engine, HNSW indexing, search, payload storage, quantization
lib/api       → Protobuf/gRPC definitions, REST↔gRPC type conversions
```

### Key Data Model

**Collection** → split into **Shards** (by shard key) → replicated as **ReplicaSets** across peers → each shard contains **Segments** (immutable append-only storage with vector indices).

### Other Important Crates

- `lib/common/` — shared utilities (CPU/IO budgets, cancellation, memory, feature flags)
- `lib/sparse` — sparse vector support
- `lib/gpu` — GPU acceleration (CUDA/Vulkan)

### Entry Point Flow

`src/main.rs` → parse CLI args → load config (`config/config.yaml`) → init consensus (if cluster mode) → create `TableOfContent` → start REST (port 6333) + gRPC (port 6334) servers.

## Code Conventions

- **Filesystem operations**: Always use `fs_err` instead of `std::fs` or `tokio::fs`. This is enforced by Clippy and will fail CI.
- **Format strings**: Always use inlined format args: `format!("{peer_id}: {version}")` not `format!("{}: {}", peer_id, version)`. Enforced by `clippy::uninlined_format_args`.
- **Formatting**: `cargo +nightly fmt` with module-level import grouping (`imports_granularity = "Module"`, `group_imports = "StdExternalCrate"`).
- **Async runtime**: Tokio. Uses jemalloc allocator on Linux.
- **Rust edition**: 2024, MSRV 1.89.
- **Pre-push hook**: Format check via rusty-hook.

## Git Commits

- Write commit messages like a human: short subject line, no description body, no `Co-Authored-By` tags.
- Example: `Remove unused peers_versions and peers_addresses`

## Configuration

Runtime config is in `config/config.yaml`. Environment variable overrides follow the pattern `QDRANT__SECTION__KEY` (double underscores as separators).

## Ports

- 6333: REST API
- 6334: gRPC API
