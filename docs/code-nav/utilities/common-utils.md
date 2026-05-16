# Common utilities across Qdrant

## Overview

Shared utilities span the **`common`** workspace crate (`lib/common/common/`) and **segment-local** helpers under `lib/segment/src/common/` and `lib/segment/src/utils/`. Together they provide:

- **Filesystem**: atomic writes, safe moves, sync helpers, validation.
- **Mmap**: typed slices, bit slices, open/create helpers, advice, flushers.
- **Memory**: cgroup-aware limits for scheduling (segment `Mem`).
- **Errors**: `OperationError` / `OperationResult` as the primary segment operation error surface.
- **Misc**: on-disk JSON wrappers, bit packing, counters, I/O abstractions, score fusion, anonymization for logging, and small algorithmic helpers.

This document maps the main entry points; it is not an exhaustive API listing.

## Location

| Layer | Path | Role |
|-------|------|------|
| Shared crate | `lib/common/common/src/` | Reusable modules exported from `common::` |
| Segment utilities | `lib/segment/src/common/` | Segment-specific wrappers, fusion, flags, mmap buffers |
| Segment misc | `lib/segment/src/utils/` | e.g. `mem`, `fs` helpers used by segment ops |

The `common` crate’s top-level `lib.rs` declares modules including: `bitpacking`*, `bitvec`, `budget`, `bytes`, `counter`, `cow`, `cpu`, `disk`, `flags`, `fs`, `mmap`, `mmap_hashmap`, `save_on_disk`, `universal_io`, `validation`, `top_k`, `toposort`, `rate_limiting`, `progress_tracker`, `tempfile_ext`, `tar_ext`, `tar_unpack`, and many small numeric/iterator helpers.

\* Several `bitpacking*` modules exist for compressed integer layouts used in storage paths.

## Filesystem Utilities

### Atomic writes (`common::fs`)

`lib/common/common/src/fs/ops.rs` implements **`atomic_save`**: writes through `atomicwrites::AtomicFile` with `OverwriteBehavior::AllowOverwrite`, using a `BufWriter` and flushing before commit. This yields crash-safe replacement of a single file.

Convenience wrappers:

- **`atomic_save_bin`**: `bincode::serialize_into` into the atomic writer.
- **`atomic_save_json`**: `serde_json::to_writer` into the atomic writer.

Companion **`read_bin`** / **`read_json`** use buffered readers. The module defines `Error`, `Result`, `FileOperationResult`, and `FileStorageError` aliases for file-layer failures (IO, bincode, serde_json, generic string).

`lib/common/common/src/fs/mod.rs` re-exports these alongside other `fs` submodules (`check`, `fadvise`, `move`, `safe_delete`, `sync`).

### Directory and file moves

- **`common::fs::move_dir` / `move_file`** (`fs/move.rs`): Prefer `rename`; fall back to copy/move across filesystems; `move_dir` can merge contents into an existing destination.
- **`segment::utils::fs::move_all`** (`lib/segment/src/utils/fs.rs`): Recursively moves all entries from `dir` into `dest_dir`, **merging** subdirectories when the destination already exists and overwriting files. Used when applying snapshots or relocating segment files (see `segment_ops`, collection snapshots).

### Persisted JSON structures

**`SaveOnDisk<T>`** (`save_on_disk.rs`): RwLock-wrapped value with a path; writes use atomic file semantics and optional change notification. Edge and other crates use it for configs that must stay consistent with on-disk state (`load_or_init_default`, `write` closures, etc.).

## Mmap Utilities

### In `common::mmap`

`lib/common/common/src/mmap/mod.rs` exports:

- **`MmapSlice`**, **`MmapType`**, **`MmapBitSlice`**: Read/write mmap-backed storage with typed access; **`MmapFlusher`** for explicit sync.
- **`MmapSliceReadOnly`**, **`MmapTypeReadOnly`**: Read-only variants.
- **`Advice`**, **`AdviceSetting`**, **`Madviseable`**: `madvise`-style hints.
- **`Error`**: Mmap-specific failures (propagated into `OperationError` in segment).
- **`ops`**: `create_and_ensure_length`, `open_read_mmap`, `open_write_mmap`, multi-mmap capability probes, temp file extension constant, and (deprecated) transmute helpers for raw byte views.

Vector and index code typically combine these with **`common::universal_io`** for optional io_uring / cached read paths.

### Buffered mmap updates (segment)

**`MmapSliceBufferedUpdateWrapper<T>`** (`lib/segment/src/common/mmap_slice_buffered_update_wrapper.rs`): Wraps `Arc<RwLock<MmapSlice<T>>>` with an `AHashMap` of pending `(PointOffsetType → T)` updates. **`set`** records changes in memory only; a **`Flusher`** closure (see below) applies them to the mmap in batch. Documented as **write-only**; the underlying slice must not grow. Uses **`IsAliveLock`** so flushers can coordinate with teardown.

A sibling **`mmap_bitslice_buffered_update_wrapper`** applies the same pattern to bit slices.

## Memory Tracking

**`Mem`** (`lib/segment/src/utils/mem.rs`): Lightweight memory statistics for throttling and OOM-aware errors.

- On **Linux**, optionally uses **cgroups** (`cgroups_mem`) when available to read a container memory limit.
- Always maintains **`sysinfo_mem`** for host totals.
- **`refresh`** updates both sources.
- **`total_memory_bytes`** prefers cgroup limit when set; otherwise system total.
- **`available_memory_bytes`** similarly prefers cgroup-aware “available” when cgroup data exists.

Used when deciding whether operations can proceed without exhausting memory (paired with `OperationError::OutOfMemory`).

## Error Types

**`OperationError`** (`lib/segment/src/common/operation_error.rs`): The main operational error enum for segment-level work. Variants include wrong vector dimensions, missing vector names, point ID errors, payload type mismatches, **`ServiceError`** (IO/hardware/corruption-style fatal issues), inconsistent storage, out-of-memory (with free bytes), cancellation, timeout, validation failures, sparse/multi-vector misuse, missing indexes for `order_by` / facet, formula/type errors, non-finite numbers, and conversions from IO, mmap, atomic writes, gridstore, rayon pool creation, etc.

**`OperationResult<T>`** is `Result<T, OperationError>`.

The `edge` crate re-exports these for embedded API stability. `ServiceError` carries an optional serialized backtrace string for diagnostics.

## Other Utilities

### `Flusher`

**`pub type Flusher = Box<dyn FnOnce() -> OperationResult<()> + Send>`** (`lib/segment/src/common/mod.rs`): Callback type returned by mmap-backed structures and wrappers to **persist pending changes** in one shot. Dense/multivector storage, chunked vectors, mmap field indexes, and buffered mmap wrappers all expose a `flusher()` that callers run at commit boundaries.

### Anonymize

**`anonymize`** (`lib/segment/src/common/anonymize.rs`): Procedural macro / helpers used on types in `types.rs` to strip or mask sensitive fields when formatting for logs or debug output (attributes like `#[anonymize(false)]` appear on structs that opt out of hiding).

### Flags

- **`common::flags`**: Shared flag / bitmap utilities used across crates.
- **`segment::common::flags`**: Segment-specific flag representations (including integration with mmap-backed or roaring structures where applicable).

**`stored_bitslice`** and related code bridge mmap, universal I/O, and compact bit storage for indexes.

### Score fusion and ranking

- **`segment::common::score_fusion`**: **`ScoreFusion`** combines multiple ranked lists with configurable **aggregation** (e.g. sum), **normalization** (e.g. min–max, distribution-based), **weights**, and **order**. Includes helpers such as **`dbsf`** for distribution-based fusion presets.
- **`segment::common::reciprocal_rank_fusion`**: RRF-style merging for multi-query or multi-shard result lists.

### Roaring / BitVec

**`common::bitvec`** and bitpacking modules support compact sets and ordered integer compression used in posting lists and links on disk.

### Counters and performance

**`common::counter`**: Hardware and referenced counters, iterator measurement wrappers — used for optional instrumentation.

### Universal I/O

**`common::universal_io`**: Abstraction over plain file reads, mmap, optional **io_uring**, and disk cache layers (`disk_cache`, `read_only`, `local_file_ops`). **`UniversalIoError`** can surface through segment operations.

### Misc `common` helpers (non-exhaustive)

- **`validation`**, **`rate_limiting`**, **`load_concurrency`**, **`budget`**: Cross-cutting control flow.
- **`top_k`**, **`fixed_length_priority_queue`**: Heap utilities for search.
- **`stable_hash`**, **`storage_version`**: Persistence compatibility.
- **`tar_ext`**, **`tar_unpack`**: Archive handling for snapshots/distribution.
- **`is_alive_lock`**: Coordination for structures that must not flush after drop starts.
- **`scope_tracker`**, **`progress_tracker`**: Long-running operation helpers.

## Cross-References

- Segment entry and constructors: `lib/segment/src/segment_constructor/`, `load_segment`.
- Vector storage flush paths: `lib/segment/src/vector_storage/`.
- Field indexes (mmap + `Flusher`): `lib/segment/src/index/field_index/*/mmap_*.rs`.
- Edge embedded runtime: `lib/edge/` (`SaveOnDisk`, `atomic_save_json`).
- GPU / HNSW: `docs/code-nav/utilities/gpu.md` when Vulkan features are enabled.
