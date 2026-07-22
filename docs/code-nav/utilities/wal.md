# WAL (Write-Ahead Log)

## Overview

The WAL is a **directory-based append-only log**. Each segment file holds a sequence of byte entries with CRC protection. Higher layers are responsible for calling `flush` on the open segment when they need durability; the raw `Wal::append` path writes through mmap and does not by itself guarantee that data is on disk until the segment is flushed.

The WAL supports **truncation from the tail** (`truncate`) and **prefix truncation** from the logical start (`prefix_truncate`), which deletes or trims closed segment files and is used for retention and acknowledgements. It is used for **crash recovery** by reopening segment files and re-parsing valid entries (see below).

## Location

| Area | Path |
|------|------|
| Core WAL crate | `lib/wal/` |
| Typed WAL wrapper | `lib/shard/src/wal.rs` (`SerdeWal`) |
| Consumers | e.g. `lib/collection/src/shards/local_shard/mod.rs` (`SerdeWal<OperationWithClockTag>`), `lib/edge/src/lib.rs` (`SerdeWal<CollectionUpdateOperations>`) |

## Core WAL (`Wal`)

`Wal` is defined in `lib/wal/src/lib.rs`. Main API:

- **`append`** — Appends a byte slice to the current open segment; returns the global entry index. May rotate to a new segment if capacity is insufficient (via `SegmentCreatorV2`).
- **`entry(index)`** — Returns `Some(Entry)` mmap view of payload bytes, or `None` if the index is out of range.
- **`truncate(from)`** — Removes entries from index `from` onward (open segment truncation, closed segment truncation or deletion, with flush of truncated closed segment when splitting mid-segment).
- **`prefix_truncate(until)`** — Removes oldest closed segments subject to `retain_closed`; may leave a gap between physical `first_index` and logical first index when used from `SerdeWal` (see shard wrapper).
- **`first_index` / `last_index` / `num_entries`** — Global indices across closed + open segments.
- **`flush_open_segment` / `flush_open_segment_async`** — Durably flush the mmap for the active segment.

**Directory layout:** Files named `open-{id}` (active append) and `closed-{start_index}` (immutable prefix). Temporary `tmp-*` files are removed on open. The WAL directory is **exclusively locked** (Unix: lock on the directory file; Windows: proxy `.wal` file) so two `Wal` instances cannot use the same path.

**Options (`WalOptions`):** `segment_capacity` (default 32 MiB), `segment_queue_len` (pre-created segments), `retain_closed` (minimum closed segments kept after prefix truncation; default 1).

## WAL Segments

Implementation: `lib/wal/src/segment.rs`. **SegmentCreatorV2** (`lib/wal/src/segment_creator.rs`) pre-creates `open-{id}` segment files asynchronously (optional queue) and syncs the directory on batch create (Unix).

### On-disk format

Documented in the `Segment` rustdoc:

- **Segment header (8 bytes):** magic `b"wal"`, format version `u8` (currently `0`), **random CRC seed** `u32` (little-endian). The seed chains into entry CRCs so reusing a file does not validate stale data.
- **Each entry:** `u64` length (LE), **data**, **0–7 bytes padding** so the record (header + data + padding) is 8-byte aligned, then **CRC32-C** over `(length || data || padding)`, chained from the previous CRC state.

Segments are **mmap-backed**; an in-memory **index** stores `(offset, len)` per entry.

### Lifecycle

1. **Create** — New file, header written, mmap established (`Segment::create`).
2. **Open** — Mmap file, parse entries from header until CRC failure or incomplete tail (`Segment::open`).
3. **Append** — Write entry, update running CRC and index (caller may `flush` for durability).
4. **Close (at WAL level)** — Rename `open-*` → `closed-{start_index}`, then `Segment::close` drops mmap handle; previous segment may be flushed asynchronously when rotating.

## SerdeWal

`SerdeWal<R>` in `lib/shard/src/wal.rs` wraps `Wal` with **serde**-based records:

- **Serialization (write):** `WalRawRecord::new` uses **`serde_cbor::to_vec`** (comment in code notes intent to prefer MessagePack once a serde issue is resolved).
- **Deserialization (read):** **`serde_cbor::from_slice`**, with **fallback to `rmp_serde::from_slice`** (MessagePack) for older records.

**`first-index` file:** JSON file `first-index` stores `WalState { ack_index }`. On open, `first_index` is derived as `ack_index.max(wal.first_index()).min(wal.last_index())` when the file exists, so the **logical** first readable index can lag the physical start of files after `prefix_truncate`.

**`ack(until_index)`:** Calls `Wal::prefix_truncate`, then updates and persists `first_index` so iterators that skip acknowledged data stay consistent. WAL truncation is **segment-granular**; the code documents that some prefix entries may remain on disk but be **logically** hidden (`truncated_prefix_entries_num`).

**`len(with_acknowledged)`:** Either full `wal.num_entries()` or subtracts the logical prefix gap vs physical `wal.first_index()`.

**Extended retention:** `set_extended_retention` multiplies `retain_closed` by `INCREASED_RETENTION_FACTOR` (10) on the inner `Wal` to keep more closed segments for recoverable history (e.g. shard transfers). `set_normal_retention` restores the configured retention.

**Other API:** `write` (append without extra durability guarantee vs `Wal`), `read` / `read_range` / `read_all`, `flush` / `flush_async`, `drop_from` → `Wal::truncate`.

Concrete `R` types in the tree include **`CollectionUpdateOperations`** (edge) and **`OperationWithClockTag`** (local shard WAL with clock tagging).

## Crash Recovery

1. **Segment open:** Parsing walks entries in order. If **stored CRC ≠ computed CRC**, parsing **stops**; trailing garbage or a partial write is treated as **end of valid log** (tail truncation). If stored CRC is `0` and mismatch, the code breaks without warning (incomplete entry).
2. **Magic / version:** Bad magic or unsupported version → open error.
3. **Multiple `open-*` files:** If more than one non-empty open segment exists (e.g. crash after rename but before directory sync), the WAL **keeps the latest** non-empty open segment and closes the previous into `closed-*` on open.
4. **`tmp-*` files** are deleted on directory scan.

Durability for new appends requires **`flush_open_segment`** (or async equivalent) after `append` when the application needs the same guarantees as “written then applied” semantics at the storage layer.

## Cross-References

- **Shard / collection WAL usage:** `lib/collection/src/shards/local_shard/mod.rs` — WAL path, flush, recovery reads.
- **Edge WAL:** `lib/edge/src/lib.rs` — `SerdeWal<CollectionUpdateOperations>`.
- **Raw WAL tests / behavior:** `lib/wal/src/lib.rs` (integration tests), `lib/wal/src/test_segment_recovery.rs` if present.
- **Storage overview:** `docs/code-nav/components/storage.md`
- **Shard component:** `docs/code-nav/components/shard.md`
