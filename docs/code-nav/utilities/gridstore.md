# Gridstore

## Overview

**Gridstore** is an **mmap-backed** storage engine mapping **sequential point IDs** (`PointOffset`, `u32`: 0, 1, 2, …) to **variable-size values**. Data lives in fixed-size **pages** (`page_{id}.dat`); a **tracker** maps each point ID to a `ValuePointer` (page, block offset, length); a **bitmask** records which fixed-size **blocks** are allocated. Values are stored as bytes via the **`Blob`** trait, with optional **LZ4** compression configured in `StorageConfig`.

The crate is used for **mmap payload storage** and **field indexes** elsewhere in Qdrant (see cross-references).

## Location

| Area | Path |
|------|------|
| Library root | `lib/gridstore/` |
| Main types | `lib/gridstore/src/lib.rs`, `lib/gridstore/src/gridstore/mod.rs` |
| Tracker / pages / bitmask | `lib/gridstore/src/tracker.rs`, `pages.rs`, `bitmask/mod.rs` |
| Config | `lib/gridstore/src/config.rs` |

## Architecture

**`Gridstore<V>`** (`gridstore/mod.rs`) holds:

- **`Arc<RwLock<Tracker>>`** — For each `PointOffset`, stores `ValuePointer` (page id, block offset, payload length in bytes) and pending in-RAM updates for flush.
- **`Arc<RwLock<Pages<MmapFile>>>`** — Vector of mmap page files; sequential listing of `page_*.dat` on open.
- **`Arc<RwLock<Bitmask>>`** — One bit per **block** (configurable block size); **1 = used**, **0 = free**. Includes **`regions_gaps`** (`bitmask/gaps.rs`) for fast free-space queries.
- **`StorageConfig`** — Page size, block size, region size (blocks per bitmask region), compression.
- **`IsAliveLock`** — Coordinates flush vs `clear`/teardown.

**Open path:** Reads `config.json` and tracker, opens bitmask, infers **expected page count** from bitmask, then opens pages and **errors** if page file count ≠ inferred count (consistency check).

**New storage:** Creates bitmask, tracker, first page file, writes config via `atomic_save_json`.

Together: the **tracker** is the index; **pages** hold bytes; the **bitmask** is the block-level allocator and must stay consistent with what the tracker references after flush.

## Blob Trait

Defined in `lib/gridstore/src/blob.rs`:

```rust
pub trait Blob {
    fn to_bytes(&self) -> Vec<u8>;
    fn from_bytes(bytes: &[u8]) -> Self;
}
```

**`put_value`** calls `V::to_bytes()`; **`get_value`** reads bytes from pages, optional decompress, then `V::from_bytes`. Built-in implementations include `Vec<u8>`, `Vec<ecow::EcoString>` (CBOR), `Vec<(f64, f64)>` (raw little-endian pairs), and zerocopy-backed `Vec<i64>`, `Vec<u128>`, `Vec<f64>`.

This is the extension point for **typed** storage without changing the page format.

## Write Path (`put_value`)

`Gridstore::put_value` (`gridstore/mod.rs`):

1. **`to_bytes`** then **compress** per `StorageConfig::compression` (`GridstoreView::compress` — none or LZ4 with size prefix).
2. Compute **required blocks** = compressed length divided by `block_size_bytes`, rounded up.
3. **`find_or_create_available_blocks`** — Read bitmask for a free run; if insufficient trailing space, **`create_new_page`** (new `page_*.dat`, **`bitmask.cover_new_page`**).
4. **`write_into_pages`** — Writes compressed bytes to page(s) via `Pages::write_to_pages` using a `ValuePointer` for start page, block offset, and length.
5. **`bitmask.mark_blocks(..., true)`** — Mark those blocks used (and gap structures updated).
6. **`tracker.set(point_offset, ValuePointer)`** — In-memory pointer for the ID; tracks whether this was an update.

**Crash-safety rationale** (documented inline in ASCII diagram): Without flushing every write, ordering is chosen so the worst case is **leaked blocks** (marked used without a committed tracker pointer), not **corrupt reads**. Data is written to pages **before** the bitmask marks blocks used **before** the tracker records the pointer in RAM; durable visibility of the new pointer happens when the user runs **`flusher()`** (see Flush).

## Read Path (`get_value`)

`Gridstore::get_value` delegates to **`GridstoreView::get_value`** (`gridstore/view.rs`):

1. **`tracker.get(point_offset)`** → optional `ValuePointer`.
2. **`pages.read_from_pages`** with the configured **`AccessPattern`** generic (`Random` vs `Sequential` for IO hints).
3. **`decompress`** according to `StorageConfig::compression`.
4. **`V::from_bytes`** → typed value.

**`GridstoreReader`** (`gridstore/reader.rs`) exposes the same read path without mutability.

## Flush

**`Gridstore::flusher()`** returns a **`Flusher`** (`Box<dyn FnOnce() -> Result<(), GridstoreError> + Send>`) that must be invoked to persist pending state. Order inside the closure (`gridstore/mod.rs`):

1. **Bitmask flusher** — Persist bitmask (and related gap state) after in-memory updates from writes.
2. **Pages flusher** — Flush mmap page data.
3. **`flush_tracker`** — `write_pending` on tracker, then tracker’s flusher so **pointers** point at flushed page data.
4. **`flush_free_blocks`** — For **old** pointers replaced during pending updates, **`mark_blocks_batch(..., false)`** on the bitmask to free those blocks, then bitmask flush again.

So after a successful flush: **bitmask (used) → pages → tracker** establishes new data; then **bitmask** is updated again to **free** superseded regions. This ordering supports the crash model described on the write path.

Concurrent flush is guarded by **`is_alive_flush_lock`**; if gridstore was cleared, flush returns **`FlushCancelled`**.

## Configuration

**`StorageOptions`** (`config.rs`) — optional overrides when **creating** storage:

- `page_size_bytes` — default **32 MiB**
- `block_size_bytes` — default **128** bytes
- `region_size_blocks` — default **8192** blocks per bitmask region
- `compression` — **`Compression::None`** or **`Compression::LZ4`** (default LZ4)

**`StorageConfig`** — resolved, validated structure: page size must be ≥ block × region and a **multiple** of region size in bytes. Serialized to **`config.json`** in the storage directory (`CONFIG_FILENAME` in reader module).

**`Gridstore::files()`** lists tracker files, all `page_*.dat`, config, bitmask files (`bitmask.dat` and gaps file).

## Cross-References

- **Payload storage component doc:** `docs/code-nav/components/payload-storage.md`
- **Payload indexes:** `docs/code-nav/components/payload-indexes.md`
- **Broader storage:** `docs/code-nav/components/storage.md`
- **Mmap / universal IO:** `common::universal_io::MmapFile` as used by tracker and pages
