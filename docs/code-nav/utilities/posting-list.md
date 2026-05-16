# Posting list (`lib/posting_list`)

## Overview

The **posting_list** crate implements **generic compressed posting lists** for sorted **point IDs** (`PointOffsetType`). IDs are compressed with **BitPacker4x** in fixed **128-element** chunks (`CHUNK_LEN`); optional **per-element values** attach to each id via the **`PostingValue`** / **`ValueHandler`** machinery (**sized** values inline in the chunk or **unsized** values in a side **`var_size_data`** buffer). The same abstraction backs **full-text inverted indexes** in `lib/segment` (term → postings, often with unsized payloads) and is the **reference** for how Qdrant names “compressed posting list” patterns elsewhere (e.g. sparse’s separate `compressed_posting_list` module).

## Location

| Area | Path |
|------|------|
| Crate entry, constants | `lib/posting_list/src/lib.rs` (`CHUNK_LEN = 128`, `BitPacker4x`) |
| Layout & types | `lib/posting_list/src/posting_list.rs` |
| Construction | `lib/posting_list/src/builder.rs` |
| Sequential access | `lib/posting_list/src/iterator.rs`, `visitor.rs` |
| Borrowed / mmap-friendly view | `lib/posting_list/src/view.rs` |
| Value strategies | `lib/posting_list/src/value_handler.rs` |

## `PostingList<V>`

**`PostingList<V: PostingValue>`** (`posting_list.rs`) owns:

- **`id_data: Vec<u8>`** — compressed id bitstreams for all full chunks.
- **`chunks: Vec<PostingChunk<SizedTypeFor<V>>>`** — one metadata record per 128-id chunk: **`initial_id`**, **`offset`** into `id_data`, and **`sized_values: [S; CHUNK_LEN]`** when the handler uses fixed-size values.
- **`remainders: Vec<RemainderPosting<SizedTypeFor<V>>>`** — tail elements (id + sized value) that do not fill a full chunk.
- **`var_size_data: Vec<u8>`** — concatenation of variable-size serialized values when using **`UnsizedHandler`**.
- **`last_id: Option<PointOffsetType>`** — last id in the list (optimization for bounds / tail handling).

**`PostingList<()>`** is **`IdsPostingList`** — ids only, no per-id payload (`lib.rs`).

**`PostingElement<V>`** is the logical **id + value** pair exposed when iterating.

## PostingChunk

**`PostingChunk<S>`** (`posting_list.rs`) is a **`#[repr(C)]`** record:

- **`initial_id`** — first id of the chunk; passed to the bitpacker for **sorted-delta** compression.
- **`offset`** — byte offset in **`id_data`** where this chunk’s compressed block starts.
- **`sized_values`** — `[S; 128]` parallel to the 128 decompressed ids when values are **fixed-size** (e.g. `()`, `u32`, `u64` per **`SizedValue`** impls in `lib.rs`).

**`RemainderPosting<S>`** stores **`id`** + **`value`** for the non-chunk tail using **`zerocopy`** `U32` for the id.

## Builder

**`PostingBuilder<V>`** (`builder.rs`):

1. **`add(id, value)`** appends elements (no ordering requirement at insert time).
2. **`build()`** sorts by **`id`**, splits **`ids`** and **`values`**, and calls **`V::Handler::process_values`** to produce **sized** chunks and/or **`var_size_data`**.
3. For each full run of **128** ids, it computes **`num_bits_sorted`** via the bitpacker, reserves **`id_data`** space, appends a **`PostingChunk`** with **`initial_id`**, **`offset`**, and the 128-sized value array.
4. Remaining ids become **`RemainderPosting`** rows.
5. A second pass **`compress_sorted`** fills **`id_data`** for each chunk.

**`PostingBuilder<()>`** exposes **`add_id`** for convenience (`builder.rs`).

## Iterator and View

**`PostingListView<'a, V>`** (`view.rs`) is a **non-owning** slice view of **`id_data`**, **`chunks`**, **`remainders`**, and **`var_size_data`** — suitable for **mmap-backed** bytes interpreted as a posting list without copying. **`PostingListComponents<'a, S>`** bundles the raw parts for low-level callers.

**`PostingVisitor<'a, V>`** (`visitor.rs`) wraps a view, **lazy-decompresses** one **128-id chunk** at a time into a cached array, and supports **binary search** / **`search_greater_or_equal`** for seekable traversal.

**`PostingIterator<'a, V>`** (`iterator.rs`) walks via a visitor; it implements **`Iterator`** and provides **`advance_until_greater_or_equal`** for merging / skipping to an id.

**`PostingList`** implements **`Borrow`** so callers can obtain views; **`PostingListView::to_owned`** reconstructs an owned **`PostingList`**.

## Cross-References

- **Full-text usage:** `lib/segment/src/index/field_index/full_text_index/inverted_index/` (`PostingBuilder`, `PostingList`, `PostingListView`, `PostingValue`, mmap postings).
- **Sparse vectors:** [`sparse-index.md`](sparse-index.md) — sparse indexing uses **`lib/sparse`’s own** `index/posting_list.rs` for mutable RAM lists and **`compressed_posting_list.rs`** for compressed mmap; same **BitPacker4x / 128** chunk idea, different types and file layout.
- **Hardware / measurement:** iterators and segment code may thread counters; sparse search uses `HardwareCounterCell` on `InvertedIndex::get` (see sparse doc).
