# Sparse index (`lib/sparse`)

## Overview

The **sparse** crate provides **inverted indexes** for sparse vector search. Sparse vectors are stored as variable-length **dimension → weight** pairs (`indices` + `values`). The inverted index maps each **dimension** (internal `DimOffset`) to a **posting list** of **point offsets** with the weight that point has on that dimension. Querying walks the posting lists for the query’s dimensions and combines weights into scores.

## Location

| Area | Path |
|------|------|
| Crate root | `lib/sparse/src/lib.rs` (re-exports `common`, `index`) |
| Index modules | `lib/sparse/src/index/mod.rs` |
| Inverted index trait & implementations | `lib/sparse/src/index/inverted_index/` |
| Search orchestration | `lib/sparse/src/index/search_context.rs` |
| Compressed posting layout (sparse-specific) | `lib/sparse/src/index/compressed_posting_list.rs` |
| RAM posting list (mutable / legacy mmap round-trip) | `lib/sparse/src/index/posting_list.rs` |
| Vector types | `lib/sparse/src/common/sparse_vector.rs` |
| `DimOffset`, `DimWeight`, `Weight` | `lib/sparse/src/common/types.rs` |

## SparseVector

**`SparseVector`** (`common/sparse_vector.rs`) is the user-facing structure: parallel **`indices: Vec<DimId>`** and **`values: Vec<DimWeight>`** (`DimWeight` is `f32`). Indices must be unique; lengths must match. Helpers include **`double_sort`** to sort by index and **`RemappedSparseVector`**, which uses **`DimOffset`** (segment-internal dimension ids) instead of global `DimId`—that is what **`InvertedIndex::upsert` / `remove`** take after remapping.

## InvertedIndex trait

**`InvertedIndex`** (`index/inverted_index/mod.rs`) is the main interface:

- **Lifecycle / storage:** `open`, `save`, `is_on_disk`, `files`, `immutable_files`, `from_ram_index` (materialize from `InvertedIndexRam` into an on-disk or compressed form).
- **Posting access:** `get(dim, hw_counter) -> Option<Self::Iter<'_>>`, `posting_list_len`, `len`, `is_empty`, `max_index`.
- **Mutations:** `upsert(point_id, vector, old_vector)`, `remove(point_id, old_vector)`.
- **Stats:** `vector_count`, `total_sparse_vectors_size`.

Associated types include **`Iter<'a>`** (posting iterator, must implement **`PostingListIter`** from `posting_list_common`) and **`Version: StorageVersion`**. On-disk filenames include **`inverted_index.dat`** (`INDEX_FILE_NAME`) and legacy **`inverted_index.data`**.

## Implementations

### RAM

**`InvertedIndexRam`** (`inverted_index/inverted_index_ram.rs`) keeps **`postings: Vec<PostingList>`** where each `PostingList` is the **sparse-local** type in `index/posting_list.rs`: a sorted **`Vec<PostingElementEx>`** (point id + weight + pruning metadata). Gaps are filled with empty lists so dimension `d` maps to `postings[d]`. It is **not** versioned for disk (`open`/`save` panic); it is the **mutable in-memory** form used while building or editing before persistence.

### Mmap

**`InvertedIndexMmap`** (`inverted_index/inverted_index_mmap.rs`) memory-maps **`inverted_index.dat`** plus JSON config (`inverted_index_config.json`). **`StorageVersion`** is **`"0.1.0"`**. Headers describe each posting list’s byte range in the file. **`InvertedIndexImmutableRam`** (`inverted_index_immutable_ram.rs`) loads that mmap layout into RAM as **`InvertedIndexRam`**; the module notes it is **legacy** and slated for removal in favor of the compressed path.

### Compressed (RAM and Mmap)

**`InvertedIndexCompressedMmap<W>`** and **`InvertedIndexCompressedImmutableRam<W>`** (`inverted_index_compressed_mmap.rs`, `inverted_index_compressed_immutable_ram.rs`) store posting lists as **`CompressedPostingList<W>`** (`compressed_posting_list.rs`): **BitPacker4x**-compressed ids in **`CHUNK_SIZE`-element** blocks (128), fixed **`weights: [W; CHUNK_SIZE]`** per chunk, remainders, optional **`last_id`**, and **`W::QuantizationParams`** for dequantization.

**`Weight`** (`common/types.rs`) implementations used for stored weights:

- **`f32`** — full precision; `QuantizationParams = ()`.
- **`half::f16`** — half-float storage.
- **`QuantizedU8`** — per-list **min** + **diff256** params; weights quantized to **u8** in `[0, 255]`.

A **`#[cfg(feature = "testing")]` `Weight` for `u8`** also exists for tests. Compressed on-disk version string is **`"0.2.0"`** (see `InvertedIndexCompressedMmap`).

> **Note:** This compressed layout lives **inside** `lib/sparse` (`compressed_posting_list.rs`). It follows the same **128-id BitPacker4x chunk** idea as **`lib/posting_list`** but is specialized for sparse weights and mmap file layout.

## Search

**`SearchContext`** (`search_context.rs`) ties a **query** (`RemappedSparseVector`), an **`InvertedIndex`**, and **top-k** scoring:

1. For each query dimension, it calls **`inverted_index.get(dim, hw_counter)`** and builds **`IndexedPostingListIterator`** entries pairing the posting iterator with **`query_index`** and **`query_weight`**.
2. It records **min/max point id** across those lists for range batching and sets **`use_pruning`** when iterators support reliable **`max_next_weight`** and all query weights are **non-negative** (documented limitation for negative query values).

**`plain_search`** walks candidate ids, uses **`skip_to(id)`** on each posting list to collect matching weights, then **`score_vectors`** (`sparse_vector.rs`) to match the partial sparse vector against the query.

The batched path uses **`advance_batch`**: for a range of point ids, it clears a **pooled** score slice and, for each posting list, **`for_each_till_id`** adds **`weight * query_weight`** into the slot **`id - batch_start_id`**, then pushes high scores into **`TopK`**.

## Cross-References

- **Generic compressed posting lists (BitPacker4x, optional payloads):** [`posting-list.md`](posting-list.md) — crate **`lib/posting_list`**, used heavily by **full-text** inverted indexes under `lib/segment/.../full_text_index/`.
- **Sparse-local RAM postings:** `lib/sparse/src/index/posting_list.rs` (not the same type as `posting_list::PostingList`).
- **Shared iterator contract:** `lib/sparse/src/index/posting_list_common.rs` (`PostingListIter`, `PostingElementEx`, etc.).
- **Segment / vector storage:** [`../components/vector-storage.md`](../components/vector-storage.md), [`../components/indexes.md`](../components/indexes.md) if present in your tree.
