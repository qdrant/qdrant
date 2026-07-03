//! Serialized HNSW graph links: build, store, parse, and read.
//!
//! # Structures and relations
//!
//! ```text
//!        BUILD (write path)                         LOAD (read path)
//!
//!   Vec<Vec<Vec<edges>>>                       fs: impl UniversalReadFs
//!           │                                          │
//!           │  + GraphLinksFormatParam (format.rs)     │  GraphLinks::load_universal
//!           │      ├─ Plain / Compressed               │   opens the file and keeps
//!           │      └─ CompressedWithVectors            │   the handle alive
//!           │             └─ &dyn GraphLinksVectors    │
//!           ▼                  (vectors.rs)            │
//!   serialize_graph_links ──────────────┐              │
//!       (serializer.rs)                 │              │
//!           │ writes bytes via `header` │              │
//!           ▼                           ▼              ▼
//!   ┌───────────────────────────────────────────────────────────────────────┐
//!   │ GraphLinks  (links.rs) — public handle, a self_cell of:               │
//!   │                                                                       │
//!   │   owner:  GraphLinksEnum (storage.rs)   dependent: GraphLinksView<'_> │
//!   │           ├─ Ram(Vec<u8>)                          (view.rs)          │
//!   │           └─ Universal(                  zero-copy parse that BORROWS │
//!   │                Box<dyn GraphLinksStorage>)  the owner's bytes; every  │
//!   │                     ▲  erases an `S: UniversalRead`  read accessor    │
//!   │                     │  file handle (mmap, io_uring, …)  (links /      │
//!   │                     │                       point_level / …) reads    │
//!   │                     │                       through the view, so the  │
//!   │                     │                       search hot path does no   │
//!   │                     │                       dynamic dispatch.         │
//!   └───────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! | file           | responsibility                                                       |
//! |----------------|----------------------------------------------------------------------|
//! | [`links`]      | [`GraphLinks`] — public handle pairing owned bytes with a parsed view |
//! | [`storage`]    | `GraphLinksEnum` + `GraphLinksStorage`: RAM vs. universal-IO backing  |
//! | [`view`]       | `GraphLinksView` — zero-copy parser/accessor over the bytes           |
//! | [`serializer`] | [`serialize_graph_links`] — edges → serialized bytes                  |
//! | [`header`]     | on-disk header structs shared by the serializer and the view         |
//! | [`format`]     | [`GraphLinksFormat`] / [`GraphLinksFormatParam`] format selectors     |
//! | [`vectors`]    | [`GraphLinksVectors`] & friends: inline vectors for `CompressedWithVectors` |
//!
//! # Serialized layout
//!
//! ```text
//!                                    sorted
//!                      points:        points:
//! points to lvl        012345         142350
//!      0 -> 0
//!      1 -> 4    lvl4:  7       lvl4: 7
//!      2 -> 2    lvl3:  Z  Y    lvl3: ZY
//!      3 -> 2    lvl2:  abcd    lvl2: adbc
//!      4 -> 3    lvl1:  ABCDE   lvl1: ADBCE
//!      5 -> 1    lvl0: 123456   lvl0: 123456  <- lvl 0 is not sorted
//!
//!
//! lvl offset:        6       11     15     17
//!                    │       │      │      │
//!                    │       │      │      │
//!                    ▼       ▼      ▼      ▼
//! indexes:  012345   6789A   BCDE   FG     H
//!
//! flatten:  123456   ADBCE   adbc   ZY     7
//!                    ▲ ▲ ▲   ▲ ▲    ▲      ▲
//!                    │ │ │   │ │    │      │
//!                    │ │ │   │ │    │      │
//!                    │ │ │   │ │    │      │
//! reindex:           142350  142350 142350 142350  (same for each level)
//!
//!
//! for lvl > 0:
//! links offset = level_offsets[level] + offsets[reindex[point_id]]
//! ```

mod format;
mod header;
mod links;
mod serializer;
mod storage;
mod vectors;
mod view;

#[cfg(test)]
mod tests;

pub use format::{GraphLinksFormat, GraphLinksFormatParam};
pub use links::{GraphLinks, GraphLinksResidency};
pub use serializer::serialize_graph_links;
pub use vectors::{GraphLinksVectors, GraphLinksVectorsLayout, StorageGraphLinksVectors};
pub use view::LinksIterator;

/// Sort the first `m` values in `links` and return them. Used to compare stored
/// links where the order of the first `m` links is not preserved.
#[cfg(test)]
pub(super) fn normalize_links(
    m: usize,
    mut links: Vec<common::types::PointOffsetType>,
) -> Vec<common::types::PointOffsetType> {
    let first = links.len().min(m);
    links[..first].sort_unstable();
    links
}
