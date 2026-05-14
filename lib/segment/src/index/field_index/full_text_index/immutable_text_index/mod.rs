use super::inverted_index::immutable_inverted_index::ImmutableInvertedIndex;
use super::mmap_text_index::MmapFullTextIndex;

mod lifecycle;
mod read_ops;

pub struct ImmutableFullTextIndex {
    pub(super) inverted_index: ImmutableInvertedIndex,
    // Backing storage, source of state, persists deletions
    pub(super) storage: Storage,
    /// Snapshot of approximate RAM usage at construction time.
    /// Not refreshed on `remove_point`.
    pub(super) cached_ram_usage_bytes: usize,
}

pub(super) enum Storage {
    Mmap(Box<MmapFullTextIndex<common::universal_io::MmapFile>>),
}
