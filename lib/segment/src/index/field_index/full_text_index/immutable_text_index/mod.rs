use common::universal_io::{MmapFile, UniversalRead};

use super::inverted_index::immutable_inverted_index::ImmutableInvertedIndex;
use super::on_disk_text_index::OnDiskFullTextIndex;

mod lifecycle;
mod live_reload;
mod read_ops;

pub struct ImmutableFullTextIndex<S: UniversalRead = MmapFile> {
    pub(super) inverted_index: ImmutableInvertedIndex,
    /// Backing mmap storage; source of state, persists deletions.
    pub(super) storage: OnDiskFullTextIndex<S>,
    /// Snapshot of approximate RAM usage at construction time.
    /// Not refreshed on `remove_point`.
    pub(super) cached_ram_usage_bytes: usize,
}
