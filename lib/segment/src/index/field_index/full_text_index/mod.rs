use std::path::PathBuf;

use common::universal_io::MmapFile;

use self::immutable_text_index::ImmutableFullTextIndex;
use self::mutable_text_index::MutableFullTextIndex;
use self::on_disk_text_index::OnDiskFullTextIndex;
use crate::data_types::index::TextIndexParams;

pub mod full_text_index_read;
mod immutable_text_index;
mod inverted_index;
mod lifecycle;
mod mutable_text_index;
pub use mutable_text_index::update_only::UpdateOnlyTextKind;
pub mod on_disk_text_index;
pub mod read_only;
mod read_ops;
pub mod stop_words;
pub mod tokenizers;

pub use read_only::ReadOnlyFullTextIndex;
pub use read_ops::FullTextConditionChecker;

/// A point's tokens as they are persisted, plus the length measured before the
/// tokens were deduplicated.
///
/// The index is rebuilt from these records on every open, so the length has to
/// travel with them: without phrase matching the stored tokens are sorted and
/// deduplicated, and the true document length cannot be recovered from them.
#[derive(Debug, serde::Deserialize)]
pub(super) struct StoredDocument {
    pub tokens: Vec<String>,
    /// Total tokens, array boundary sentinels excluded. Zero on records written
    /// before this field existed; such indexes are rebuilt from payload.
    #[serde(default)]
    pub doc_len: u32,
}

#[cfg(test)]
mod tests;

pub enum FullTextIndex {
    Mutable(MutableFullTextIndex),
    Immutable(ImmutableFullTextIndex),
    OnDisk(OnDiskFullTextIndex<MmapFile>),
}

pub struct FullTextGridstoreIndexBuilder {
    dir: PathBuf,
    config: TextIndexParams,
    index: Option<FullTextIndex>,
}
