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

/// A point's tokens as they are persisted.
///
/// The index is rebuilt from these records on every open, and without phrase
/// matching the tokens are sorted and deduplicated on the way in, so a length
/// cannot be recovered from them and has to travel alongside.
#[derive(serde::Deserialize)]
struct StoredDocument {
    tokens: Vec<String>,
    /// Array boundary sentinels excluded. `None` on a record written by an
    /// index that does not record lengths.
    #[serde(default)]
    doc_len: Option<u32>,
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
    /// Whether the built index records document lengths. Production passes
    /// `config.scoring()`; tests override it, as they do for the mmap builder.
    scoring: bool,
    index: Option<FullTextIndex>,
}
