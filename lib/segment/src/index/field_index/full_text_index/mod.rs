use std::path::PathBuf;

use common::universal_io::MmapFile;

use self::immutable_text_index::ImmutableFullTextIndex;
use self::mmap_text_index::MmapFullTextIndex;
use self::mutable_text_index::MutableFullTextIndex;
use crate::data_types::index::TextIndexParams;

pub mod full_text_index_read;
mod immutable_text_index;
mod inverted_index;
mod lifecycle;
pub mod mmap_text_index;
mod mutable_text_index;
pub mod read_only;
mod read_ops;
pub mod stop_words;
pub mod tokenizers;

#[cfg(test)]
mod tests;

#[allow(clippy::large_enum_variant)]
pub enum FullTextIndex {
    Mutable(MutableFullTextIndex),
    Immutable(ImmutableFullTextIndex),
    Mmap(Box<MmapFullTextIndex<MmapFile>>),
}

pub struct FullTextGridstoreIndexBuilder {
    dir: PathBuf,
    config: TextIndexParams,
    index: Option<FullTextIndex>,
}
