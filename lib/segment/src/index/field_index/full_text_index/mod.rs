#![allow(dead_code)]
mod fuzzy_index;
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
pub mod on_disk_text_index;
pub mod read_only;
mod read_ops;
pub mod stop_words;
pub mod tokenizers;

pub use read_only::ReadOnlyFullTextIndex;
pub use read_ops::FullTextConditionChecker;

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
