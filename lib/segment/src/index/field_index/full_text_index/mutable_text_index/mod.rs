use gridstore::Gridstore;
use gridstore::config::StorageOptions;

use super::inverted_index::mutable_inverted_index::MutableInvertedIndex;
use super::tokenizers::Tokenizer;
use crate::data_types::index::TextIndexParams;

mod lifecycle;
mod read_ops;
#[cfg(test)]
mod tests;

pub(super) const GRIDSTORE_OPTIONS: StorageOptions = StorageOptions {
    compression: Some(gridstore::config::Compression::None),
    page_size_bytes: None,
    block_size_bytes: None,
    region_size_blocks: None,
};

pub struct MutableFullTextIndex {
    pub(super) inverted_index: MutableInvertedIndex,
    pub(super) config: TextIndexParams,
    pub(super) storage: Gridstore<Vec<u8>>,
    pub(super) tokenizer: Tokenizer,
}
