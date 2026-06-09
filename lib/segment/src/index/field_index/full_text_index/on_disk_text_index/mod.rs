use std::path::PathBuf;

use common::bitvec::BitVec;
use common::universal_io::{MmapFile, UniversalRead};

use super::inverted_index::mutable_inverted_index::MutableInvertedIndex;
use super::inverted_index::on_disk_inverted_index::OnDiskInvertedIndex;
use super::tokenizers::Tokenizer;
use crate::data_types::index::TextIndexParams;

mod lifecycle;
mod live_reload;
mod read_ops;

pub struct OnDiskFullTextIndex<S: UniversalRead = MmapFile> {
    pub(in super::super) inverted_index: OnDiskInvertedIndex<S>,
    pub(in super::super) tokenizer: Tokenizer,
}

pub struct FullTextMmapIndexBuilder {
    pub(super) path: PathBuf,
    pub(super) mutable_index: MutableInvertedIndex,
    pub(super) config: TextIndexParams,
    pub(super) is_on_disk: bool,
    pub(super) tokenizer: Tokenizer,
    pub(super) deleted_points: BitVec,
}
