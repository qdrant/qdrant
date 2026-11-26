use std::marker::PhantomData;
use std::path::PathBuf;

use common::universal_io::{MmapFile, UniversalRead};

use super::inverted_index::immutable_inverted_index::ImmutableInvertedIndex;
use super::tokenizers::Tokenizer;

mod lifecycle;
mod live_reload;
mod read_ops;

/// An on-disk index loaded whole into RAM.
///
/// The on-disk index is consumed by the load rather than kept as backing
/// storage: its files are written once at build time and deletions are not
/// persisted, so all that is left of it is where its files are.
pub struct ImmutableFullTextIndex<S: UniversalRead = MmapFile> {
    pub(super) inverted_index: ImmutableInvertedIndex,
    pub(super) tokenizer: Tokenizer,
    /// Directory of the on-disk files, removed whole by `wipe`.
    pub(super) path: PathBuf,
    /// The on-disk index's files, as it listed them when loaded.
    pub(super) files: Vec<PathBuf>,
    /// Snapshot of approximate RAM usage at construction time.
    /// Not refreshed on `remove_point`.
    pub(super) cached_ram_usage_bytes: usize,
    pub(super) _storage: PhantomData<S>,
}
