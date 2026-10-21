use common::flags::feature_flags;

/// Format of the vocabulary of a newly created index. Existing indexes are
/// opened in whichever format they were written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VocabFormat {
    /// `vocab.dat`, a hash map read in place, token by token, and posting list
    /// headers read in place from `postings.dat`.
    Hashmap,
    /// `compact_vocab.dat` and `posting_directory.dat`, both read whole into
    /// RAM at open, so that a query reads posting bytes alone. See
    /// [`compact_vocab`](super::compact_vocab) and
    /// [`posting_directory`](super::posting_directory).
    Compact,
}

impl VocabFormat {
    /// [`Compact`](Self::Compact) in serverless-compatible deployments, where
    /// every read in place is a request to object storage.
    pub fn from_feature_flags() -> Self {
        if feature_flags().serverless_compatible() {
            Self::Compact
        } else {
            Self::Hashmap
        }
    }
}
