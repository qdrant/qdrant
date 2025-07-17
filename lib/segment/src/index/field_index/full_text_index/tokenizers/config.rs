use std::borrow::Cow;

use super::stemmer::Stemmer;
use crate::index::field_index::full_text_index::stop_words::StopwordsFilter;

// TODO(rocksdb): Remove `Clone` once rocksdb has been removed!
#[derive(Debug, Clone, Default)]
pub struct TokenizerConfig {
    pub lowercase: bool,
    pub stopwords_filter: StopwordsFilter,
    pub stemmer: Option<Stemmer>,
    pub min_token_len: Option<usize>,
    pub max_token_len: Option<usize>,
}

impl TokenizerConfig {
    /// Applies stemming if enabled and applies the configured stemming algorithm. Does nothing if
    /// stemming is disabled.
    pub fn stem_if_enabled<'a>(&self, input: Cow<'a, str>) -> Cow<'a, str> {
        let Some(stemmer) = self.stemmer.as_ref() else {
            return input;
        };

        stemmer.stem(input)
    }
}
