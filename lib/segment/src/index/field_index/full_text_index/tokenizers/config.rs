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

    /// Processes a token for indexing. Applies all configured options to the token.
    ///
    /// Returns `None` if:
    /// - The token is empty.
    /// - The token is a stopword.
    /// - The token's chars length is outside of the `min_token_len` and (optionally) `max_token_len` range.
    pub fn process_token<'a>(&self, token: &'a str, check_max_len: bool) -> Option<Cow<'a, str>> {
        let Self {
            lowercase,
            stopwords_filter,
            stemmer,
            min_token_len,
            max_token_len,
        } = self;

        if token.is_empty() {
            return None;
        }

        // Handle lowercase
        let mut token_cow = if *lowercase {
            Cow::Owned(token.to_lowercase())
        } else {
            Cow::Borrowed(token)
        };

        // Handle stopwords
        if stopwords_filter.is_stopword(&token_cow) {
            return None;
        }

        // Handle stemming
        if let Some(stemmer) = stemmer.as_ref() {
            token_cow = stemmer.stem(token_cow);
        };

        // Handle token length
        if min_token_len.is_some_and(|min_len| token_cow.chars().count() < min_len)
            || (check_max_len
                && max_token_len.is_some_and(|max_len| token_cow.chars().count() > max_len))
        {
            return None;
        }

        Some(token_cow)
    }
}
