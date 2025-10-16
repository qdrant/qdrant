use std::borrow::Cow;
use std::sync::Arc;

use super::stemmer::Stemmer;
use crate::index::field_index::full_text_index::stop_words::StopwordsFilter;

// TODO(rocksdb): Remove `Clone` once rocksdb has been removed!
#[derive(Debug, Clone, Default)]
pub struct TokensProcessor {
    pub lowercase: bool,
    pub ascii_folding: bool,
    stopwords_filter: Arc<StopwordsFilter>, // TDOO(rocksdb): Remove once rocksdb has been removed!
    stemmer: Option<Stemmer>,
    pub min_token_len: Option<usize>,
    pub max_token_len: Option<usize>,
}

impl TokensProcessor {
    pub fn new(
        lowercase: bool,
        ascii_folding: bool,
        stopwords_filter: Arc<StopwordsFilter>,
        stemmer: Option<Stemmer>,
        min_token_len: Option<usize>,
        max_token_len: Option<usize>,
    ) -> Self {
        Self {
            lowercase,
            ascii_folding,
            stopwords_filter,
            stemmer,
            min_token_len,
            max_token_len,
        }
    }

    #[cfg(test)]
    pub fn set_stopwords(&mut self, stopwords_filter: Arc<StopwordsFilter>) {
        self.stopwords_filter = stopwords_filter;
    }

    /// Applies stemming if enabled and applies the configured stemming algorithm. Does nothing if
    /// stemming is disabled.
    pub fn stem_if_enabled<'a>(&self, input: Cow<'a, str>) -> Cow<'a, str> {
        let Some(stemmer) = self.stemmer.as_ref() else {
            return input;
        };

        stemmer.stem(input)
    }

    /// Applies ASCII folding if enabled. Converts accented characters to their ASCII equivalents.
    pub fn fold_if_enabled<'a>(&self, input: Cow<'a, str>) -> Cow<'a, str> {
        if self.ascii_folding {
            super::ascii_folding::fold_to_ascii_cow(input)
        } else {
            input
        }
    }

    pub fn is_stopword(&self, token: &str) -> bool {
        self.stopwords_filter.is_stopword(token)
    }

    pub fn process_token_cow<'a>(
        &self,
        mut token_cow: Cow<'a, str>,
        check_max_len: bool,
    ) -> Option<Cow<'a, str>> {
        let Self {
            lowercase,
            stopwords_filter,
            stemmer,
            min_token_len,
            max_token_len,
            ascii_folding,
        } = self;

        if token_cow.is_empty() {
            return None;
        }

        // Handle ASCII folding (normalize accents)
        if *ascii_folding {
            token_cow = super::ascii_folding::fold_to_ascii_cow(token_cow);
        }

        // Handle lowercase
        if *lowercase {
            token_cow = Cow::Owned(token_cow.to_lowercase());
        }

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

    /// Processes a token for indexing. Applies all configured options to the token.
    ///
    /// Returns `None` if:
    /// - The token is empty.
    /// - The token is a stopword.
    /// - The token's chars length is outside of the `min_token_len` and (optionally) `max_token_len` range.
    pub fn process_token<'a>(&self, token: &'a str, check_max_len: bool) -> Option<Cow<'a, str>> {
        self.process_token_cow(Cow::Borrowed(token), check_max_len)
    }
}
