use std::borrow::Cow;
use std::str::FromStr;
use std::sync::{Arc, Once};

use super::stemmer::Stemmer;
use crate::data_types::index::{Language, StemmingAlgorithm, StopwordsInterface};
use crate::index::field_index::full_text_index::stop_words::StopwordsFilter;

const DEFAULT_BM25_LANGUAGE: &str = "english";

/// Tokens are cut to this many characters before any other processing, whatever `max_token_len`
/// is. Real words are much shorter, and processing longer tokens gets expensive: snowball
/// stemming takes time quadratic in token length. Cutting instead of dropping keeps long tokens,
/// like URLs, searchable by their beginning.
pub const MAX_TOKEN_CHARS: usize = 1024;

/// Cuts `token` to its first [`MAX_TOKEN_CHARS`] characters.
pub fn truncate_to_max_token_chars(token: Cow<'_, str>) -> Cow<'_, str> {
    // A character takes at least one byte, so most tokens don't need counting
    if token.len() <= MAX_TOKEN_CHARS {
        return token;
    }
    match token.char_indices().nth(MAX_TOKEN_CHARS) {
        Some((end, _)) => super::truncate_cow(token, end),
        None => token,
    }
}

#[derive(Debug, Default)]
pub struct TokensProcessor {
    pub lowercase: bool,
    pub ascii_folding: bool,
    stopwords_filter: Arc<StopwordsFilter>, // TODO: remove `Arc<>`
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

    /// BM25 inference preprocessing, shared by qdrant and edge so both embed text alike. An
    /// unrecognized `language` disables stemming and stopwords: deprecated, kept for old configs.
    pub fn for_bm25(
        language: Option<&str>,
        lowercase: Option<bool>,
        ascii_folding: Option<bool>,
        stopwords: Option<StopwordsInterface>,
        stemmer: Option<StemmingAlgorithm>,
        min_token_len: Option<usize>,
        max_token_len: Option<usize>,
    ) -> Self {
        let lowercase = lowercase.unwrap_or(true);
        let language = language.unwrap_or(DEFAULT_BM25_LANGUAGE);

        if stopwords.is_none() && stemmer.is_none() && Language::from_str(language).is_err() {
            // Once per process: this runs for every embedded input.
            static DEPRECATION_WARNING: Once = Once::new();
            DEPRECATION_WARNING.call_once(|| {
                log::warn!(
                    "BM25 text preprocessing: language {language:?} is not recognized; \
                     stemming and stopwords are disabled as a side effect. This behavior \
                     (notably `language: \"none\"`) is deprecated and may be rejected in a \
                     future release. To disable language-specific processing explicitly, \
                     set `stemmer` to {{\"type\": \"none\"}} and configure an empty stopword set.",
                );
            });
        }

        let stemmer = match stemmer {
            None => Stemmer::try_default_from_language(language),
            // `Disabled` resolves to `None` here, giving an explicit opt-out of stemming.
            Some(algorithm) => Stemmer::from_algorithm(&algorithm),
        };
        let stopwords = match stopwords {
            None => Language::from_str(language)
                .ok()
                .map(StopwordsInterface::Language),
            Some(interface) => Some(interface),
        };

        Self::new(
            lowercase,
            ascii_folding.unwrap_or(false),
            Arc::new(StopwordsFilter::new(&stopwords, lowercase)),
            stemmer,
            min_token_len,
            max_token_len,
        )
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

    /// Whether processing an ASCII token does nothing but (optionally) lowercase it: no stopwords,
    /// stemming or configured length limits. ASCII folding leaves ASCII unchanged.
    ///
    /// Tokens are still cut to [`MAX_TOKEN_CHARS`].
    pub fn is_ascii_lowercase_only(&self) -> bool {
        self.stopwords_filter.is_empty()
            && self.stemmer.is_none()
            && self.min_token_len.is_none()
            && self.max_token_len.is_none()
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

        token_cow = truncate_to_max_token_chars(token_cow);

        // Handle ASCII folding (normalize accents)
        if *ascii_folding {
            token_cow = super::ascii_folding::fold_to_ascii_cow(token_cow);
        }

        // Handle lowercase
        if *lowercase {
            token_cow = lowercase_cow(token_cow);
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

    /// Processes a token for indexing. Cuts it to [`MAX_TOKEN_CHARS`], and applies all configured
    /// options to it.
    ///
    /// Returns `None` if:
    /// - The token is empty.
    /// - The token is a stopword.
    /// - The token's chars length is outside of the `min_token_len` and (optionally) `max_token_len` range.
    pub fn process_token<'a>(&self, token: &'a str, check_max_len: bool) -> Option<Cow<'a, str>> {
        self.process_token_cow(Cow::Borrowed(token), check_max_len)
    }
}

/// Lowercases `token`, without allocating when it is ASCII and already lowercase.
fn lowercase_cow(token: Cow<'_, str>) -> Cow<'_, str> {
    if !token.is_ascii() {
        return Cow::Owned(token.to_lowercase());
    }
    if !token.bytes().any(|b| b.is_ascii_uppercase()) {
        return token;
    }
    let mut owned = token.into_owned();
    owned.make_ascii_lowercase();
    Cow::Owned(owned)
}
