use std::borrow::Cow;
mod japanese;
mod multilingual;
mod stemmer;

use multilingual::MultilingualTokenizer;
use stemmer::Stemmer;

use crate::data_types::index::{TextIndexParams, TokenizerType};
use crate::index::field_index::full_text_index::stop_words::StopwordsFilter;

struct WhiteSpaceTokenizer;

impl WhiteSpaceTokenizer {
    fn tokenize<'a, C: FnMut(Cow<'a, str>)>(
        text: &'a str,
        config: &TokenizerConfig,
        mut callback: C,
    ) {
        for token in text.split_whitespace() {
            if token.is_empty() {
                continue;
            }

            let token_cow = if config.lowercase {
                Cow::Owned(token.to_lowercase())
            } else {
                Cow::Borrowed(token)
            };

            if config.stopwords_filter.is_stopword(&token_cow) {
                continue;
            }

            callback(config.stem_if_enabled(token_cow));
        }
    }
}

struct WordTokenizer;

impl WordTokenizer {
    fn tokenize<'a, C: FnMut(Cow<'a, str>)>(
        text: &'a str,
        config: &TokenizerConfig,
        mut callback: C,
    ) {
        for token in text.split(|c| !char::is_alphanumeric(c)) {
            if token.is_empty() {
                continue;
            }

            let token_cow = if config.lowercase {
                Cow::Owned(token.to_lowercase())
            } else {
                Cow::Borrowed(token)
            };

            if config.stopwords_filter.is_stopword(&token_cow) {
                continue;
            }

            callback(config.stem_if_enabled(token_cow));
        }
    }
}

struct PrefixTokenizer;

impl PrefixTokenizer {
    fn tokenize<'a, C: FnMut(Cow<'a, str>)>(
        text: &'a str,
        config: &TokenizerConfig,
        min_ngram: usize,
        max_ngram: usize,
        mut callback: C,
    ) {
        text.split(|c| !char::is_alphanumeric(c))
            .filter(|token| !token.is_empty())
            .for_each(|word| {
                let word_cow = if config.lowercase {
                    Cow::Owned(word.to_lowercase())
                } else {
                    Cow::Borrowed(word)
                };

                if config.stopwords_filter.is_stopword(&word_cow) {
                    return;
                }

                let word_cow = config.stem_if_enabled(word_cow);

                for n in min_ngram..=max_ngram {
                    let ngram = word_cow.as_ref().char_indices().map(|(i, _)| i).nth(n);
                    match ngram {
                        Some(end) => callback(truncate_cow_ref(&word_cow, end)),
                        None => {
                            callback(word_cow);
                            break;
                        }
                    }
                }
            });
    }

    /// For querying prefixes, it makes sense to use a maximal ngram only.
    /// E.g.
    ///
    /// Warn: Stopwords filter is not applied here, as if we want to start searching
    /// for matches before the full query is typed, we need to allow search on partial words.
    ///
    /// For example:
    ///
    /// Document: `["theory" -> ["th", "the", "theo", "theor", "theory"]]`
    /// Stopwords: `["the"]`
    /// Query: `"the"` -> should match "theory" as it is a prefix.
    ///
    /// Docs. tokens: `"hello"` -> `["he", "hel", "hell", "hello"]`
    /// Query tokens: `"hel"`   -> `["hel"]`
    /// Query tokens: `"hell"`  -> `["hell"]`
    /// Query tokens: `"hello"` -> `["hello"]`
    fn tokenize_query<'a, C: FnMut(Cow<'a, str>)>(
        text: &'a str,
        config: &TokenizerConfig,
        max_ngram: usize,
        mut callback: C,
    ) {
        text.split(|c| !char::is_alphanumeric(c))
            .filter(|token| !token.is_empty())
            .for_each(|word| {
                let word_cow = if config.lowercase {
                    Cow::Owned(word.to_lowercase())
                } else {
                    Cow::Borrowed(word)
                };

                let word_cow = config.stem_if_enabled(word_cow);

                let ngram = word_cow.char_indices().map(|(i, _)| i).nth(max_ngram);
                match ngram {
                    Some(end) => callback(truncate_cow(word_cow, end)),
                    None => {
                        callback(word_cow);
                    }
                }
            });
    }
}

/// Truncates a string inside a `Cow<str>` to the given `len` preserving the `Borrowed` and `Owned` state.
fn truncate_cow<'a>(inp: Cow<'a, str>, len: usize) -> Cow<'a, str> {
    match inp {
        Cow::Borrowed(b) => Cow::Borrowed(&b[..len]),
        Cow::Owned(mut b) => {
            b.truncate(len);
            Cow::Owned(b)
        }
    }
}

/// Truncates a string inside a `&Cow<str>` to the given `len` preserving the `Borrowed` and `Owned` state.
/// `truncate_cow` should be preferred over this function if Cow doesn't need to be passed as reference.
fn truncate_cow_ref<'a>(inp: &Cow<'a, str>, len: usize) -> Cow<'a, str> {
    match inp {
        Cow::Borrowed(b) => Cow::Borrowed(&b[..len]),
        Cow::Owned(b) => Cow::Owned(b[..len].to_string()),
    }
}

#[derive(Debug, Clone)]
pub struct Tokenizer {
    tokenizer_type: TokenizerType,
    min_token_len: Option<usize>,
    max_token_len: Option<usize>,
    config: TokenizerConfig,
}

impl Tokenizer {
    pub fn new(params: &TextIndexParams) -> Self {
        let TextIndexParams {
            r#type: _,
            tokenizer,
            min_token_len,
            max_token_len,
            lowercase,
            on_disk: _,
            phrase_matching: _,
            stopwords,
            stemmer,
        } = params;

        let lowercase = lowercase.unwrap_or(true);
        let stopwords_filter = StopwordsFilter::new(stopwords, lowercase);

        let config = TokenizerConfig {
            lowercase,
            stopwords_filter,
            stemmer: stemmer.as_ref().map(Stemmer::from_algorithm),
        };

        Self {
            tokenizer_type: *tokenizer,
            min_token_len: *min_token_len,
            max_token_len: *max_token_len,
            config,
        }
    }

    fn doc_token_filter<'a, 'b, C: FnMut(Cow<'b, str>) + 'a>(
        &'a self,
        mut callback: C,
    ) -> impl FnMut(Cow<'b, str>) + 'a {
        move |token: Cow<'b, str>| {
            if self
                .min_token_len
                .map(|min_len| token.len() < min_len && token.chars().count() < min_len)
                .unwrap_or(false)
            {
                return;
            }
            if self
                .max_token_len
                .map(|max_len| token.len() > max_len && token.chars().count() > max_len)
                .unwrap_or(false)
            {
                return;
            }

            callback(token);
        }
    }

    pub fn tokenize_doc<'a, C: FnMut(Cow<'a, str>)>(&self, text: &'a str, mut callback: C) {
        let token_filter = self.doc_token_filter(&mut callback);
        match self.tokenizer_type {
            TokenizerType::Whitespace => {
                WhiteSpaceTokenizer::tokenize(text, &self.config, token_filter)
            }
            TokenizerType::Word => WordTokenizer::tokenize(text, &self.config, token_filter),
            TokenizerType::Multilingual => {
                MultilingualTokenizer::tokenize(text, &self.config, token_filter)
            }
            TokenizerType::Prefix => PrefixTokenizer::tokenize(
                text,
                &self.config,
                self.min_token_len.unwrap_or(1),
                self.max_token_len.unwrap_or(usize::MAX),
                token_filter,
            ),
        }
    }

    pub fn tokenize_query<C: FnMut(Cow<str>)>(&self, text: &str, mut callback: C) {
        let token_filter = self.doc_token_filter(&mut callback);
        match self.tokenizer_type {
            TokenizerType::Whitespace => {
                WhiteSpaceTokenizer::tokenize(text, &self.config, token_filter)
            }
            TokenizerType::Word => WordTokenizer::tokenize(text, &self.config, token_filter),
            TokenizerType::Multilingual => {
                MultilingualTokenizer::tokenize(text, &self.config, token_filter)
            }
            TokenizerType::Prefix => PrefixTokenizer::tokenize_query(
                text,
                &self.config,
                self.max_token_len.unwrap_or(usize::MAX),
                token_filter,
            ),
        }
    }
}

// TODO(rocksdb): Remove `Clone` once rocksdb has been removed!
#[derive(Debug, Clone, Default)]
pub struct TokenizerConfig {
    pub(crate) lowercase: bool,
    pub(crate) stopwords_filter: StopwordsFilter,
    pub(crate) stemmer: Option<Stemmer>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_types::index::{
        Language, Snowball, SnowballLanguage, SnowballParams, StemmingAlgorithm,
        StopwordsInterface, TextIndexType,
    };

    fn make_stemmer(language: SnowballLanguage) -> Stemmer {
        Stemmer::from_algorithm(&StemmingAlgorithm::Snowball(SnowballParams {
            r#type: Snowball::Snowball,
            language,
        }))
    }

    #[test]
    fn test_whitespace_tokenizer() {
        let text = "hello world";
        let config = TokenizerConfig::default();

        let mut tokens = Vec::new();
        WhiteSpaceTokenizer::tokenize(text, &config, |token| tokens.push(token));

        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens.first(), Some(&Cow::Borrowed("hello")));
        assert_eq!(tokens.get(1), Some(&Cow::Borrowed("world")));
    }

    #[test]
    fn test_word_tokenizer() {
        let text = "hello, world! Привет, мир!";
        let mut config = TokenizerConfig::default();
        let mut tokens = Vec::new();
        WordTokenizer::tokenize(text, &config, |token| tokens.push(token));
        assert_eq!(tokens.len(), 4);
        assert_eq!(tokens.first(), Some(&Cow::Borrowed("hello")));
        assert_eq!(tokens.get(1), Some(&Cow::Borrowed("world")));
        assert_eq!(tokens.get(2), Some(&Cow::Borrowed("Привет")));
        assert_eq!(tokens.get(3), Some(&Cow::Borrowed("мир")));

        tokens.clear();
        config.lowercase = true;
        WordTokenizer::tokenize(text, &config, |token| tokens.push(token));
        assert_eq!(tokens.len(), 4);
        assert_eq!(tokens.first(), Some(&Cow::Borrowed("hello")));
        assert_eq!(tokens.get(1), Some(&Cow::Borrowed("world")));
        assert_eq!(tokens.get(2), Some(&Cow::Borrowed("привет")));
        assert_eq!(tokens.get(3), Some(&Cow::Borrowed("мир")));
    }

    #[test]
    fn test_prefix_tokenizer() {
        let text = "hello, мир!";
        let config = TokenizerConfig::default();
        let mut tokens = Vec::new();
        PrefixTokenizer::tokenize(text, &config, 1, 4, |token| tokens.push(token));
        eprintln!("tokens = {tokens:#?}");
        assert_eq!(tokens.len(), 7);
        assert_eq!(tokens.first(), Some(&Cow::Borrowed("h")));
        assert_eq!(tokens.get(1), Some(&Cow::Borrowed("he")));
        assert_eq!(tokens.get(2), Some(&Cow::Borrowed("hel")));
        assert_eq!(tokens.get(3), Some(&Cow::Borrowed("hell")));
        assert_eq!(tokens.get(4), Some(&Cow::Borrowed("м")));
        assert_eq!(tokens.get(5), Some(&Cow::Borrowed("ми")));
        assert_eq!(tokens.get(6), Some(&Cow::Borrowed("мир")));
    }

    #[test]
    fn test_prefix_query_tokenizer() {
        let text = "hello, мир!";
        let config = TokenizerConfig::default();
        let mut tokens = Vec::new();
        PrefixTokenizer::tokenize_query(text, &config, 4, |token| tokens.push(token));
        eprintln!("tokens = {tokens:#?}");
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens.first(), Some(&Cow::Borrowed("hell")));
        assert_eq!(tokens.get(1), Some(&Cow::Borrowed("мир")));
    }

    #[test]
    fn test_multilingual_tokenizer_japanese() {
        let text = "本日の日付は";
        let mut config = TokenizerConfig::default();
        let mut tokens = Vec::new();
        MultilingualTokenizer::tokenize(text, &config, |token| tokens.push(token));
        eprintln!("tokens = {tokens:#?}");
        assert_eq!(tokens.len(), 4);
        assert_eq!(tokens.first(), Some(&Cow::Borrowed("本日")));
        assert_eq!(tokens.get(1), Some(&Cow::Borrowed("の")));
        assert_eq!(tokens.get(2), Some(&Cow::Borrowed("日付")));
        assert_eq!(tokens.get(3), Some(&Cow::Borrowed("は")));

        tokens.clear();

        // Test stopwords getting applied
        let filter =
            StopwordsFilter::new(&Some(StopwordsInterface::new_custom(&["の", "は"])), false);
        config.stopwords_filter = filter;
        MultilingualTokenizer::tokenize(text, &config, |token| tokens.push(token));
        eprintln!("tokens = {tokens:#?}");
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens.first(), Some(&Cow::Borrowed("本日")));
        assert_eq!(tokens.get(1), Some(&Cow::Borrowed("日付")));
    }

    #[test]
    fn test_multilingual_tokenizer_chinese() {
        let text = "今天是星期一";
        let mut config = TokenizerConfig::default();
        let mut tokens = Vec::new();
        MultilingualTokenizer::tokenize(text, &config, |token| tokens.push(token));
        eprintln!("tokens = {tokens:#?}");
        assert_eq!(tokens.len(), 4);
        assert_eq!(tokens.first(), Some(&Cow::Borrowed("今天")));
        assert_eq!(tokens.get(1), Some(&Cow::Borrowed("是")));
        assert_eq!(tokens.get(2), Some(&Cow::Borrowed("星期")));
        assert_eq!(tokens.get(3), Some(&Cow::Borrowed("一")));

        tokens.clear();

        // Test stopwords getting applied
        let filter = StopwordsFilter::new(&Some(StopwordsInterface::new_custom(&["是"])), false);
        config.stopwords_filter = filter;
        MultilingualTokenizer::tokenize(text, &config, |token| tokens.push(token));
        eprintln!("tokens = {tokens:#?}");
        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens.first(), Some(&Cow::Borrowed("今天")));
        assert_eq!(tokens.get(1), Some(&Cow::Borrowed("星期")));
        assert_eq!(tokens.get(2), Some(&Cow::Borrowed("一")));
    }

    #[test]
    fn test_multilingual_tokenizer_thai() {
        let text = "มาทำงานกันเถอะ";
        let mut tokens = Vec::new();
        let config = TokenizerConfig::default();
        MultilingualTokenizer::tokenize(text, &config, |token| tokens.push(token));
        eprintln!("tokens = {tokens:#?}");
        assert_eq!(tokens.len(), 4);
        assert_eq!(tokens.first(), Some(&Cow::Borrowed("มา")));
        assert_eq!(tokens.get(1), Some(&Cow::Borrowed("ทางาน")));
        assert_eq!(tokens.get(2), Some(&Cow::Borrowed("กน")));
        assert_eq!(tokens.get(3), Some(&Cow::Borrowed("เถอะ")));
    }

    #[test]
    fn test_multilingual_tokenizer_english() {
        let text = "What are you waiting for?";
        let config = TokenizerConfig::default();
        let mut tokens = Vec::new();
        MultilingualTokenizer::tokenize(text, &config, |token| tokens.push(token));
        eprintln!("tokens = {tokens:#?}");
        assert_eq!(tokens.len(), 5);
        assert_eq!(tokens.first(), Some(&Cow::Borrowed("what")));
        assert_eq!(tokens.get(1), Some(&Cow::Borrowed("are")));
        assert_eq!(tokens.get(2), Some(&Cow::Borrowed("you")));
        assert_eq!(tokens.get(3), Some(&Cow::Borrowed("waiting")));
        assert_eq!(tokens.get(4), Some(&Cow::Borrowed("for")));
    }

    #[test]
    fn test_tokenizer() {
        let text = "Hello, Мир!";
        let mut tokens = Vec::new();
        let params = TextIndexParams {
            r#type: TextIndexType::Text,
            tokenizer: TokenizerType::Prefix,
            min_token_len: Some(1),
            max_token_len: Some(4),
            lowercase: Some(true),
            on_disk: None,
            phrase_matching: None,
            stopwords: None,
            stemmer: None,
        };

        let tokenizer = Tokenizer::new(&params);

        tokenizer.tokenize_doc(text, |token| tokens.push(token));
        eprintln!("tokens = {tokens:#?}");
        assert_eq!(tokens.len(), 7);
        assert_eq!(tokens.first(), Some(&Cow::Borrowed("h")));
        assert_eq!(tokens.get(1), Some(&Cow::Borrowed("he")));
        assert_eq!(tokens.get(2), Some(&Cow::Borrowed("hel")));
        assert_eq!(tokens.get(3), Some(&Cow::Borrowed("hell")));
        assert_eq!(tokens.get(4), Some(&Cow::Borrowed("м")));
        assert_eq!(tokens.get(5), Some(&Cow::Borrowed("ми")));
        assert_eq!(tokens.get(6), Some(&Cow::Borrowed("мир")));
    }

    #[test]
    fn test_tokenizer_with_language_stopwords() {
        use crate::data_types::index::Language;
        let text = "The quick brown fox jumps over the lazy dog";
        let mut tokens = Vec::new();
        let params = TextIndexParams {
            r#type: TextIndexType::Text,
            tokenizer: TokenizerType::Word,
            min_token_len: None,
            max_token_len: None,
            lowercase: Some(true),
            on_disk: None,
            phrase_matching: None,
            stopwords: Some(StopwordsInterface::Language(Language::English)),
            stemmer: None,
        };

        let tokenizer = Tokenizer::new(&params);

        tokenizer.tokenize_doc(text, |token| tokens.push(token));
        eprintln!("tokens = {tokens:#?}");

        // Check that stopwords are filtered out
        assert!(!tokens.contains(&Cow::Borrowed("the")));
        assert!(!tokens.contains(&Cow::Borrowed("over")));

        // Check that non-stopwords are present
        assert!(tokens.contains(&Cow::Borrowed("quick")));
        assert!(tokens.contains(&Cow::Borrowed("brown")));
        assert!(tokens.contains(&Cow::Borrowed("fox")));
        assert!(tokens.contains(&Cow::Borrowed("jumps")));
        assert!(tokens.contains(&Cow::Borrowed("lazy")));
        assert!(tokens.contains(&Cow::Borrowed("dog")));
    }

    #[test]
    fn test_tokenizer_can_handle_apostrophes_parametrized() {
        use crate::data_types::index::TokenizerType;
        let text = "you'll be in town";
        let tokenizer_types = [
            TokenizerType::Word,
            TokenizerType::Whitespace,
            TokenizerType::Prefix,
        ];

        for &tokenizer_type in &tokenizer_types {
            let mut tokens = Vec::new();
            let params = TextIndexParams {
                r#type: TextIndexType::Text,
                tokenizer: tokenizer_type,
                min_token_len: None,
                max_token_len: None,
                lowercase: Some(true),
                on_disk: None,
                phrase_matching: None,
                stopwords: Some(StopwordsInterface::Language(Language::English)),
                stemmer: None,
            };

            let tokenizer = Tokenizer::new(&params);

            tokenizer.tokenize_doc(text, |token| tokens.push(token));

            // Check that stopwords are filtered out
            assert!(!tokens.contains(&Cow::Borrowed("you")));
            assert!(!tokens.contains(&Cow::Borrowed("ll")));
            assert!(!tokens.contains(&Cow::Borrowed("you'll")));

            // Check that non-stopwords are present
            assert!(tokens.contains(&Cow::Borrowed("town")));
        }
    }

    #[test]
    fn test_tokenizer_with_mixed_stopwords() {
        let text = "The quick brown fox jumps over the lazy dog";
        let mut tokens = Vec::new();
        use crate::data_types::index::Language;

        let params = TextIndexParams {
            r#type: TextIndexType::Text,
            tokenizer: TokenizerType::Word,
            min_token_len: None,
            max_token_len: None,
            lowercase: Some(true),
            on_disk: None,
            phrase_matching: None,
            stopwords: Some(StopwordsInterface::new_set(
                &[Language::English],
                &["quick", "fox"],
            )),
            stemmer: None,
        };

        let tokenizer = Tokenizer::new(&params);
        tokenizer.tokenize_doc(text, |token| tokens.push(token));
        eprintln!("tokens = {tokens:#?}");

        // Check that English stopwords are filtered out
        assert!(!tokens.contains(&Cow::Borrowed("the")));
        assert!(!tokens.contains(&Cow::Borrowed("over")));

        // Check that custom stopwords are filtered out
        assert!(!tokens.contains(&Cow::Borrowed("quick")));
        assert!(!tokens.contains(&Cow::Borrowed("fox")));

        // Check that non-stopwords are present
        assert!(tokens.contains(&Cow::Borrowed("brown")));
        assert!(tokens.contains(&Cow::Borrowed("jumps")));
        assert!(tokens.contains(&Cow::Borrowed("lazy")));
        assert!(tokens.contains(&Cow::Borrowed("dog")));
    }

    #[test]
    fn test_tokenizer_with_custom_stopwords_as_the_a() {
        let text = "The quick brown fox jumps over the lazy dog as a test";
        let mut tokens = Vec::new();
        let params = TextIndexParams {
            r#type: TextIndexType::Text,
            tokenizer: TokenizerType::Word,
            min_token_len: None,
            max_token_len: None,
            lowercase: Some(true),
            on_disk: None,
            phrase_matching: None,
            stopwords: Some(StopwordsInterface::new_custom(&["as", "the", "a"])),
            stemmer: None,
        };

        let tokenizer = Tokenizer::new(&params);

        tokenizer.tokenize_doc(text, |token| tokens.push(token));
        eprintln!("tokens = {tokens:#?}");

        // stopwords are filtered out
        assert!(!tokens.contains(&Cow::Borrowed("as")));
        assert!(!tokens.contains(&Cow::Borrowed("the")));
        assert!(!tokens.contains(&Cow::Borrowed("a")));

        // non-stopwords are present
        assert!(tokens.contains(&Cow::Borrowed("quick")));
        assert!(tokens.contains(&Cow::Borrowed("brown")));
        assert!(tokens.contains(&Cow::Borrowed("fox")));
        assert!(tokens.contains(&Cow::Borrowed("jumps")));
        assert!(tokens.contains(&Cow::Borrowed("over")));
        assert!(tokens.contains(&Cow::Borrowed("lazy")));
        assert!(tokens.contains(&Cow::Borrowed("dog")));
        assert!(tokens.contains(&Cow::Borrowed("test")));
    }

    #[test]
    fn test_tokenizer_with_english_stopwords_string() {
        let text = "The quick brown fox jumps over the lazy dog";
        let mut tokens = Vec::new();
        use crate::data_types::index::Language;
        let params = TextIndexParams {
            r#type: TextIndexType::Text,
            tokenizer: TokenizerType::Word,
            min_token_len: None,
            max_token_len: None,
            lowercase: Some(true),
            on_disk: None,
            phrase_matching: None,
            stopwords: Some(StopwordsInterface::Language(Language::English)),
            stemmer: None,
        };

        let tokenizer = Tokenizer::new(&params);

        tokenizer.tokenize_doc(text, |token| tokens.push(token));
        eprintln!("tokens = {tokens:#?}");

        // Check that English stopwords are filtered out
        assert!(!tokens.contains(&Cow::Borrowed("the")));
        assert!(!tokens.contains(&Cow::Borrowed("over")));

        // Check that non-stopwords are present
        assert!(tokens.contains(&Cow::Borrowed("quick")));
        assert!(tokens.contains(&Cow::Borrowed("brown")));
        assert!(tokens.contains(&Cow::Borrowed("fox")));
        assert!(tokens.contains(&Cow::Borrowed("jumps")));
        assert!(tokens.contains(&Cow::Borrowed("lazy")));
        assert!(tokens.contains(&Cow::Borrowed("dog")));
    }

    #[test]
    fn test_tokenizer_with_languages_english_spanish_custom_aaa() {
        let text = "The quick brown fox jumps over the lazy dog I'd y de";
        let mut tokens = Vec::new();
        use crate::data_types::index::Language;
        let params = TextIndexParams {
            r#type: TextIndexType::Text,
            tokenizer: TokenizerType::Word,
            min_token_len: None,
            max_token_len: None,
            lowercase: Some(true),
            on_disk: None,
            phrase_matching: None,
            stopwords: Some(StopwordsInterface::new_set(
                &[Language::English, Language::Spanish],
                &["I'd"],
            )),
            stemmer: None,
        };

        let tokenizer = Tokenizer::new(&params);

        tokenizer.tokenize_doc(text, |token| tokens.push(token));
        eprintln!("tokens = {tokens:#?}");

        // Check that English stopwords are filtered out
        assert!(!tokens.contains(&Cow::Borrowed("the")));
        assert!(!tokens.contains(&Cow::Borrowed("over")));

        // Check that Spanish stopwords are filtered out
        assert!(!tokens.contains(&Cow::Borrowed("y")));
        assert!(!tokens.contains(&Cow::Borrowed("de")));

        // Check that custom stopwords are filtered out
        assert!(!tokens.contains(&Cow::Borrowed("i'd")));

        // Check that non-stopwords are present
        assert!(tokens.contains(&Cow::Borrowed("quick")));
        assert!(tokens.contains(&Cow::Borrowed("brown")));
        assert!(tokens.contains(&Cow::Borrowed("fox")));
        assert!(tokens.contains(&Cow::Borrowed("jumps")));
        assert!(tokens.contains(&Cow::Borrowed("lazy")));
        assert!(tokens.contains(&Cow::Borrowed("dog")));
    }

    #[test]
    fn test_tokenizer_with_case_sensitive_stopwords() {
        let text = "The quick brown fox jumps over the lazy dog";
        let mut tokens = Vec::new();
        let params = TextIndexParams {
            r#type: TextIndexType::Text,
            tokenizer: TokenizerType::Word,
            min_token_len: None,
            max_token_len: None,
            lowercase: Some(false), // Case sensitivity is enabled
            on_disk: None,
            phrase_matching: None,
            stopwords: Some(StopwordsInterface::new_custom(&["the", "The", "LAZY"])),
            stemmer: None,
        };

        let tokenizer = Tokenizer::new(&params);

        tokenizer.tokenize_doc(text, |token| tokens.push(token));
        eprintln!("tokens = {tokens:#?}");

        // Check that exact case stopwords are filtered out
        assert!(!tokens.contains(&Cow::Borrowed("The")));
        assert!(!tokens.contains(&Cow::Borrowed("the")));

        // Check that different case stopwords are not filtered out
        assert!(tokens.contains(&Cow::Borrowed("lazy"))); // "LAZY" is in stopwords, but "lazy" is not

        // Check that non-stopwords are present
        assert!(tokens.contains(&Cow::Borrowed("quick")));
        assert!(tokens.contains(&Cow::Borrowed("brown")));
        assert!(tokens.contains(&Cow::Borrowed("fox")));
        assert!(tokens.contains(&Cow::Borrowed("jumps")));
        assert!(tokens.contains(&Cow::Borrowed("over")));
        assert!(tokens.contains(&Cow::Borrowed("dog")));
    }

    #[test]
    fn test_stemming_snowball() {
        let input = "interestingly proceeding living";
        let config = TokenizerConfig {
            stemmer: Some(make_stemmer(SnowballLanguage::English)),
            ..Default::default()
        };

        let mut out = Vec::new();
        WhiteSpaceTokenizer::tokenize(input, &config, |i| out.push(i.to_string()));
        assert_eq!(out, vec!["interest", "proceed", "live"]);

        out.clear();
        WordTokenizer::tokenize(input, &config, |i| out.push(i.to_string()));
        assert_eq!(out, vec!["interest", "proceed", "live"]);

        out.clear();
        PrefixTokenizer::tokenize(input, &config, 3, 4, |i| out.push(i.to_string()));
        assert_eq!(out, vec!["int", "inte", "pro", "proc", "liv", "live"]);

        out.clear();
        MultilingualTokenizer::tokenize(input, &config, |i| out.push(i.to_string()));
        assert_eq!(out, vec!["interest", "proceed", "live"]);
    }
}
