use std::borrow::Cow;
use std::sync::Arc;
mod ascii_folding;
mod japanese;
mod multilingual;
mod stemmer;
pub mod tokens_processor;

use multilingual::MultilingualTokenizer;
pub use stemmer::Stemmer;
pub use tokens_processor::TokensProcessor;

use crate::data_types::index::{TextIndexParams, TokenizerType};
use crate::index::field_index::full_text_index::stop_words::StopwordsFilter;

struct WhiteSpaceTokenizer;

impl WhiteSpaceTokenizer {
    fn tokenize<'a, C: FnMut(Cow<'a, str>)>(
        text: &'a str,
        tokens_processor: &TokensProcessor,
        mut callback: C,
    ) {
        for token in text.split_whitespace() {
            let Some(token_cow) = tokens_processor.process_token(token, true) else {
                continue;
            };

            callback(token_cow);
        }
    }
}

struct WordTokenizer;

impl WordTokenizer {
    fn tokenize<'a, C: FnMut(Cow<'a, str>)>(
        text: &'a str,
        tokens_processor: &TokensProcessor,
        mut callback: C,
    ) {
        for token in text.split(|c| !char::is_alphanumeric(c)) {
            let Some(token_cow) = tokens_processor.process_token(token, true) else {
                continue;
            };

            callback(token_cow);
        }
    }
}

struct PrefixTokenizer;

impl PrefixTokenizer {
    fn tokenize<'a, C: FnMut(Cow<'a, str>)>(
        text: &'a str,
        tokens_processor: &TokensProcessor,
        mut callback: C,
    ) {
        let min_ngram = tokens_processor.min_token_len.unwrap_or(1);
        let max_ngram = tokens_processor.max_token_len.unwrap_or(usize::MAX);

        text.split(|c| !char::is_alphanumeric(c)).for_each(|word| {
            let Some(word_cow) = tokens_processor.process_token(word, false) else {
                return;
            };

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
        tokens_processor: &TokensProcessor,
        mut callback: C,
    ) {
        let max_ngram = tokens_processor.max_token_len.unwrap_or(usize::MAX);

        text.split(|c| !char::is_alphanumeric(c))
            .filter(|token| !token.is_empty())
            .for_each(|word| {
                // Apply ASCII folding if enabled
                let mut word_cow = tokens_processor.fold_if_enabled(Cow::Borrowed(word));

                // Handle lowercase
                if tokens_processor.lowercase {
                    word_cow = Cow::Owned(word_cow.to_lowercase());
                }

                let word_cow = tokens_processor.stem_if_enabled(word_cow);

                if tokens_processor
                    .min_token_len
                    .is_some_and(|min_len| word_cow.chars().count() < min_len)
                {
                    // Tokens shorter than min_token_len don't exist in the index
                    return;
                }

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
    tokens_processor: TokensProcessor,
}

impl Tokenizer {
    pub fn new_from_text_index_params(params: &TextIndexParams) -> Self {
        let TextIndexParams {
            r#type: _,
            tokenizer,
            min_token_len,
            max_token_len,
            lowercase,
            ascii_folding,
            on_disk: _,
            phrase_matching: _,
            stopwords,
            stemmer,
        } = params;

        let lowercase = lowercase.unwrap_or(true);
        let ascii_folding = ascii_folding.unwrap_or(false);
        let stopwords_filter = Arc::new(StopwordsFilter::new(stopwords, lowercase));

        let tokens_processor = TokensProcessor::new(
            lowercase,
            ascii_folding,
            stopwords_filter,
            stemmer.as_ref().map(Stemmer::from_algorithm),
            *min_token_len,
            *max_token_len,
        );

        Self::new(*tokenizer, tokens_processor)
    }

    pub fn new(tokenizer_type: TokenizerType, tokens_processor: TokensProcessor) -> Self {
        Self {
            tokenizer_type,
            tokens_processor,
        }
    }

    pub fn tokenize_doc<'a, C: FnMut(Cow<'a, str>)>(&'a self, text: &'a str, callback: C) {
        match self.tokenizer_type {
            TokenizerType::Whitespace => {
                WhiteSpaceTokenizer::tokenize(text, &self.tokens_processor, callback)
            }
            TokenizerType::Word => WordTokenizer::tokenize(text, &self.tokens_processor, callback),
            TokenizerType::Multilingual => {
                MultilingualTokenizer::tokenize(text, &self.tokens_processor, callback)
            }
            TokenizerType::Prefix => {
                PrefixTokenizer::tokenize(text, &self.tokens_processor, callback)
            }
        }
    }

    pub fn tokenize_query<'a, C: FnMut(Cow<'a, str>)>(&'a self, text: &'a str, callback: C) {
        match self.tokenizer_type {
            TokenizerType::Whitespace => {
                WhiteSpaceTokenizer::tokenize(text, &self.tokens_processor, callback)
            }
            TokenizerType::Word => WordTokenizer::tokenize(text, &self.tokens_processor, callback),
            TokenizerType::Multilingual => {
                MultilingualTokenizer::tokenize(text, &self.tokens_processor, callback)
            }
            TokenizerType::Prefix => {
                PrefixTokenizer::tokenize_query(text, &self.tokens_processor, callback)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::default::Default;

    use itertools::Itertools;

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
        let tokens_processor = TokensProcessor::default();

        let mut tokens = Vec::new();
        WhiteSpaceTokenizer::tokenize(text, &tokens_processor, |token| tokens.push(token));

        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens.first(), Some(&Cow::Borrowed("hello")));
        assert_eq!(tokens.get(1), Some(&Cow::Borrowed("world")));
    }

    #[test]
    fn test_word_tokenizer() {
        let text = "hello, world! Привет, мир!";
        let mut tokens_processor = TokensProcessor::default();
        let mut tokens = Vec::new();
        WordTokenizer::tokenize(text, &tokens_processor, |token| tokens.push(token));
        assert_eq!(tokens.len(), 4);
        assert_eq!(tokens.first(), Some(&Cow::Borrowed("hello")));
        assert_eq!(tokens.get(1), Some(&Cow::Borrowed("world")));
        assert_eq!(tokens.get(2), Some(&Cow::Borrowed("Привет")));
        assert_eq!(tokens.get(3), Some(&Cow::Borrowed("мир")));

        tokens.clear();
        tokens_processor.lowercase = true;
        WordTokenizer::tokenize(text, &tokens_processor, |token| tokens.push(token));
        assert_eq!(tokens.len(), 4);
        assert_eq!(tokens.first(), Some(&Cow::Borrowed("hello")));
        assert_eq!(tokens.get(1), Some(&Cow::Borrowed("world")));
        assert_eq!(tokens.get(2), Some(&Cow::Borrowed("привет")));
        assert_eq!(tokens.get(3), Some(&Cow::Borrowed("мир")));
    }

    #[test]
    fn test_prefix_tokenizer() {
        let text = "hello, мир!";
        let tokens_processor =
            TokensProcessor::new(true, false, Default::default(), None, Some(1), Some(4));

        let mut tokens = Vec::new();
        PrefixTokenizer::tokenize(text, &tokens_processor, |token| tokens.push(token));
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
        let tokens_processor =
            TokensProcessor::new(true, false, Default::default(), None, None, Some(4));

        let mut tokens = Vec::new();
        PrefixTokenizer::tokenize_query(text, &tokens_processor, |token| tokens.push(token));
        eprintln!("tokens = {tokens:#?}");
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens.first(), Some(&Cow::Borrowed("hell")));
        assert_eq!(tokens.get(1), Some(&Cow::Borrowed("мир")));
    }

    #[test]
    fn test_multilingual_tokenizer_japanese() {
        let text = "本日の日付は";
        let tokens_processor = TokensProcessor::default();
        let mut tokens = Vec::new();
        MultilingualTokenizer::tokenize(text, &tokens_processor, |token| tokens.push(token));
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
        let tokens_processor =
            TokensProcessor::new(true, false, Arc::new(filter), None, None, None);
        MultilingualTokenizer::tokenize(text, &tokens_processor, |token| tokens.push(token));
        eprintln!("tokens = {tokens:#?}");
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens.first(), Some(&Cow::Borrowed("本日")));
        assert_eq!(tokens.get(1), Some(&Cow::Borrowed("日付")));
    }

    #[test]
    fn test_multilingual_tokenizer_chinese() {
        let text = "今天是星期一";
        let tokens_processor = TokensProcessor::default();
        let mut tokens = Vec::new();
        MultilingualTokenizer::tokenize(text, &tokens_processor, |token| tokens.push(token));
        eprintln!("tokens = {tokens:#?}");
        assert_eq!(tokens.len(), 4);
        assert_eq!(tokens.first(), Some(&Cow::Borrowed("今天")));
        assert_eq!(tokens.get(1), Some(&Cow::Borrowed("是")));
        assert_eq!(tokens.get(2), Some(&Cow::Borrowed("星期")));
        assert_eq!(tokens.get(3), Some(&Cow::Borrowed("一")));

        tokens.clear();

        // Test stopwords getting applied
        let filter = StopwordsFilter::new(&Some(StopwordsInterface::new_custom(&["是"])), false);
        let tokens_processor =
            TokensProcessor::new(true, false, Arc::new(filter), None, None, None);
        MultilingualTokenizer::tokenize(text, &tokens_processor, |token| tokens.push(token));
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
        let tokens_processor = TokensProcessor::default();
        MultilingualTokenizer::tokenize(text, &tokens_processor, |token| tokens.push(token));
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
        let tokens_processor = TokensProcessor::default();
        let mut tokens = Vec::new();
        MultilingualTokenizer::tokenize(text, &tokens_processor, |token| tokens.push(token));
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
            ascii_folding: None,
            on_disk: None,
            phrase_matching: None,
            stopwords: None,
            stemmer: None,
        };

        let tokenizer = Tokenizer::new_from_text_index_params(&params);

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
            ascii_folding: None,
            on_disk: None,
            phrase_matching: None,
            stopwords: Some(StopwordsInterface::Language(Language::English)),
            stemmer: None,
        };

        let tokenizer = Tokenizer::new_from_text_index_params(&params);

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
                ascii_folding: None,
                on_disk: None,
                phrase_matching: None,
                stopwords: Some(StopwordsInterface::Language(Language::English)),
                stemmer: None,
            };

            let tokenizer = Tokenizer::new_from_text_index_params(&params);

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
            ascii_folding: None,
            on_disk: None,
            phrase_matching: None,
            stopwords: Some(StopwordsInterface::new_set(
                &[Language::English],
                &["quick", "fox"],
            )),
            stemmer: None,
        };

        let tokenizer = Tokenizer::new_from_text_index_params(&params);
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
            ascii_folding: None,
            on_disk: None,
            phrase_matching: None,
            stopwords: Some(StopwordsInterface::new_custom(&["as", "the", "a"])),
            stemmer: None,
        };

        let tokenizer = Tokenizer::new_from_text_index_params(&params);

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
            ascii_folding: None,
            on_disk: None,
            phrase_matching: None,
            stopwords: Some(StopwordsInterface::Language(Language::English)),
            stemmer: None,
        };

        let tokenizer = Tokenizer::new_from_text_index_params(&params);

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
            ascii_folding: None,
            on_disk: None,
            phrase_matching: None,
            stopwords: Some(StopwordsInterface::new_set(
                &[Language::English, Language::Spanish],
                &["I'd"],
            )),
            stemmer: None,
        };

        let tokenizer = Tokenizer::new_from_text_index_params(&params);

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
            ascii_folding: None,
            on_disk: None,
            phrase_matching: None,
            stopwords: Some(StopwordsInterface::new_custom(&["the", "The", "LAZY"])),
            stemmer: None,
        };

        let tokenizer = Tokenizer::new_from_text_index_params(&params);

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
    fn test_ascii_folding_word_tokenizer_on_off() {
        let text = "ação café jalapeño Über";

        let expected_disabled = ["ação", "café", "jalapeño", "über"]
            .into_iter()
            .map(str::to_string)
            .collect_vec();
        let expected_enabled = ["acao", "cafe", "jalapeno", "uber"]
            .into_iter()
            .map(str::to_string)
            .collect_vec();

        // ascii_folding disabled (default)
        let params_disabled = TextIndexParams {
            r#type: TextIndexType::Text,
            tokenizer: TokenizerType::Word,
            min_token_len: None,
            max_token_len: None,
            lowercase: Some(true),
            ascii_folding: Some(false),
            on_disk: None,
            phrase_matching: None,
            stopwords: None,
            stemmer: None,
        };
        let tokenizer_disabled = Tokenizer::new_from_text_index_params(&params_disabled);
        let mut tokens_disabled = Vec::new();
        tokenizer_disabled.tokenize_doc(text, |token| tokens_disabled.push(token.to_string()));
        assert_eq!(tokens_disabled, expected_disabled);

        // ascii_folding enabled
        let params_enabled = TextIndexParams {
            r#type: TextIndexType::Text,
            tokenizer: TokenizerType::Word,
            min_token_len: None,
            max_token_len: None,
            lowercase: Some(true),
            ascii_folding: Some(true),
            on_disk: None,
            phrase_matching: None,
            stopwords: None,
            stemmer: None,
        };
        let tokenizer_enabled = Tokenizer::new_from_text_index_params(&params_enabled);
        let mut tokens_enabled = Vec::new();
        tokenizer_enabled.tokenize_doc(text, |token| tokens_enabled.push(token.to_string()));
        assert_eq!(tokens_enabled, expected_enabled);
    }

    #[test]
    fn test_ascii_folding_prefix_tokenizer() {
        let text = "ação";
        // With folding disabled: prefixes should preserve accents
        let tokens_processor_disabled =
            TokensProcessor::new(true, false, Default::default(), None, Some(1), Some(4));
        let mut tokens_disabled = Vec::new();
        PrefixTokenizer::tokenize(text, &tokens_processor_disabled, |t| {
            tokens_disabled.push(t.to_string())
        });
        assert!(
            tokens_disabled.contains(&"a".to_string())
                || tokens_disabled.contains(&"a".to_string())
        );
        // Because the first char is 'a', but next prefixes should include accented letters
        assert!(
            tokens_disabled.iter().any(|t| t.starts_with("aç"))
                || tokens_disabled.iter().any(|t| t.contains('ç'))
        );

        // With folding enabled: prefixes should be ASCII-only (acao, acao prefixes)
        let tokens_processor_enabled =
            TokensProcessor::new(true, true, Default::default(), None, Some(1), Some(4));
        let mut tokens_enabled = Vec::new();
        PrefixTokenizer::tokenize(text, &tokens_processor_enabled, |t| {
            tokens_enabled.push(t.to_string())
        });
        // We expect prefixes like a, ac, aca, acao
        assert!(tokens_enabled.contains(&"a".to_string()));
        assert!(tokens_enabled.contains(&"ac".to_string()));
        assert!(tokens_enabled.contains(&"aca".to_string()));
        assert!(tokens_enabled.contains(&"acao".to_string()));
        assert!(tokens_enabled.iter().all(|t| t.is_ascii()));
    }

    #[test]
    fn test_stemming_snowball() {
        let input = "interestingly proceeding living";
        let mut tokens_processor = TokensProcessor::new(
            true,
            false,
            Default::default(),
            Some(make_stemmer(SnowballLanguage::English)),
            None,
            None,
        );

        let mut out = Vec::new();
        WhiteSpaceTokenizer::tokenize(input, &tokens_processor, |i| out.push(i.to_string()));
        assert_eq!(out, vec!["interest", "proceed", "live"]);

        out.clear();
        WordTokenizer::tokenize(input, &tokens_processor, |i| out.push(i.to_string()));
        assert_eq!(out, vec!["interest", "proceed", "live"]);

        out.clear();
        MultilingualTokenizer::tokenize(input, &tokens_processor, |i| out.push(i.to_string()));
        assert_eq!(out, vec!["interest", "proceed", "live"]);

        out.clear();
        tokens_processor.min_token_len = Some(3);
        tokens_processor.max_token_len = Some(4);
        PrefixTokenizer::tokenize(input, &tokens_processor, |i| out.push(i.to_string()));
        assert_eq!(out, vec!["int", "inte", "pro", "proc", "liv", "live"]);
    }
}
