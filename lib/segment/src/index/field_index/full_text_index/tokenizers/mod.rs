use std::borrow::Cow;
mod japanese;
mod multilingual;

use charabia::Tokenize;

use crate::data_types::index::{TextIndexParams, TokenizerType};
use crate::index::field_index::full_text_index::stop_words::StopwordsFilter;

struct WhiteSpaceTokenizer;

impl WhiteSpaceTokenizer {
    fn tokenize<C: FnMut(&str)>(
        text: &str,
        lowercase: bool,
        stopwords_filter: &StopwordsFilter,
        mut callback: C,
    ) {
        for token in text.split_whitespace() {
            if token.is_empty() {
                continue;
            }

            let token_cow = if lowercase {
                Cow::Owned(token.to_lowercase())
            } else {
                Cow::Borrowed(token)
            };

            if stopwords_filter.is_stopword(&token_cow) {
                continue;
            }
            callback(&token_cow);
        }
    }
}

struct WordTokenizer;

impl WordTokenizer {
    fn tokenize<C: FnMut(&str)>(
        text: &str,
        lowercase: bool,
        stopwords_filter: &StopwordsFilter,
        mut callback: C,
    ) {
        for token in text.split(|c| !char::is_alphanumeric(c)) {
            if token.is_empty() {
                continue;
            }

            let token_cow = if lowercase {
                Cow::Owned(token.to_lowercase())
            } else {
                Cow::Borrowed(token)
            };

            if stopwords_filter.is_stopword(&token_cow) {
                continue;
            }

            callback(&token_cow);
        }
    }
}

struct PrefixTokenizer;

impl PrefixTokenizer {
    fn tokenize<C: FnMut(&str)>(
        text: &str,
        lowercase: bool,
        stopwords_filter: &StopwordsFilter,
        min_ngram: usize,
        max_ngram: usize,
        mut callback: C,
    ) {
        text.split(|c| !char::is_alphanumeric(c))
            .filter(|token| !token.is_empty())
            .for_each(|word| {
                let word_cow = if lowercase {
                    Cow::Owned(word.to_lowercase())
                } else {
                    Cow::Borrowed(word)
                };

                if stopwords_filter.is_stopword(&word_cow) {
                    return;
                }

                for n in min_ngram..=max_ngram {
                    let ngram = word_cow.char_indices().map(|(i, _)| i).nth(n);
                    match ngram {
                        Some(end) => callback(&word_cow[..end]),
                        None => {
                            callback(&word_cow);
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
    fn tokenize_query<C: FnMut(&str)>(
        text: &str,
        lowercase: bool,
        max_ngram: usize,
        mut callback: C,
    ) {
        text.split(|c| !char::is_alphanumeric(c))
            .filter(|token| !token.is_empty())
            .for_each(|word| {
                let word_cow = if lowercase {
                    Cow::Owned(word.to_lowercase())
                } else {
                    Cow::Borrowed(word)
                };

                let ngram = word_cow.char_indices().map(|(i, _)| i).nth(max_ngram);
                match ngram {
                    Some(end) => callback(&word_cow[..end]),
                    None => {
                        callback(&word_cow);
                    }
                }
            });
    }
}

struct MultilingualTokenizer;

impl MultilingualTokenizer {
    fn tokenize<C: FnMut(&str)>(
        text: &str,
        lowercase: bool,
        stopwords_filter: &StopwordsFilter,
        mut callback: C,
    ) {
        text.tokenize().for_each(|token| {
            if token.is_word() {
                let lemma = if lowercase {
                    Cow::Owned(token.lemma.to_lowercase())
                } else {
                    token.lemma
                };
                if stopwords_filter.is_stopword(&lemma) {
                    return;
                }
                callback(&lemma);
            }
        });
    }
}

#[derive(Debug, Clone)]
pub struct Tokenizer {
    tokenizer_type: TokenizerType,
    lowercase: bool,
    min_token_len: Option<usize>,
    max_token_len: Option<usize>,
    stopwords_filter: StopwordsFilter,
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
        } = params;

        let lowercase = lowercase.unwrap_or(true);
        let stopwords_filter = StopwordsFilter::new(stopwords, lowercase);
        Self {
            tokenizer_type: *tokenizer,
            lowercase,
            min_token_len: *min_token_len,
            max_token_len: *max_token_len,
            stopwords_filter,
        }
    }

    fn doc_token_filter<'a, C: FnMut(&str) + 'a>(
        &'a self,
        mut callback: C,
    ) -> impl FnMut(&str) + 'a {
        move |token: &str| {
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

    pub fn tokenize_doc<C: FnMut(&str)>(&self, text: &str, mut callback: C) {
        let token_filter = self.doc_token_filter(&mut callback);
        match self.tokenizer_type {
            TokenizerType::Whitespace => WhiteSpaceTokenizer::tokenize(
                text,
                self.lowercase,
                &self.stopwords_filter,
                token_filter,
            ),
            TokenizerType::Word => {
                WordTokenizer::tokenize(text, self.lowercase, &self.stopwords_filter, token_filter)
            }
            TokenizerType::Multilingual => MultilingualTokenizer::tokenize(
                text,
                self.lowercase,
                &self.stopwords_filter,
                token_filter,
            ),
            TokenizerType::Prefix => PrefixTokenizer::tokenize(
                text,
                self.lowercase,
                &self.stopwords_filter,
                self.min_token_len.unwrap_or(1),
                self.max_token_len.unwrap_or(usize::MAX),
                token_filter,
            ),
        }
    }

    pub fn tokenize_query<C: FnMut(&str)>(&self, text: &str, mut callback: C) {
        let token_filter = self.doc_token_filter(&mut callback);
        match self.tokenizer_type {
            TokenizerType::Whitespace => WhiteSpaceTokenizer::tokenize(
                text,
                self.lowercase,
                &self.stopwords_filter,
                token_filter,
            ),
            TokenizerType::Word => {
                WordTokenizer::tokenize(text, self.lowercase, &self.stopwords_filter, token_filter)
            }
            TokenizerType::Multilingual => MultilingualTokenizer::tokenize(
                text,
                self.lowercase,
                &self.stopwords_filter,
                token_filter,
            ),
            TokenizerType::Prefix => PrefixTokenizer::tokenize_query(
                text,
                self.lowercase,
                self.max_token_len.unwrap_or(usize::MAX),
                token_filter,
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_types::index::{Language, StopwordsInterface, TextIndexType};

    #[test]
    fn test_whitespace_tokenizer() {
        let text = "hello world";
        let mut tokens = Vec::new();
        WhiteSpaceTokenizer::tokenize(text, true, &StopwordsFilter::default(), |token| {
            tokens.push(token.to_owned())
        });
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens.first(), Some(&"hello".to_owned()));
        assert_eq!(tokens.get(1), Some(&"world".to_owned()));
    }

    #[test]
    fn test_word_tokenizer() {
        let text = "hello, world! Привет, мир!";
        let mut tokens = Vec::new();
        WordTokenizer::tokenize(text, true, &StopwordsFilter::default(), |token| {
            tokens.push(token.to_owned())
        });
        assert_eq!(tokens.len(), 4);
        assert_eq!(tokens.first(), Some(&"hello".to_owned()));
        assert_eq!(tokens.get(1), Some(&"world".to_owned()));
        assert_eq!(tokens.get(2), Some(&"привет".to_owned()));
        assert_eq!(tokens.get(3), Some(&"мир".to_owned()));
    }

    #[test]
    fn test_prefix_tokenizer() {
        let text = "hello, мир!";
        let mut tokens = Vec::new();
        PrefixTokenizer::tokenize(text, true, &StopwordsFilter::default(), 1, 4, |token| {
            tokens.push(token.to_owned())
        });
        eprintln!("tokens = {tokens:#?}");
        assert_eq!(tokens.len(), 7);
        assert_eq!(tokens.first(), Some(&"h".to_owned()));
        assert_eq!(tokens.get(1), Some(&"he".to_owned()));
        assert_eq!(tokens.get(2), Some(&"hel".to_owned()));
        assert_eq!(tokens.get(3), Some(&"hell".to_owned()));
        assert_eq!(tokens.get(4), Some(&"м".to_owned()));
        assert_eq!(tokens.get(5), Some(&"ми".to_owned()));
        assert_eq!(tokens.get(6), Some(&"мир".to_owned()));
    }

    #[test]
    fn test_prefix_query_tokenizer() {
        let text = "hello, мир!";
        let mut tokens = Vec::new();
        PrefixTokenizer::tokenize_query(text, true, 4, |token| tokens.push(token.to_owned()));
        eprintln!("tokens = {tokens:#?}");
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens.first(), Some(&"hell".to_owned()));
        assert_eq!(tokens.get(1), Some(&"мир".to_owned()));
    }

    #[cfg(feature = "multiling-japanese")]
    #[test]
    fn test_multilingual_tokenizer_japanese() {
        let text = "本日の日付は";
        let mut tokens = Vec::new();
        MultilingualTokenizer::tokenize(text, true, &StopwordsFilter::default(), |token| {
            tokens.push(token.to_owned())
        });
        eprintln!("tokens = {tokens:#?}");
        assert_eq!(tokens.len(), 4);
        assert_eq!(tokens.first(), Some(&"本日".to_owned()));
        assert_eq!(tokens.get(1), Some(&"の".to_owned()));
        assert_eq!(tokens.get(2), Some(&"日付".to_owned()));
        assert_eq!(tokens.get(3), Some(&"は".to_owned()));
    }

    #[cfg(feature = "multiling-chinese")]
    #[test]
    fn test_multilingual_tokenizer_chinese() {
        let text = "今天是星期一";
        let mut tokens = Vec::new();
        MultilingualTokenizer::tokenize(text, true, &StopwordsFilter::default(), |token| {
            tokens.push(token.to_owned())
        });
        eprintln!("tokens = {tokens:#?}");
        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens.first(), Some(&"jīntiān".to_owned()));
        assert_eq!(tokens.get(1), Some(&"shì".to_owned()));
        assert_eq!(tokens.get(2), Some(&"xīngqīyī".to_owned()));
    }

    #[test]
    fn test_multilingual_tokenizer_thai() {
        let text = "มาทำงานกันเถอะ";
        let mut tokens = Vec::new();
        MultilingualTokenizer::tokenize(text, true, &StopwordsFilter::default(), |token| {
            tokens.push(token.to_owned())
        });
        eprintln!("tokens = {tokens:#?}");
        assert_eq!(tokens.len(), 4);
        assert_eq!(tokens.first(), Some(&"มา".to_owned()));
        assert_eq!(tokens.get(1), Some(&"ทางาน".to_owned()));
        assert_eq!(tokens.get(2), Some(&"กน".to_owned()));
        assert_eq!(tokens.get(3), Some(&"เถอะ".to_owned()));
    }

    #[test]
    fn test_multilingual_tokenizer_english() {
        let text = "What are you waiting for?";
        let mut tokens = Vec::new();
        MultilingualTokenizer::tokenize(text, true, &StopwordsFilter::default(), |token| {
            tokens.push(token.to_owned())
        });
        eprintln!("tokens = {tokens:#?}");
        assert_eq!(tokens.len(), 5);
        assert_eq!(tokens.first(), Some(&"what".to_owned()));
        assert_eq!(tokens.get(1), Some(&"are".to_owned()));
        assert_eq!(tokens.get(2), Some(&"you".to_owned()));
        assert_eq!(tokens.get(3), Some(&"waiting".to_owned()));
        assert_eq!(tokens.get(4), Some(&"for".to_owned()));
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
        };

        let tokenizer = Tokenizer::new(&params);

        tokenizer.tokenize_doc(text, |token| tokens.push(token.to_owned()));
        eprintln!("tokens = {tokens:#?}");
        assert_eq!(tokens.len(), 7);
        assert_eq!(tokens.first(), Some(&"h".to_owned()));
        assert_eq!(tokens.get(1), Some(&"he".to_owned()));
        assert_eq!(tokens.get(2), Some(&"hel".to_owned()));
        assert_eq!(tokens.get(3), Some(&"hell".to_owned()));
        assert_eq!(tokens.get(4), Some(&"м".to_owned()));
        assert_eq!(tokens.get(5), Some(&"ми".to_owned()));
        assert_eq!(tokens.get(6), Some(&"мир".to_owned()));
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
        };

        let tokenizer = Tokenizer::new(&params);

        tokenizer.tokenize_doc(text, |token| tokens.push(token.to_owned()));
        eprintln!("tokens = {tokens:#?}");

        // Check that stopwords are filtered out
        assert!(!tokens.contains(&"the".to_owned()));
        assert!(!tokens.contains(&"over".to_owned()));

        // Check that non-stopwords are present
        assert!(tokens.contains(&"quick".to_owned()));
        assert!(tokens.contains(&"brown".to_owned()));
        assert!(tokens.contains(&"fox".to_owned()));
        assert!(tokens.contains(&"jumps".to_owned()));
        assert!(tokens.contains(&"lazy".to_owned()));
        assert!(tokens.contains(&"dog".to_owned()));
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
            };

            let tokenizer = Tokenizer::new(&params);

            tokenizer.tokenize_doc(text, |token| tokens.push(token.to_owned()));

            // Check that stopwords are filtered out
            assert!(!tokens.contains(&"you".to_owned()));
            assert!(!tokens.contains(&"ll".to_owned()));
            assert!(!tokens.contains(&"you'll".to_owned()));

            // Check that non-stopwords are present
            assert!(tokens.contains(&"town".to_owned()));
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
        };

        let tokenizer = Tokenizer::new(&params);
        tokenizer.tokenize_doc(text, |token| tokens.push(token.to_owned()));
        eprintln!("tokens = {tokens:#?}");

        // Check that English stopwords are filtered out
        assert!(!tokens.contains(&"the".to_owned()));
        assert!(!tokens.contains(&"over".to_owned()));

        // Check that custom stopwords are filtered out
        assert!(!tokens.contains(&"quick".to_owned()));
        assert!(!tokens.contains(&"fox".to_owned()));

        // Check that non-stopwords are present
        assert!(tokens.contains(&"brown".to_owned()));
        assert!(tokens.contains(&"jumps".to_owned()));
        assert!(tokens.contains(&"lazy".to_owned()));
        assert!(tokens.contains(&"dog".to_owned()));
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
        };

        let tokenizer = Tokenizer::new(&params);

        tokenizer.tokenize_doc(text, |token| tokens.push(token.to_owned()));
        eprintln!("tokens = {tokens:#?}");

        // stopwords are filtered out
        assert!(!tokens.contains(&"as".to_owned()));
        assert!(!tokens.contains(&"the".to_owned()));
        assert!(!tokens.contains(&"a".to_owned()));

        // non-stopwords are present
        assert!(tokens.contains(&"quick".to_owned()));
        assert!(tokens.contains(&"brown".to_owned()));
        assert!(tokens.contains(&"fox".to_owned()));
        assert!(tokens.contains(&"jumps".to_owned()));
        assert!(tokens.contains(&"over".to_owned()));
        assert!(tokens.contains(&"lazy".to_owned()));
        assert!(tokens.contains(&"dog".to_owned()));
        assert!(tokens.contains(&"test".to_owned()));
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
        };

        let tokenizer = Tokenizer::new(&params);

        tokenizer.tokenize_doc(text, |token| tokens.push(token.to_owned()));
        eprintln!("tokens = {tokens:#?}");

        // Check that English stopwords are filtered out
        assert!(!tokens.contains(&"the".to_owned()));
        assert!(!tokens.contains(&"over".to_owned()));

        // Check that non-stopwords are present
        assert!(tokens.contains(&"quick".to_owned()));
        assert!(tokens.contains(&"brown".to_owned()));
        assert!(tokens.contains(&"fox".to_owned()));
        assert!(tokens.contains(&"jumps".to_owned()));
        assert!(tokens.contains(&"lazy".to_owned()));
        assert!(tokens.contains(&"dog".to_owned()));
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
        };

        let tokenizer = Tokenizer::new(&params);

        tokenizer.tokenize_doc(text, |token| tokens.push(token.to_owned()));
        eprintln!("tokens = {tokens:#?}");

        // Check that English stopwords are filtered out
        assert!(!tokens.contains(&"the".to_owned()));
        assert!(!tokens.contains(&"over".to_owned()));

        // Check that Spanish stopwords are filtered out
        assert!(!tokens.contains(&"y".to_owned()));
        assert!(!tokens.contains(&"de".to_owned()));

        // Check that custom stopwords are filtered out
        assert!(!tokens.contains(&"i'd".to_owned()));

        // Check that non-stopwords are present
        assert!(tokens.contains(&"quick".to_owned()));
        assert!(tokens.contains(&"brown".to_owned()));
        assert!(tokens.contains(&"fox".to_owned()));
        assert!(tokens.contains(&"jumps".to_owned()));
        assert!(tokens.contains(&"lazy".to_owned()));
        assert!(tokens.contains(&"dog".to_owned()));
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
        };

        let tokenizer = Tokenizer::new(&params);

        tokenizer.tokenize_doc(text, |token| tokens.push(token.to_owned()));
        eprintln!("tokens = {tokens:#?}");

        // Check that exact case stopwords are filtered out
        assert!(!tokens.contains(&"The".to_owned()));
        assert!(!tokens.contains(&"the".to_owned()));

        // Check that different case stopwords are not filtered out
        assert!(tokens.contains(&"lazy".to_owned())); // "LAZY" is in stopwords, but "lazy" is not

        // Check that non-stopwords are present
        assert!(tokens.contains(&"quick".to_owned()));
        assert!(tokens.contains(&"brown".to_owned()));
        assert!(tokens.contains(&"fox".to_owned()));
        assert!(tokens.contains(&"jumps".to_owned()));
        assert!(tokens.contains(&"over".to_owned()));
        assert!(tokens.contains(&"dog".to_owned()));
    }
}
