use crate::data_types::text_index::{TextIndexParams, TokenizerType};

struct WhiteSpaceTokenizer;

impl WhiteSpaceTokenizer {
    fn tokenize<C: FnMut(&str)>(text: &str, callback: C) {
        text.split_whitespace().for_each(callback);
    }
}

struct WordTokenizer;

impl WordTokenizer {
    fn tokenize<C: FnMut(&str)>(text: &str, callback: C) {
        text.split(|c| !char::is_alphanumeric(c))
            .filter(|x| !x.is_empty())
            .for_each(callback);
    }
}

struct PrefixTokenizer;

impl PrefixTokenizer {
    fn tokenize<C: FnMut(&str)>(text: &str, min_ngram: usize, max_ngram: usize, mut callback: C) {
        text.split(|c| !char::is_alphanumeric(c))
            .filter(|token| !token.is_empty())
            .for_each(|word| {
                for n in min_ngram..=max_ngram {
                    let ngram = word.char_indices().map(|(i, _)| i).nth(n);
                    match ngram {
                        Some(end) => callback(&word[..end]),
                        None => {
                            callback(word);
                            break;
                        }
                    }
                }
            });
    }

    /// For querying prefixes, it makes sense to use a maximal ngram only.
    /// E.g.
    ///
    /// Docs. tokens: "hello" -> ["he", "hel", "hell", "hello"]
    /// Query tokens: "hel"   -> ["hel"]
    /// Query tokens: "hell"  -> ["hell"]
    /// Query tokens: "hello" -> ["hello"]
    fn tokenize_query<C: FnMut(&str)>(text: &str, max_ngram: usize, mut callback: C) {
        text.split(|c| !char::is_alphanumeric(c))
            .filter(|token| !token.is_empty())
            .for_each(|word| {
                let ngram = word.char_indices().map(|(i, _)| i).nth(max_ngram);
                match ngram {
                    Some(end) => callback(&word[..end]),
                    None => {
                        callback(word);
                    }
                }
            });
    }
}

pub struct Tokenizer;

impl Tokenizer {
    fn doc_token_filter<'a, C: FnMut(&str) + 'a>(
        config: &'a TextIndexParams,
        mut callback: C,
    ) -> impl FnMut(&str) + 'a {
        move |token: &str| {
            if config
                .min_token_len
                .map(|min_len| token.len() < min_len && token.chars().count() < min_len)
                .unwrap_or(false)
            {
                return;
            }
            if config
                .max_token_len
                .map(|max_len| token.len() > max_len && token.chars().count() > max_len)
                .unwrap_or(false)
            {
                return;
            }
            if config.lowercase.unwrap_or(true) {
                callback(&token.to_lowercase());
            } else {
                callback(token);
            }
        }
    }

    pub fn tokenize_doc<C: FnMut(&str)>(text: &str, config: &TextIndexParams, mut callback: C) {
        let token_filter = Self::doc_token_filter(config, &mut callback);
        match config.tokenizer {
            TokenizerType::Whitespace => WhiteSpaceTokenizer::tokenize(text, token_filter),
            TokenizerType::Word => WordTokenizer::tokenize(text, token_filter),
            TokenizerType::Prefix => PrefixTokenizer::tokenize(
                text,
                config.min_token_len.unwrap_or(1),
                config.max_token_len.unwrap_or(usize::MAX),
                token_filter,
            ),
        }
    }

    pub fn tokenize_query<C: FnMut(&str)>(text: &str, config: &TextIndexParams, mut callback: C) {
        let token_filter = Self::doc_token_filter(config, &mut callback);
        match config.tokenizer {
            TokenizerType::Whitespace => WhiteSpaceTokenizer::tokenize(text, token_filter),
            TokenizerType::Word => WordTokenizer::tokenize(text, token_filter),
            TokenizerType::Prefix => PrefixTokenizer::tokenize_query(
                text,
                config.max_token_len.unwrap_or(usize::MAX),
                token_filter,
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_types::text_index::TextIndexType;

    #[test]
    fn test_whitespace_tokenizer() {
        let text = "hello world";
        let mut tokens = Vec::new();
        WhiteSpaceTokenizer::tokenize(text, |token| tokens.push(token.to_owned()));
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens.get(0), Some(&"hello".to_owned()));
        assert_eq!(tokens.get(1), Some(&"world".to_owned()));
    }

    #[test]
    fn test_word_tokenizer() {
        let text = "hello, world! Привет, мир!";
        let mut tokens = Vec::new();
        WordTokenizer::tokenize(text, |token| tokens.push(token.to_owned()));
        assert_eq!(tokens.len(), 4);
        assert_eq!(tokens.get(0), Some(&"hello".to_owned()));
        assert_eq!(tokens.get(1), Some(&"world".to_owned()));
        assert_eq!(tokens.get(2), Some(&"Привет".to_owned()));
        assert_eq!(tokens.get(3), Some(&"мир".to_owned()));
    }

    #[test]
    fn test_prefix_tokenizer() {
        let text = "hello, мир!";
        let mut tokens = Vec::new();
        PrefixTokenizer::tokenize(text, 1, 4, |token| tokens.push(token.to_owned()));
        eprintln!("tokens = {:#?}", tokens);
        assert_eq!(tokens.len(), 7);
        assert_eq!(tokens.get(0), Some(&"h".to_owned()));
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
        PrefixTokenizer::tokenize_query(text, 4, |token| tokens.push(token.to_owned()));
        eprintln!("tokens = {:#?}", tokens);
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens.get(0), Some(&"hell".to_owned()));
        assert_eq!(tokens.get(1), Some(&"мир".to_owned()));
    }

    #[test]
    fn test_tokenizer() {
        let text = "Hello, Мир!";
        let mut tokens = Vec::new();
        Tokenizer::tokenize_doc(
            text,
            &TextIndexParams {
                r#type: TextIndexType::Text,
                tokenizer: TokenizerType::Prefix,
                min_token_len: Some(1),
                max_token_len: Some(4),
                lowercase: Some(true),
            },
            |token| tokens.push(token.to_owned()),
        );
        eprintln!("tokens = {:#?}", tokens);
        assert_eq!(tokens.len(), 7);
        assert_eq!(tokens.get(0), Some(&"h".to_owned()));
        assert_eq!(tokens.get(1), Some(&"he".to_owned()));
        assert_eq!(tokens.get(2), Some(&"hel".to_owned()));
        assert_eq!(tokens.get(3), Some(&"hell".to_owned()));
        assert_eq!(tokens.get(4), Some(&"м".to_owned()));
        assert_eq!(tokens.get(5), Some(&"ми".to_owned()));
        assert_eq!(tokens.get(6), Some(&"мир".to_owned()));
    }
}
