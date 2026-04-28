//! Whitespace tokenizer with optional lowercasing. Enabled by the
//! `basic-tokenizer` feature.
//!
//! Provided for quick-start and tests — production users will normally bring
//! a richer pipeline (stemming, stopwords, language-aware splitting).

use std::borrow::Cow;

use crate::Tokenizer;

#[derive(Debug, Clone, Copy, Default)]
pub struct BasicTokenizer {
    pub lowercase: bool,
}

impl BasicTokenizer {
    pub fn new() -> Self {
        Self { lowercase: true }
    }

    pub fn case_sensitive() -> Self {
        Self { lowercase: false }
    }
}

impl Tokenizer for BasicTokenizer {
    fn tokenize<'a>(&'a self, input: &'a str, out: &mut dyn FnMut(Cow<'a, str>)) {
        for tok in input.split_whitespace() {
            if self.lowercase && tok.chars().any(char::is_uppercase) {
                out(Cow::Owned(tok.to_lowercase()));
            } else {
                out(Cow::Borrowed(tok));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect<'a>(t: &'a impl Tokenizer, s: &'a str) -> Vec<String> {
        let mut out = Vec::new();
        t.tokenize(s, &mut |c| out.push(c.into_owned()));
        out
    }

    #[test]
    fn lowercase_default() {
        let t = BasicTokenizer::new();
        assert_eq!(collect(&t, "Foo BAR baz"), vec!["foo", "bar", "baz"]);
    }

    #[test]
    fn case_sensitive_keeps_original() {
        let t = BasicTokenizer::case_sensitive();
        assert_eq!(collect(&t, "Foo BAR"), vec!["Foo", "BAR"]);
    }

    #[test]
    fn whitespace_splits_on_any_whitespace() {
        let t = BasicTokenizer::case_sensitive();
        assert_eq!(collect(&t, "a\tb\nc  d"), vec!["a", "b", "c", "d"]);
    }
}
