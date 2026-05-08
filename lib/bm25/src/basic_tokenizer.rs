//! Whitespace tokenizer with optional lowercasing. Enabled by the
//! `basic-tokenizer` feature.
//!
//! Provided for quick-start and tests — production users will normally bring
//! a richer pipeline (stemming, stopwords, language-aware splitting).

use std::borrow::Cow;

#[derive(Debug, Clone, Copy)]
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

    /// Split `input` on whitespace, optionally lowercasing tokens. Borrows from
    /// `input` when no transformation is needed; allocates only when lowercasing.
    pub fn tokenize<'a>(&self, input: &'a str) -> Vec<Cow<'a, str>> {
        input
            .split_whitespace()
            .map(|tok| {
                if self.lowercase && tok.chars().any(char::is_uppercase) {
                    Cow::Owned(tok.to_lowercase())
                } else {
                    Cow::Borrowed(tok)
                }
            })
            .collect()
    }
}

impl Default for BasicTokenizer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect(t: &BasicTokenizer, s: &str) -> Vec<String> {
        t.tokenize(s).into_iter().map(Cow::into_owned).collect()
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
