use std::borrow::Cow;
use std::sync::LazyLock;

use vaporetto::{Model, Predictor, Sentence};

use super::TokensProcessor;

/// Vaporetto prediction model. Source: https://github.com/daac-tools/vaporetto-models/releases/tag/v0.5.0
const MODEL: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tokenizer/bccwj-suw_c1.0.model"
));

/// Sha512 checksum of the model to ensure integrity and make modifications or corrupt model file easier to detect.
#[cfg(test)]
const MODEL_CHECKSUM: [u8; 64] = [
    34, 108, 156, 130, 7, 199, 31, 24, 147, 156, 119, 202, 98, 129, 109, 101, 114, 8, 250, 182,
    159, 28, 112, 122, 214, 50, 51, 191, 118, 112, 143, 237, 70, 15, 96, 45, 78, 76, 90, 62, 178,
    14, 86, 194, 87, 33, 19, 79, 55, 50, 212, 99, 98, 65, 102, 171, 123, 150, 110, 229, 88, 224,
    43, 203,
];

// Global initialization of the Japanese tokenizer.
static GLOBAL_JAPANESE_TOKENIZER: LazyLock<JapaneseTokenizer> =
    LazyLock::new(JapaneseTokenizer::init);

/// Tokenizer for Japanese text using vaporetto tokenizer.
struct JapaneseTokenizer {
    predictor: Predictor,
}

impl JapaneseTokenizer {
    /// Initializes a new `JapaneseTokenizer`. Should only called once and then kept allocated somewhere for efficient reuse.
    fn init() -> Self {
        let model = Model::read_slice(MODEL).unwrap().0;
        let predictor = Predictor::new(model, false).unwrap();
        Self { predictor }
    }

    fn tokenize<'a, C: FnMut(Cow<'a, str>)>(
        &self,
        input: &'a str,
        tokens_processor: &TokensProcessor,
        mut cb: C,
    ) {
        let Ok(mut s) = Sentence::from_raw(Cow::Borrowed(input)) else {
            return;
        };

        self.predictor.predict(&mut s);

        // TODO(multilingual): Implement similar method to `iter_tokens()` that allows returning borrowed Cows instead of needlessly cloning here.
        for i in s.iter_tokens() {
            // Clone up front: `iter_tokens()` borrows from `s`, so the surface
            // cannot outlive this loop iteration.
            let surface: Cow<'a, str> = Cow::Owned(i.surface().to_string());

            // Skip if all characters are not alphanumeric.
            if surface.chars().all(|char| !char.is_alphabetic()) {
                continue;
            }

            // Run the same pipeline as the charabia branch, so that
            // `ascii_folding`, `lowercase`, `stopwords`, `stemmer` and the
            // `min_token_len` / `max_token_len` bounds all apply. Checking
            // stopwords here instead would compare them against the raw
            // surface, before lowercasing, so a capitalised stopword would
            // survive.
            if let Some(surface) = tokens_processor.process_token_cow(surface, true) {
                cb(surface);
            }
        }
    }
}

/// Tokenizes the given `input` of Japanese text and calls `cb` with each tokens.
pub fn tokenize<'a, C: FnMut(Cow<'a, str>)>(input: &'a str, config: &TokensProcessor, cb: C) {
    GLOBAL_JAPANESE_TOKENIZER.tokenize(input, config, cb);
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use sha2::{Digest, Sha512};

    use super::super::TokensProcessor;
    use super::*;
    use crate::data_types::index::StopwordsInterface;
    use crate::index::field_index::full_text_index::stop_words::StopwordsFilter;

    #[test]
    fn test_assert_model_integrity() {
        let mut sha512 = Sha512::new();
        sha512.update(MODEL);
        let sum = sha512.finalize();

        assert!(
            sum.as_ref() == MODEL_CHECKSUM,
            "Japanese Tokenizer Model integrity check failed! The file might be modified or corrupted."
        );

        // The init() function is completely deterministic, since the model that gets loaded is included in
        // qdrant binary file. We test initialization here once to ensure it won't panic on runtime (eg. when a model has changed).
        let _ = JapaneseTokenizer::init();
    }

    #[test]
    fn test_tokenization() {
        let input = "日本語のテキストです。Qdrantのコードで単体テストで使用されています。";
        let tokens_processor = TokensProcessor::default();
        let mut out = vec![];
        tokenize(input, &tokens_processor, |i| {
            out.push(i.to_string());
        });
        assert_eq!(
            out,
            vec![
                "日本",
                "語",
                "の",
                "テキスト",
                "です",
                "Qdrant",
                "の",
                "コード",
                "で",
                "単体",
                "テスト",
                "で",
                "使用",
                "さ",
                "れ",
                "て",
                "い",
                "ます",
            ]
        );
    }

    #[test]
    fn test_tokenization_partially_japanese() {
        let input = "日本語のテキストです。It's used in Qdrant's code in a unit test";
        let tokens_processor = TokensProcessor::default();
        let mut out = vec![];
        tokenize(input, &tokens_processor, |i| {
            out.push(i.to_string());
        });
        assert_eq!(
            out,
            vec![
                "日本",
                "語",
                "の",
                "テキスト",
                "です",
                "It",
                "s",
                "used",
                "in",
                "Qdrant",
                "s",
                "code",
                "in",
                "a",
                "unit",
                "test"
            ]
        );
    }

    /// The japanese branch must run the same pipeline as the charabia branch.
    ///
    /// It used to post-process tokens inline instead of going through
    /// `TokensProcessor`, which dropped `min_token_len`, `max_token_len`,
    /// `stemmer` and `ascii_folding` entirely, and matched stopwords against
    /// the raw surface before lowercasing it.
    #[test]
    fn test_min_token_len_is_applied() {
        // Vaporetto segments this into multi-character words plus the
        // single-character particles "語" and "の".
        let input = "日本語のテキストです。";
        let tokens_processor = TokensProcessor::new(
            true,
            false,
            Arc::new(StopwordsFilter::new(&None, true)),
            None,
            Some(2),
            None,
        );
        let mut out = vec![];
        tokenize(input, &tokens_processor, |i| {
            out.push(i.to_string());
        });

        assert!(
            !out.is_empty(),
            "sanity: the japanese branch must still emit tokens, got {out:?}"
        );
        assert!(
            out.iter().all(|token| token.chars().count() >= 2),
            "min_token_len = 2 must drop shorter tokens, got {out:?}"
        );
    }

    /// `StopwordsFilter` stores its entries lowercased when `lowercase` is on
    /// but does not lowercase the token it is asked about, so every caller has
    /// to lowercase first. The japanese branch checked stopwords before
    /// lowercasing, so a capitalised stopword reached the index.
    #[test]
    fn test_stopwords_are_matched_case_insensitively() {
        // This input is already exercised by
        // `test_tokenization_partially_japanese`, so it is known to take the
        // japanese branch. It contains "It's", which segments to "It".
        let input = "日本語のテキストです。It's used in Qdrant's code in a unit test";
        let tokens_processor = TokensProcessor::new(
            true,
            false,
            Arc::new(StopwordsFilter::new(
                &Some(StopwordsInterface::new_custom(&["it"])),
                true,
            )),
            None,
            None,
            None,
        );
        let mut out = vec![];
        tokenize(input, &tokens_processor, |i| {
            out.push(i.to_string());
        });

        assert!(
            !out.iter().any(|token| token == "It" || token == "it"),
            "the lowercase stopword \"it\" must drop the capitalised token \"It\", got {out:?}"
        );
    }

    /// `max_token_len` is part of the same contract, and was skipped too.
    #[test]
    fn test_max_token_len_is_applied() {
        let input = "日本語のテキストです。";
        let tokens_processor = TokensProcessor::new(
            true,
            false,
            Arc::new(StopwordsFilter::new(&None, true)),
            None,
            None,
            Some(2),
        );
        let mut out = vec![];
        tokenize(input, &tokens_processor, |i| {
            out.push(i.to_string());
        });

        assert!(
            !out.is_empty(),
            "sanity: the japanese branch must still emit tokens, got {out:?}"
        );
        assert!(
            out.iter().all(|token| token.chars().count() <= 2),
            "max_token_len = 2 must drop longer tokens, got {out:?}"
        );
    }
}
