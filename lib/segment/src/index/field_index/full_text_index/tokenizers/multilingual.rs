use std::borrow::Cow;

use charabia::normalizer::{ClassifierOption, NormalizedTokenIter, NormalizerOption};
use charabia::{Language, Script, Segment, StrDetection};

use super::{TokensProcessor, japanese};

/// Default normalizer options from charabia(https://github.com/meilisearch/charabia/blob/main/charabia/src/normalizer/mod.rs#L82) used
/// in `str::tokenize()`.
const DEFAULT_NORMALIZER: NormalizerOption = NormalizerOption {
    create_char_map: false,
    lossy: true,
    classifier: ClassifierOption {
        stop_words: None,
        separators: None,
    },
};

pub struct MultilingualTokenizer;

impl MultilingualTokenizer {
    pub fn tokenize<'a, C: FnMut(Cow<'a, str>)>(
        input: &'a str,
        config: &'a TokensProcessor,
        cb: C,
    ) {
        let script = detect_script_of_language(input);

        // If the script of the input is latin and we don't need to stem early, tokenize as-is.
        // This skips language detection, reduces overhead, and improves performance.
        if script_is_latin(script) {
            Self::tokenize_charabia(input, config, cb);
            return;
        }

        // If the script of the input is Japanese, use vaporetto to segment.
        if detect_language(input) == Some(Language::Jpn) {
            japanese::tokenize(input, config, cb);
            return;
        }

        Self::tokenize_charabia(input, config, cb);
    }

    // Tokenize input using charabia. Automatically applies stemming and filters stopwords if configured.
    fn tokenize_charabia<'a, C>(input: &'a str, tokens_processor: &'a TokensProcessor, mut cb: C)
    where
        C: FnMut(Cow<'a, str>),
    {
        for token in charabia_token_iter(input) {
            let lemma = token.lemma;

            if lemma.chars().all(|char| !char.is_alphabetic()) {
                // Skip tokens that are not alphanumeric.
                continue;
            }

            if let Some(processed_token) = tokens_processor.process_token_cow(lemma, true) {
                cb(processed_token);
            }
        }
    }
}

// Tokenize::tokenize() function from charabia unrolled due to lifetime issues
// when using .tokenize() on a `str` directly.
fn charabia_token_iter(inp: &str) -> NormalizedTokenIter<'_, '_, '_, '_> {
    inp.segment().normalize(&DEFAULT_NORMALIZER)
}

// Detect the script of the given input using charabia.
fn detect_script_of_language(input: &str) -> Script {
    StrDetection::new(input, Some(SUPPORTED_LANGUAGES)).script()
}

// Detect the script of the given input using charabia.
fn detect_language(input: &str) -> Option<charabia::Language> {
    StrDetection::new(input, Some(SUPPORTED_LANGUAGES)).language()
}

/// Returns `true` if the given `script` is latin.
#[inline]
fn script_is_latin(script: Script) -> bool {
    matches!(script, Script::Latin)
}

/// Languages that are supported by rust-stemmers and thus should be used in language detection white-list.
/// Also includes Languages that we manually need to check against, such as Japanese and Chinese.
const SUPPORTED_LANGUAGES: &[charabia::Language] = &[
    charabia::Language::Eng,
    charabia::Language::Rus,
    charabia::Language::Por,
    charabia::Language::Ita,
    charabia::Language::Deu,
    charabia::Language::Ara,
    charabia::Language::Dan,
    charabia::Language::Swe,
    charabia::Language::Fin,
    charabia::Language::Tur,
    charabia::Language::Nld,
    charabia::Language::Hun,
    charabia::Language::Ell,
    charabia::Language::Tam,
    charabia::Language::Ron,
    charabia::Language::Cmn,
    charabia::Language::Jpn,
];

#[cfg(test)]
mod test {
    use charabia::Language;

    use super::*;
    use crate::data_types::index::{SnowballLanguage, SnowballParams, StemmingAlgorithm};
    use crate::index::field_index::full_text_index::tokenizers::stemmer::Stemmer;

    #[test]
    fn test_lang_detection() {
        // Japanese
        let input = "日本語のテキストです。Qdrantのコードで単体テストで使用されています。";
        assert_eq!(detect_language(input), Some(Language::Jpn));

        let input = "This is english text. It's being used within Qdrant's code in a unit test.";
        assert_eq!(detect_language(input), Some(Language::Eng));

        let input =
            "Das ist ein deutscher Text. Er wird in Qdrants code in einem unit Test benutzt."; // codespell:ignore ist
        assert_eq!(detect_language(input), Some(Language::Deu));

        // Chinese traditional
        let input = "這是一段德文文本。它用於 Qdrant 程式碼的單元測試中。";
        assert_eq!(detect_language(input), Some(Language::Cmn));

        // Chinese simplified
        let input = "这是一段德语文本。它用于 Qdrant 代码的单元测试中。";
        assert_eq!(detect_language(input), Some(Language::Cmn));
    }

    #[test]
    fn test_script_detection() {
        let input = "日本語のテキストです。Qdrantのコードで単体テストで使用されています。";
        assert!(!script_is_latin(detect_script_of_language(input)));

        let input = "This is english text. It's being used within Qdrant's code in a unit test.";
        assert!(script_is_latin(detect_script_of_language(input)));

        let input =
            "Das ist ein deutscher Text. Er wird in Qdrants code in einem unit Test benutzt."; // codespell:ignore ist
        assert!(script_is_latin(detect_script_of_language(input)));
    }

    fn assert_tokenization(inp: &str, expected: &str) {
        let tokens_processor = TokensProcessor::default();

        let mut out = vec![];
        MultilingualTokenizer::tokenize(inp, &tokens_processor, |i| out.push(i.to_string()));
        let expected: Vec<_> = expected.split('|').collect();
        for i in out.iter().zip(expected.iter()) {
            assert_eq!(i.0, i.1);
        }
        assert_eq!(out, expected)
    }

    #[test]
    fn test_multilingual_tokenization() {
        assert_tokenization("This is a test", "this|is|a|test");
        assert_tokenization(
            "This is english text. It's being used within Qdrant's code in a unit test.",
            "this|is|english|text|it|s|being|used|within|qdrant|s|code|in|a|unit|test",
        );

        assert_tokenization("Dies ist ein Test", "dies|ist|ein|test"); // codespell:ignore ist
        assert_tokenization("これはテストです", "これ|は|テスト|です");
        assert_tokenization(
            "日本語のテキストです。Qdrantのコードで単体テストで使用されています。",
            "日本|語|の|テキスト|です|Qdrant|の|コード|で|単体|テスト|で|使用|さ|れ|て|い|ます",
        );
    }

    #[test]
    fn test_multilingual_stemming() {
        let tokens_processor = TokensProcessor::new(
            true,
            false,
            Default::default(),
            Some(Stemmer::from_algorithm(&StemmingAlgorithm::Snowball(
                SnowballParams {
                    r#type: Default::default(),
                    language: SnowballLanguage::English,
                },
            ))),
            None,
            None,
        );

        let input = "Testing this";
        let mut out = vec![];
        MultilingualTokenizer::tokenize(input, &tokens_processor, |i| out.push(i.to_string()));
        assert_eq!(out, vec!["test", "this"]);
    }
}
