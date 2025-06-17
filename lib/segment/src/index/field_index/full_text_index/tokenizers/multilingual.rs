// TODO(multilingual): Remove and ensure all is used or removed.
#![allow(dead_code)]

use std::borrow::Cow;

use charabia::{Language, Script, StrDetection, Tokenize};
use rust_stemmers::{Algorithm, Stemmer};

use super::japanese;
use crate::data_types::index::TextIndexParams;

pub struct MultilingualV2;

impl MultilingualV2 {
    pub fn tokenize<C: FnMut(Cow<str>)>(input: &str, _config: &TextIndexParams, cb: C) {
        let script = detect_script_of_language(input);

        // TODO(multilingual): Replace this with a value from `config`!
        let stem = false;

        // If the script of the input is latin and we don't need to stem early, tokenize as-is.
        // This skips language detection, reduces overhead, and improves performance.
        if script_is_latin(script) && !stem {
            Self::tokenize_charabia(input, cb);
            return;
        }

        // Detect language to know if we're dealing with Japanese text, or what stemming algorithm to apply.
        let language = detect_language(input);

        // If the script of the input is Japanese, use vaporetto to segment.
        if language == Some(Language::Jpn) {
            japanese::tokenize(input, cb);
            return;
        }

        // If language detected, stemming is enabled and available.
        if let Some(algo) = language.and_then(lang_to_algorithm) {
            if stem {
                Self::tokenize_charabia_and_stem(input, algo, cb);
                return;
            }
        }

        // Fallback for other (asian) languages, such as Chinese or Thai.
        Self::tokenize_charabia(input, cb);
    }

    // Tokenize input using charabia and stem using rust-stemmers.
    fn tokenize_charabia_and_stem<C>(input: &str, stemming_algo: Algorithm, mut cb: C)
    where
        C: FnMut(Cow<str>),
    {
        let stemmer = Stemmer::create(stemming_algo);
        for token in input.tokenize() {
            let base = stemmer.stem_cow(token.lemma);
            
            // Prevent whitespaces and punctuation treated as separate tokens in the output.
            if base.chars().all(|char| !char.is_alphabetic()) {
                continue;
            }

            cb(base)
        }
    }

    // Tokenize input using charabia without applying stemming.
    fn tokenize_charabia<C: FnMut(Cow<str>)>(input: &str, mut cb: C) {
        for token in input.tokenize() {
            let lemma = token.lemma;

            // Prevent whitespaces and punctuation treated as separate tokens in the output.
            if lemma.chars().all(|char| !char.is_alphabetic()) {
                continue;
            }

            cb(lemma)
        }
    }
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

fn lang_to_algorithm(lang: charabia::Language) -> Option<Algorithm> {
    // Source: https://github.com/greyblake/whatlang-rs/blob/master/SUPPORTED_LANGUAGES.md
    let algo = match lang {
        charabia::Language::Eng => Algorithm::English,
        charabia::Language::Rus => Algorithm::Russian,
        charabia::Language::Por => Algorithm::Portuguese,
        charabia::Language::Ita => Algorithm::Italian,
        charabia::Language::Deu => Algorithm::German,
        charabia::Language::Ara => Algorithm::Arabic,
        charabia::Language::Dan => Algorithm::Danish,
        charabia::Language::Swe => Algorithm::Swedish,
        charabia::Language::Fin => Algorithm::Finnish,
        charabia::Language::Tur => Algorithm::Turkish,
        charabia::Language::Nld => Algorithm::Dutch,
        charabia::Language::Hun => Algorithm::Hungarian,
        charabia::Language::Ell => Algorithm::Greek,
        charabia::Language::Tam => Algorithm::Tamil,
        charabia::Language::Ron => Algorithm::Romanian,
        _ => return None,
    };
    Some(algo)
}

#[cfg(test)]
mod test {
    use charabia::Language;

    use super::*;

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
        let default_params = TextIndexParams::default();
        let mut out = vec![];
        MultilingualV2::tokenize(inp, &default_params, |i| out.push(i.to_string()));
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
}
