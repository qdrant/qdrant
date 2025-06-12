use std::borrow::Cow;

use vaporetto::{Model, Predictor, Sentence};

/// Vaporetto prediction model. Source: https://github.com/daac-tools/vaporetto-models/release
const MODEL: &[u8] = include_bytes!("../../../../../../../tokenizer/bccwj-suw_c1.0.model");

/// Sha512 checksum of the model to ensure integrity and make modifications or corrupt model file easier to detect.
#[cfg(test)]
const MODEL_CHECKSUM: [u8; 64] = [
    34, 108, 156, 130, 7, 199, 31, 24, 147, 156, 119, 202, 98, 129, 109, 101, 114, 8, 250, 182,
    159, 28, 112, 122, 214, 50, 51, 191, 118, 112, 143, 237, 70, 15, 96, 45, 78, 76, 90, 62, 178,
    14, 86, 194, 87, 33, 19, 79, 55, 50, 212, 99, 98, 65, 102, 171, 123, 150, 110, 229, 88, 224,
    43, 203,
];

lazy_static::lazy_static! {
    // Global initialization of the Japanese tokenizer.
    static ref GLOBAL_JAPANESE_TOKENIZER: JapaneseTokenizer = JapaneseTokenizer::init();
}

/// Tokenizer for Japanese text using vaporetto tokenizer.
struct JapaneseTokenizer {
    predictor: Predictor,
}

impl JapaneseTokenizer {
    /// Initializes a new `JapaneseTokenizer`. Should only called once and then kept allocated somewhere for efficient reuse.
    pub fn init() -> Self {
        let model = Model::read_slice(MODEL).unwrap().0;
        let predictor = Predictor::new(model, false).unwrap();
        Self { predictor }
    }

    fn tokenize<C: FnMut(Cow<str>)>(&self, input: &str, mut cb: C) {
        let Ok(mut s) = Sentence::from_raw(Cow::Borrowed(input)) else {
            return;
        };

        self.predictor.predict(&mut s);

        for i in s.iter_tokens() {
            let surface = i.surface();

            // Skip whitespaces of non-japanese text that could be part of input.
            if surface.split_ascii_whitespace().next().is_none() {
                continue;
            }

            cb(Cow::Borrowed(surface));
        }
    }
}

/// Tokenizes the given `input` of Japanese text and calls `cb` with each tokens.
pub fn tokenize<C: FnMut(Cow<str>)>(input: &str, cb: C) {
    GLOBAL_JAPANESE_TOKENIZER.tokenize(input, cb);
}

#[cfg(test)]
mod test {
    use std::io::Write;

    use sha2::{Digest, Sha512};

    use super::*;

    #[test]
    fn test_assert_model_integrity() {
        // Sha512 checksum of model
        let mut sha512 = Sha512::new();
        sha512.write_all(MODEL).unwrap();
        let sum = sha512.finalize();

        assert!(
            sum.as_ref() == MODEL_CHECKSUM,
            "Japanese Tokenizer Model integrity check failed! The file might be modified or corrupted."
        );
    }

    #[test]
    fn test_tokenization() {
        let input = "日本語のテキストです。Qdrantのコードで単体テストで使用されています。";
        let mut out = vec![];
        tokenize(input, |i| {
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
                "。",
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
                "。"
            ]
        );
    }

    #[test]
    fn test_tokenization_partially_japanese() {
        let input = "日本語のテキストです。It's used in Qdrant's code in a unit test";
        let mut out = vec![];
        tokenize(input, |i| {
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
                "。",
                "It",
                "'",
                "s",
                "used",
                "in",
                "Qdrant",
                "'",
                "s",
                "code",
                "in",
                "a",
                "unit",
                "test"
            ]
        );
    }
}
