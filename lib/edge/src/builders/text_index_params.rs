//! Fluent builder for [`TextIndexParams`].
//!
//! Builder fields mirror [`TextIndexParams`] explicitly so adding a field
//! to the target struct forces a compile error here.

use segment::data_types::index::{
    StemmingAlgorithm, StopwordsInterface, TextIndexParams, TextIndexType, TextScoringParams,
    TokenizerType,
};
use segment::types::Memory;

/// Fluent builder for [`TextIndexParams`].
///
/// All fields are optional; fields left unset take the index defaults.
#[derive(Debug, Clone, Default)]
pub struct TextIndexParamsBuilder {
    tokenizer: TokenizerType,
    min_token_len: Option<usize>,
    max_token_len: Option<usize>,
    lowercase: Option<bool>,
    ascii_folding: Option<bool>,
    phrase_matching: Option<bool>,
    stopwords: Option<StopwordsInterface>,
    on_disk: Option<bool>,
    memory: Option<Memory>,
    stemmer: Option<StemmingAlgorithm>,
    enable_hnsw: Option<bool>,
    scoring: Option<TextScoringParams>,
}

impl TextIndexParamsBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn tokenizer(mut self, tokenizer: TokenizerType) -> Self {
        self.tokenizer = tokenizer;
        self
    }

    /// Minimum characters to be tokenized.
    pub fn min_token_len(mut self, min_token_len: usize) -> Self {
        self.min_token_len = Some(min_token_len);
        self
    }

    /// Maximum characters to be tokenized.
    pub fn max_token_len(mut self, max_token_len: usize) -> Self {
        self.max_token_len = Some(max_token_len);
        self
    }

    /// If `true`, lowercase all tokens. Default: `true`.
    pub fn lowercase(mut self, lowercase: bool) -> Self {
        self.lowercase = Some(lowercase);
        self
    }

    /// If `true`, fold accented characters to ASCII. Default: `false`.
    pub fn ascii_folding(mut self, ascii_folding: bool) -> Self {
        self.ascii_folding = Some(ascii_folding);
        self
    }

    /// If `true`, support phrase matching. Default: `false`.
    pub fn phrase_matching(mut self, phrase_matching: bool) -> Self {
        self.phrase_matching = Some(phrase_matching);
        self
    }

    /// Tokens to ignore: a predefined list per language, and/or a custom set.
    pub fn stopwords(mut self, stopwords: StopwordsInterface) -> Self {
        self.stopwords = Some(stopwords);
        self
    }

    /// Deprecated: use [`Self::memory`] instead.
    /// If `true`, store the index on disk.
    #[deprecated(since = "1.19.0", note = "Use `memory` instead")]
    pub fn on_disk(mut self, on_disk: bool) -> Self {
        self.on_disk = Some(on_disk);
        self
    }

    /// Memory placement of the index. Overrides the deprecated `on_disk` flag
    /// if both are set.
    pub fn memory(mut self, memory: Memory) -> Self {
        self.memory = Some(memory);
        self
    }

    /// Stemming algorithm. [`StemmingAlgorithm::Disabled`] opts out of the
    /// language's default stemmer.
    pub fn stemmer(mut self, stemmer: StemmingAlgorithm) -> Self {
        self.stemmer = Some(stemmer);
        self
    }

    /// Build additional HNSW links for this payload field. Default: `true`.
    pub fn enable_hnsw(mut self, enable_hnsw: bool) -> Self {
        self.enable_hnsw = Some(enable_hnsw);
        self
    }

    /// Enable ranking points by BM25 over this field. Implies phrase matching.
    /// Default: disabled.
    pub fn scoring(mut self, scoring: TextScoringParams) -> Self {
        self.scoring = Some(scoring);
        self
    }

    #[allow(deprecated)]
    pub fn build(self) -> TextIndexParams {
        // Exhaustively destructure Self and construct TextIndexParams:
        // adding a field to either type forces a compile error here.
        let Self {
            tokenizer,
            min_token_len,
            max_token_len,
            lowercase,
            ascii_folding,
            phrase_matching,
            stopwords,
            on_disk,
            memory,
            stemmer,
            enable_hnsw,
            scoring,
        } = self;
        TextIndexParams {
            r#type: TextIndexType::Text,
            tokenizer,
            min_token_len,
            max_token_len,
            lowercase,
            ascii_folding,
            phrase_matching,
            stopwords,
            on_disk,
            memory,
            stemmer,
            enable_hnsw,
            scoring,
        }
    }
}
