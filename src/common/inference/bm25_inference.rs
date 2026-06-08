use std::borrow::Cow;
use std::str::FromStr;
use std::sync::Arc;

use api::rest::{Bm25Config, TextPreprocessingConfig};
use bm25::{Bm25 as Bm25Core, Bm25Params, SparseEmbedding};
use collection::operations::point_ops::VectorPersisted;
use segment::data_types::index::{Language, StopwordsInterface};
use segment::index::field_index::full_text_index::stop_words::StopwordsFilter;
use segment::index::field_index::full_text_index::tokenizers::{
    Stemmer, Tokenizer, TokensProcessor,
};
use storage::content_manager::errors::StorageError;

const DEFAULT_LANGUAGE: &str = "english";

/// Adapter: builds a `bm25::Bm25` from the REST/gRPC `Bm25Config` and runs it
/// over `segment`'s text-preprocessing pipeline. Returns qdrant's `VectorPersisted`.
#[derive(Debug)]
pub struct Bm25 {
    bm25: Bm25Core,
    tokenizer: Tokenizer,
}

impl Bm25 {
    pub fn new(mut config: Bm25Config) -> Result<Self, StorageError> {
        let preproc = std::mem::take(&mut config.text_preprocessing_config);
        let processor = build_tokens_processor(preproc);
        let tokenizer = Tokenizer::new(config.tokenizer, processor);

        let params = Bm25Params {
            k1: config.k.into_inner(),
            b: config.b.into_inner(),
            avg_doc_len: config.avg_len.into_inner(),
        };

        let bm25 = Bm25Core::new(params).map_err(|e| StorageError::bad_input(e.to_string()))?;
        Ok(Self { bm25, tokenizer })
    }

    pub fn search_embed(&self, input: &str) -> VectorPersisted {
        let mut tokens: Vec<Cow<'_, str>> = Vec::new();
        self.tokenizer.tokenize_query(input, |t| tokens.push(t));
        to_persisted(self.bm25.embed_query(&tokens))
    }

    pub fn doc_embed(&self, input: &str) -> VectorPersisted {
        let mut tokens: Vec<Cow<'_, str>> = Vec::new();
        self.tokenizer.tokenize_doc(input, |t| tokens.push(t));
        to_persisted(self.bm25.embed_document(&tokens))
    }
}

fn to_persisted(e: SparseEmbedding) -> VectorPersisted {
    if e.is_empty() {
        VectorPersisted::empty_sparse()
    } else {
        VectorPersisted::new_sparse(e.indices, e.values)
    }
}

fn build_tokens_processor(value: TextPreprocessingConfig) -> TokensProcessor {
    let TextPreprocessingConfig {
        language,
        lowercase,
        ascii_folding,
        stopwords,
        stemmer,
        min_token_len,
        max_token_len,
    } = value;

    let lowercase = lowercase.unwrap_or(true);
    let ascii_folding = ascii_folding.unwrap_or(false);
    let language = language.unwrap_or_else(|| DEFAULT_LANGUAGE.to_string());

    // Warn once if the language is unrecognized. Historically `language: "none"`
    // (and any other unsupported value) silently disabled both stemming and
    // stopwords. That hack is deprecated: use `stemmer: {"type": "none"}` plus an
    // empty stopword set instead. We still tolerate it for now to avoid breaking
    // existing configs on upgrade, but emit a warning so users can migrate.
    if stopwords.is_none() && Language::from_str(&language).is_err() {
        log::warn!(
            "BM25 text preprocessing: language {language:?} is not recognized; \
             stemming and stopwords are disabled as a side effect. This behavior \
             (notably `language: \"none\"`) is deprecated and may be rejected in a \
             future release. To disable language-specific processing explicitly, \
             set `stemmer` to {{\"type\": \"none\"}} and configure an empty stopword set.",
        );
    }

    let stemmer = match stemmer {
        None => Stemmer::try_default_from_language(&language),
        // `Disabled` resolves to `None` here, giving an explicit opt-out of stemming.
        Some(algorithm) => Stemmer::from_algorithm(&algorithm),
    };

    let stopwords_config = match stopwords {
        None => Language::from_str(&language)
            .ok()
            .map(StopwordsInterface::Language),
        Some(interface) => Some(interface),
    };

    TokensProcessor::new(
        lowercase,
        ascii_folding,
        Arc::new(StopwordsFilter::new(&stopwords_config, lowercase)),
        stemmer,
        min_token_len,
        max_token_len,
    )
}

#[cfg(test)]
mod tests {
    use ordered_float::NotNan;
    use segment::data_types::index::TokenizerType;

    use super::*;

    fn make_config(tok: TokenizerType) -> Bm25Config {
        Bm25Config {
            k: NotNan::new(1.2).unwrap(),
            b: NotNan::new(0.75).unwrap(),
            avg_len: NotNan::new(256.0).unwrap(),
            tokenizer: tok,
            text_preprocessing_config: TextPreprocessingConfig {
                language: Some("english".to_string()),
                lowercase: Some(true),
                ascii_folding: Some(false),
                stopwords: None,
                stemmer: None,
                min_token_len: None,
                max_token_len: None,
            },
        }
    }

    /// Replicate the pre-refactor flow: a single `tokenize_query` call drives
    /// both query and document embeddings. Used to assert byte-for-byte
    /// equality of the new adapter against the old behavior.
    fn legacy_embed(config: Bm25Config, input: &str, doc: bool) -> VectorPersisted {
        let mut config = config;
        let preproc = std::mem::take(&mut config.text_preprocessing_config);
        let processor = build_tokens_processor(preproc);
        let tokenizer = Tokenizer::new(config.tokenizer, processor);

        let params = Bm25Params {
            k1: config.k.into_inner(),
            b: config.b.into_inner(),
            avg_doc_len: config.avg_len.into_inner(),
        };
        let bm25 = Bm25Core::new(params).unwrap();

        let mut tokens: Vec<Cow<'_, str>> = Vec::new();
        tokenizer.tokenize_query(input, |t| tokens.push(t));
        let e = if doc {
            bm25.embed_document(&tokens)
        } else {
            bm25.embed_query(&tokens)
        };
        to_persisted(e)
    }

    const CORPUS: &[&str] = &[
        "",
        "hello",
        "Hello World",
        "the quick brown fox jumps over the lazy dog",
        "alpha beta alpha gamma alpha",
        "The Cat Sat On The Mat",
        "lorem ipsum dolor sit amet consectetur adipiscing elit",
        "rust is a systems programming language focused on safety",
        "café naïve façade",
        "qdrant search engine vector database",
    ];

    /// Word/Whitespace/Multilingual: `tokenize_query == tokenize_doc`, so the
    /// new adapter must produce byte-identical output to the pre-refactor flow.
    #[test]
    fn parity_with_legacy_flow_word_tokenizer() {
        let bm25 = Bm25::new(make_config(TokenizerType::Word)).unwrap();
        for &input in CORPUS {
            let q_new = bm25.search_embed(input);
            let d_new = bm25.doc_embed(input);
            let q_old = legacy_embed(make_config(TokenizerType::Word), input, false);
            let d_old = legacy_embed(make_config(TokenizerType::Word), input, true);
            assert_eq!(q_new, q_old, "search_embed differs for {input:?}");
            assert_eq!(d_new, d_old, "doc_embed differs for {input:?}");
        }
    }

    #[test]
    fn parity_with_legacy_flow_whitespace_tokenizer() {
        let bm25 = Bm25::new(make_config(TokenizerType::Whitespace)).unwrap();
        for &input in CORPUS {
            let q_new = bm25.search_embed(input);
            let d_new = bm25.doc_embed(input);
            let q_old = legacy_embed(make_config(TokenizerType::Whitespace), input, false);
            let d_old = legacy_embed(make_config(TokenizerType::Whitespace), input, true);
            assert_eq!(q_new, q_old, "search_embed differs for {input:?}");
            assert_eq!(d_new, d_old, "doc_embed differs for {input:?}");
        }
    }

    #[test]
    fn parity_with_legacy_flow_multilingual_tokenizer() {
        let bm25 = Bm25::new(make_config(TokenizerType::Multilingual)).unwrap();
        for &input in CORPUS {
            let q_new = bm25.search_embed(input);
            let d_new = bm25.doc_embed(input);
            let q_old = legacy_embed(make_config(TokenizerType::Multilingual), input, false);
            let d_old = legacy_embed(make_config(TokenizerType::Multilingual), input, true);
            assert_eq!(q_new, q_old, "search_embed differs for {input:?}");
            assert_eq!(d_new, d_old, "doc_embed differs for {input:?}");
        }
    }

    /// Prefix: queries match the legacy flow exactly; documents intentionally
    /// differ because the pre-refactor code passed query-side tokens to the
    /// document embed path (the bug CodeRabbit flagged).
    /// `stemmer: {"type": "none"}` must produce a processor with no stemmer,
    /// i.e. the language default stemmer is suppressed even for a known language.
    #[test]
    fn disabled_stemmer_suppresses_language_default() {
        use segment::data_types::index::{
            DisabledStemmerParams, NoStemmer, StemmingAlgorithm, StopwordsInterface,
        };

        let mut cfg = make_config(TokenizerType::Word);
        cfg.text_preprocessing_config.stemmer =
            Some(StemmingAlgorithm::Disabled(DisabledStemmerParams {
                r#type: NoStemmer::None,
            }));
        // Empty stopword set => the recommended language-neutral setup.
        cfg.text_preprocessing_config.stopwords = Some(StopwordsInterface::Set(Default::default()));

        let disabled = Bm25::new(cfg).unwrap();

        fn sparse_len(v: &VectorPersisted) -> usize {
            match v {
                VectorPersisted::Sparse(s) => s.indices.len(),
                other => panic!("expected sparse vector, got {other:?}"),
            }
        }

        // With English defaults, "running" stems to "run" and collides with "run".
        let default_model = Bm25::new(make_config(TokenizerType::Word)).unwrap();
        let default_vec = default_model.doc_embed("running run");
        assert_eq!(
            sparse_len(&default_vec),
            1,
            "default English stemmer should collapse running/run"
        );

        // With stemming disabled they stay distinct.
        let disabled_vec = disabled.doc_embed("running run");
        assert_eq!(
            sparse_len(&disabled_vec),
            2,
            "disabled stemmer should keep running/run distinct"
        );
    }

    #[test]
    fn prefix_tokenizer_query_parity_doc_diverges() {
        let bm25 = Bm25::new(make_config(TokenizerType::Prefix)).unwrap();
        let input = "hello world example";

        let q_new = bm25.search_embed(input);
        let q_old = legacy_embed(make_config(TokenizerType::Prefix), input, false);
        assert_eq!(q_new, q_old, "prefix query path must match legacy");

        let d_new = bm25.doc_embed(input);
        let d_old = legacy_embed(make_config(TokenizerType::Prefix), input, true);
        assert_ne!(
            d_new, d_old,
            "prefix doc path must use the full n-gram set (intentional fix)"
        );
    }
}
