//! Python bindings for [`edge::bm25`].

use bytemuck::TransparentWrapper;
use derive_more::Into;
use edge::bm25::{EdgeBm25, EdgeBm25Config};
use pyo3::prelude::*;
use segment::data_types::index::{
    StemmingAlgorithm, StopwordsInterface, TokenizerType,
};

use crate::repr::*;
use crate::types::payload_schema::{
    PyStemmingAlgorithm, PyStopwords, PyTokenizerType,
};
use crate::types::vector::PySparseVector;

/// Configuration for an edge-side BM25 model.
#[pyclass(name = "Bm25Config", from_py_object)]
#[derive(Clone, Debug, Default, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyBm25Config(pub EdgeBm25Config);

#[pyclass_repr]
#[pymethods]
impl PyBm25Config {
    #[expect(clippy::too_many_arguments)]
    #[new]
    #[pyo3(signature = (
        k1 = None,
        b = None,
        avg_doc_len = None,
        tokenizer = None,
        language = None,
        lowercase = None,
        ascii_folding = None,
        stopwords = None,
        stemmer = None,
        min_token_len = None,
        max_token_len = None,
    ))]
    pub fn new(
        k1: Option<f64>,
        b: Option<f64>,
        avg_doc_len: Option<f64>,
        tokenizer: Option<PyTokenizerType>,
        language: Option<String>,
        lowercase: Option<bool>,
        ascii_folding: Option<bool>,
        stopwords: Option<PyStopwords>,
        stemmer: Option<PyStemmingAlgorithm>,
        min_token_len: Option<usize>,
        max_token_len: Option<usize>,
    ) -> Self {
        Self(EdgeBm25Config {
            k1,
            b,
            avg_doc_len,
            tokenizer: tokenizer.map(TokenizerType::from).unwrap_or_default(),
            language,
            lowercase,
            ascii_folding,
            stopwords: stopwords.map(StopwordsInterface::from),
            stemmer: stemmer.map(StemmingAlgorithm::from),
            min_token_len,
            max_token_len,
        })
    }

    #[getter]
    pub fn k1(&self) -> Option<f64> {
        self.0.k1
    }

    #[getter]
    pub fn b(&self) -> Option<f64> {
        self.0.b
    }

    #[getter]
    pub fn avg_doc_len(&self) -> Option<f64> {
        self.0.avg_doc_len
    }

    #[getter]
    pub fn tokenizer(&self) -> PyTokenizerType {
        PyTokenizerType::from(self.0.tokenizer)
    }

    #[getter]
    pub fn language(&self) -> Option<&str> {
        self.0.language.as_deref()
    }

    #[getter]
    pub fn lowercase(&self) -> Option<bool> {
        self.0.lowercase
    }

    #[getter]
    pub fn ascii_folding(&self) -> Option<bool> {
        self.0.ascii_folding
    }

    #[getter]
    pub fn stopwords(&self) -> Option<&PyStopwords> {
        self.0.stopwords.as_ref().map(PyStopwords::wrap_ref)
    }

    #[getter]
    pub fn stemmer(&self) -> Option<&PyStemmingAlgorithm> {
        self.0.stemmer.as_ref().map(PyStemmingAlgorithm::wrap_ref)
    }

    #[getter]
    pub fn min_token_len(&self) -> Option<usize> {
        self.0.min_token_len
    }

    #[getter]
    pub fn max_token_len(&self) -> Option<usize> {
        self.0.max_token_len
    }
}

impl PyBm25Config {
    fn _getters(self) {
        // Every field should have a getter method
        let EdgeBm25Config {
            k1: _,
            b: _,
            avg_doc_len: _,
            tokenizer: _,
            language: _,
            lowercase: _,
            ascii_folding: _,
            stopwords: _,
            stemmer: _,
            min_token_len: _,
            max_token_len: _,
        } = self.0;
    }
}

/// BM25 sparse-vector embedding model. Construct once with a [`Bm25Config`],
/// then call [`embed_query`] / [`embed_document`] to get sparse vectors.
#[pyclass(name = "Bm25")]
#[derive(Debug)]
pub struct PyBm25(EdgeBm25);

#[pymethods]
impl PyBm25 {
    #[new]
    #[pyo3(signature = (config = None))]
    pub fn new(config: Option<PyBm25Config>) -> Self {
        let config = config.map(EdgeBm25Config::from).unwrap_or_default();
        Self(EdgeBm25::new(config))
    }

    /// Embed `text` as a search query: each unique token gets weight 1.0.
    pub fn embed_query(&self, text: &str) -> PySparseVector {
        let doc = bm25::Bm25Document::new(text);
        PySparseVector(self.0.embed_query(&doc))
    }

    /// Embed `text` as an indexed document: term-frequency weights with `(k1, b, avg_doc_len)`.
    pub fn embed_document(&self, text: &str) -> PySparseVector {
        let doc = bm25::Bm25Document::new(text);
        PySparseVector(self.0.embed_document(&doc))
    }
}
