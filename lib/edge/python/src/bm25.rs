//! Python bindings for [`edge::bm25_embed`].

use bytemuck::TransparentWrapper;
use derive_more::Into;
use edge::bm25_embed::{EdgeBm25, EdgeBm25Config};
use ordered_float::NotNan;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use segment::data_types::index::{StemmingAlgorithm, StopwordsInterface, TokenizerType};

use crate::repr::*;
use crate::types::payload_schema::{PyStemmingAlgorithm, PyStopwords, PyTokenizerType};
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
        k = None,
        b = None,
        avg_len = None,
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
        k: Option<f64>,
        b: Option<f64>,
        avg_len: Option<f64>,
        tokenizer: Option<PyTokenizerType>,
        language: Option<String>,
        lowercase: Option<bool>,
        ascii_folding: Option<bool>,
        stopwords: Option<PyStopwords>,
        stemmer: Option<PyStemmingAlgorithm>,
        min_token_len: Option<usize>,
        max_token_len: Option<usize>,
    ) -> PyResult<Self> {
        let mut cfg = EdgeBm25Config::default();
        if let Some(k) = k {
            cfg.k = NotNan::new(k).map_err(|_| PyValueError::new_err("k must not be NaN"))?;
        }
        if let Some(b) = b {
            cfg.b = NotNan::new(b).map_err(|_| PyValueError::new_err("b must not be NaN"))?;
        }
        if let Some(avg_len) = avg_len {
            cfg.avg_len = NotNan::new(avg_len)
                .map_err(|_| PyValueError::new_err("avg_len must not be NaN"))?;
        }
        if let Some(tokenizer) = tokenizer {
            cfg.tokenizer = TokenizerType::from(tokenizer);
        }
        cfg.language = language;
        cfg.lowercase = lowercase;
        cfg.ascii_folding = ascii_folding;
        cfg.stopwords = stopwords.map(StopwordsInterface::from);
        cfg.stemmer = stemmer.map(StemmingAlgorithm::from);
        cfg.min_token_len = min_token_len;
        cfg.max_token_len = max_token_len;
        Ok(Self(cfg))
    }

    #[getter]
    pub fn k(&self) -> f64 {
        self.0.k.into_inner()
    }

    #[getter]
    pub fn b(&self) -> f64 {
        self.0.b.into_inner()
    }

    #[getter]
    pub fn avg_len(&self) -> f64 {
        self.0.avg_len.into_inner()
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
            k: _,
            b: _,
            avg_len: _,
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
    pub fn new(config: Option<PyBm25Config>) -> PyResult<Self> {
        let config = config.map(EdgeBm25Config::from).unwrap_or_default();
        EdgeBm25::new(config)
            .map(Self)
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    /// Embed `text` as a search query: each unique token gets weight 1.0.
    pub fn embed_query(&self, text: &str) -> PySparseVector {
        PySparseVector(self.0.embed_query(text))
    }

    /// Embed `text` as an indexed document: term-frequency weights with `(k, b, avg_len)`.
    pub fn embed_document(&self, text: &str) -> PySparseVector {
        PySparseVector(self.0.embed_document(text))
    }
}
