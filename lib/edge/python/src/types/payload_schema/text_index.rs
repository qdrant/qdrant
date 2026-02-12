use std::collections::BTreeSet;
use std::fmt;

use bytemuck::TransparentWrapper;
use derive_more::Into;
use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use segment::data_types::index::*;

use crate::repr::*;

#[pyclass(name = "TextIndexParams", from_py_object)]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyTextIndexParams(pub TextIndexParams);

#[pyclass_repr]
#[pymethods]
impl PyTextIndexParams {
    #[getter]
    pub fn tokenizer(&self) -> PyTokenizerType {
        PyTokenizerType::from(self.0.tokenizer)
    }

    #[getter]
    pub fn min_token_len(&self) -> Option<usize> {
        self.0.min_token_len
    }

    #[getter]
    pub fn max_token_len(&self) -> Option<usize> {
        self.0.max_token_len
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
    pub fn phrase_matching(&self) -> Option<bool> {
        self.0.phrase_matching
    }

    #[getter]
    pub fn stopwords(&self) -> Option<&PyStopwords> {
        self.0.stopwords.as_ref().map(PyStopwords::wrap_ref)
    }

    #[getter]
    pub fn on_disk(&self) -> Option<bool> {
        self.0.on_disk
    }

    #[getter]
    pub fn stemmer(&self) -> Option<&PyStemmingAlgorithm> {
        self.0.stemmer.as_ref().map(PyStemmingAlgorithm::wrap_ref)
    }

    #[getter]
    pub fn enable_hnsw(&self) -> Option<bool> {
        self.0.enable_hnsw
    }
}

impl PyTextIndexParams {
    fn _getters(self) {
        // Every field should have a getter method
        let TextIndexParams {
            r#type: _, // not relevant for Qdrant Edge
            tokenizer: _,
            min_token_len: _,
            max_token_len: _,
            lowercase: _,
            ascii_folding: _,
            phrase_matching: _,
            stopwords: _,
            on_disk: _,
            stemmer: _,
            enable_hnsw: _,
        } = self.0;
    }
}

#[pyclass(name = "TokenizerType", from_py_object)]
#[derive(Copy, Clone, Debug)]
pub enum PyTokenizerType {
    Prefix,
    Whitespace,
    Word,
    Multilingual,
}

impl Repr for PyTokenizerType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let repr = match self {
            Self::Prefix => "Prefix",
            Self::Whitespace => "Whitespace",
            Self::Word => "Word",
            Self::Multilingual => "Multilingual",
        };

        f.simple_enum::<Self>(repr)
    }
}

impl From<TokenizerType> for PyTokenizerType {
    fn from(tokenizer_type: TokenizerType) -> Self {
        match tokenizer_type {
            TokenizerType::Prefix => PyTokenizerType::Prefix,
            TokenizerType::Whitespace => PyTokenizerType::Whitespace,
            TokenizerType::Word => PyTokenizerType::Word,
            TokenizerType::Multilingual => PyTokenizerType::Multilingual,
        }
    }
}

impl From<PyTokenizerType> for TokenizerType {
    fn from(tokenizer_type: PyTokenizerType) -> Self {
        match tokenizer_type {
            PyTokenizerType::Prefix => TokenizerType::Prefix,
            PyTokenizerType::Whitespace => TokenizerType::Whitespace,
            PyTokenizerType::Word => TokenizerType::Word,
            PyTokenizerType::Multilingual => TokenizerType::Multilingual,
        }
    }
}

#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyStopwords(StopwordsInterface);

impl FromPyObject<'_, '_> for PyStopwords {
    type Error = PyErr;

    fn extract(stopwords: Borrowed<'_, '_, PyAny>) -> Result<Self, Self::Error> {
        #[derive(FromPyObject)]
        enum Helper {
            Language(PyLanguage),
            Set(PyStopwordsSet),
        }

        fn _variants(stopwords: StopwordsInterface) {
            match stopwords {
                StopwordsInterface::Language(_) => {}
                StopwordsInterface::Set(_) => {}
            }
        }

        let stopwords = match stopwords.extract()? {
            Helper::Language(lang) => StopwordsInterface::Language(lang.into()),
            Helper::Set(set) => StopwordsInterface::Set(set.into()),
        };

        Ok(Self(stopwords))
    }
}

impl<'py> IntoPyObject<'py> for PyStopwords {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        match self.0 {
            StopwordsInterface::Language(lang) => PyLanguage::from(lang).into_bound_py_any(py),
            StopwordsInterface::Set(set) => PyStopwordsSet(set).into_bound_py_any(py),
        }
    }
}

impl<'py> IntoPyObject<'py> for &PyStopwords {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        IntoPyObject::into_pyobject(self.clone(), py)
    }
}

impl Repr for PyStopwords {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.0 {
            StopwordsInterface::Language(lang) => PyLanguage::from(*lang).fmt(f),
            StopwordsInterface::Set(set) => PyStopwordsSet::wrap_ref(set).fmt(f),
        }
    }
}

#[pyclass(name = "Language", from_py_object)]
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum PyLanguage {
    Arabic,
    Azerbaijani,
    Basque,
    Bengali,
    Catalan,
    Chinese,
    Danish,
    Dutch,
    English,
    Finnish,
    French,
    German,
    Greek,
    Hebrew,
    Hinglish,
    Hungarian,
    Indonesian,
    Italian,
    Japanese,
    Kazakh,
    Nepali,
    Norwegian,
    Portuguese,
    Romanian,
    Russian,
    Slovene,
    Spanish,
    Swedish,
    Tajik,
    Turkish,
}

impl Repr for PyLanguage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let repr = match self {
            Self::Arabic => "Arabic",
            Self::Azerbaijani => "Azerbaijani",
            Self::Basque => "Basque",
            Self::Bengali => "Bengali",
            Self::Catalan => "Catalan",
            Self::Chinese => "Chinese",
            Self::Danish => "Danish",
            Self::Dutch => "Dutch",
            Self::English => "English",
            Self::Finnish => "Finnish",
            Self::French => "French",
            Self::German => "German",
            Self::Greek => "Greek",
            Self::Hebrew => "Hebrew",
            Self::Hinglish => "Hinglish",
            Self::Hungarian => "Hungarian",
            Self::Indonesian => "Indonesian",
            Self::Italian => "Italian",
            Self::Japanese => "Japanese",
            Self::Kazakh => "Kazakh",
            Self::Nepali => "Nepali",
            Self::Norwegian => "Norwegian",
            Self::Portuguese => "Portuguese",
            Self::Romanian => "Romanian",
            Self::Russian => "Russian",
            Self::Slovene => "Slovene",
            Self::Spanish => "Spanish",
            Self::Swedish => "Swedish",
            Self::Tajik => "Tajik",
            Self::Turkish => "Turkish",
        };

        f.simple_enum::<Self>(repr)
    }
}

impl From<Language> for PyLanguage {
    fn from(language: Language) -> Self {
        match language {
            Language::Arabic => PyLanguage::Arabic,
            Language::Azerbaijani => PyLanguage::Azerbaijani,
            Language::Basque => PyLanguage::Basque,
            Language::Bengali => PyLanguage::Bengali,
            Language::Catalan => PyLanguage::Catalan,
            Language::Chinese => PyLanguage::Chinese,
            Language::Danish => PyLanguage::Danish,
            Language::Dutch => PyLanguage::Dutch,
            Language::English => PyLanguage::English,
            Language::Finnish => PyLanguage::Finnish,
            Language::French => PyLanguage::French,
            Language::German => PyLanguage::German,
            Language::Greek => PyLanguage::Greek,
            Language::Hebrew => PyLanguage::Hebrew,
            Language::Hinglish => PyLanguage::Hinglish,
            Language::Hungarian => PyLanguage::Hungarian,
            Language::Indonesian => PyLanguage::Indonesian,
            Language::Italian => PyLanguage::Italian,
            Language::Japanese => PyLanguage::Japanese,
            Language::Kazakh => PyLanguage::Kazakh,
            Language::Nepali => PyLanguage::Nepali,
            Language::Norwegian => PyLanguage::Norwegian,
            Language::Portuguese => PyLanguage::Portuguese,
            Language::Romanian => PyLanguage::Romanian,
            Language::Russian => PyLanguage::Russian,
            Language::Slovene => PyLanguage::Slovene,
            Language::Spanish => PyLanguage::Spanish,
            Language::Swedish => PyLanguage::Swedish,
            Language::Tajik => PyLanguage::Tajik,
            Language::Turkish => PyLanguage::Turkish,
        }
    }
}

impl From<PyLanguage> for Language {
    fn from(language: PyLanguage) -> Self {
        match language {
            PyLanguage::Arabic => Language::Arabic,
            PyLanguage::Azerbaijani => Language::Azerbaijani,
            PyLanguage::Basque => Language::Basque,
            PyLanguage::Bengali => Language::Bengali,
            PyLanguage::Catalan => Language::Catalan,
            PyLanguage::Chinese => Language::Chinese,
            PyLanguage::Danish => Language::Danish,
            PyLanguage::Dutch => Language::Dutch,
            PyLanguage::English => Language::English,
            PyLanguage::Finnish => Language::Finnish,
            PyLanguage::French => Language::French,
            PyLanguage::German => Language::German,
            PyLanguage::Greek => Language::Greek,
            PyLanguage::Hebrew => Language::Hebrew,
            PyLanguage::Hinglish => Language::Hinglish,
            PyLanguage::Hungarian => Language::Hungarian,
            PyLanguage::Indonesian => Language::Indonesian,
            PyLanguage::Italian => Language::Italian,
            PyLanguage::Japanese => Language::Japanese,
            PyLanguage::Kazakh => Language::Kazakh,
            PyLanguage::Nepali => Language::Nepali,
            PyLanguage::Norwegian => Language::Norwegian,
            PyLanguage::Portuguese => Language::Portuguese,
            PyLanguage::Romanian => Language::Romanian,
            PyLanguage::Russian => Language::Russian,
            PyLanguage::Slovene => Language::Slovene,
            PyLanguage::Spanish => Language::Spanish,
            PyLanguage::Swedish => Language::Swedish,
            PyLanguage::Tajik => Language::Tajik,
            PyLanguage::Turkish => Language::Turkish,
        }
    }
}

#[pyclass(name = "StopwordsSet", from_py_object)]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyStopwordsSet(StopwordsSet);

#[pyclass_repr]
#[pymethods]
impl PyStopwordsSet {
    #[getter]
    pub fn languages(&self) -> Option<BTreeSet<PyLanguage>> {
        self.0
            .languages
            .as_ref()
            .map(|langs| langs.iter().cloned().map(PyLanguage::from).collect())
    }

    #[getter]
    pub fn custom(&self) -> Option<&BTreeSet<String>> {
        self.0.custom.as_ref()
    }
}

impl PyStopwordsSet {
    fn _getters(self) {
        // Every field should have a getter method
        let StopwordsSet {
            languages: _,
            custom: _,
        } = self.0;
    }
}

impl<'py> IntoPyObject<'py> for &PyStopwordsSet {
    type Target = PyStopwordsSet;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        IntoPyObject::into_pyobject(self.clone(), py)
    }
}

#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyStemmingAlgorithm(StemmingAlgorithm);

impl FromPyObject<'_, '_> for PyStemmingAlgorithm {
    type Error = PyErr;

    fn extract(algo: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        #[derive(FromPyObject)]
        enum Helper {
            Snowball(PySnowballParams),
        }

        fn _variants(algo: StemmingAlgorithm) {
            match algo {
                StemmingAlgorithm::Snowball(_) => {}
            }
        }

        let algo = match algo.extract()? {
            Helper::Snowball(snowball) => StemmingAlgorithm::Snowball(snowball.into()),
        };

        Ok(Self(algo))
    }
}

impl<'py> IntoPyObject<'py> for PyStemmingAlgorithm {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        match self.0 {
            StemmingAlgorithm::Snowball(snowball) => {
                PySnowballParams(snowball).into_bound_py_any(py)
            }
        }
    }
}

impl<'py> IntoPyObject<'py> for &PyStemmingAlgorithm {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        IntoPyObject::into_pyobject(self.clone(), py)
    }
}

impl Repr for PyStemmingAlgorithm {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.0 {
            StemmingAlgorithm::Snowball(snowball) => PySnowballParams::wrap_ref(snowball).fmt(f),
        }
    }
}

#[pyclass(name = "SnowballParams", from_py_object)]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PySnowballParams(SnowballParams);

#[pyclass_repr]
#[pymethods]
impl PySnowballParams {
    #[getter]
    pub fn language(&self) -> PySnowballLanguage {
        PySnowballLanguage::from(self.0.language)
    }
}

impl PySnowballParams {
    fn _getters(self) {
        // Every field should have a getter method
        let SnowballParams {
            r#type: _, // not relevant for Qdrant Edge
            language: _,
        } = self.0;
    }
}

#[pyclass(name = "SnowballLanguage", from_py_object)]
#[derive(Copy, Clone, Debug)]
pub enum PySnowballLanguage {
    Arabic,
    Armenian,
    Danish,
    Dutch,
    English,
    Finnish,
    French,
    German,
    Greek,
    Hungarian,
    Italian,
    Norwegian,
    Portuguese,
    Romanian,
    Russian,
    Spanish,
    Swedish,
    Tamil,
    Turkish,
}

impl Repr for PySnowballLanguage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let repr = match self {
            Self::Arabic => "Arabic",
            Self::Armenian => "Armenian",
            Self::Danish => "Danish",
            Self::Dutch => "Dutch",
            Self::English => "English",
            Self::Finnish => "Finnish",
            Self::French => "French",
            Self::German => "German",
            Self::Greek => "Greek",
            Self::Hungarian => "Hungarian",
            Self::Italian => "Italian",
            Self::Norwegian => "Norwegian",
            Self::Portuguese => "Portuguese",
            Self::Romanian => "Romanian",
            Self::Russian => "Russian",
            Self::Spanish => "Spanish",
            Self::Swedish => "Swedish",
            Self::Tamil => "Tamil",
            Self::Turkish => "Turkish",
        };

        f.simple_enum::<Self>(repr)
    }
}

impl From<SnowballLanguage> for PySnowballLanguage {
    fn from(lang: SnowballLanguage) -> Self {
        match lang {
            SnowballLanguage::Arabic => PySnowballLanguage::Arabic,
            SnowballLanguage::Armenian => PySnowballLanguage::Armenian,
            SnowballLanguage::Danish => PySnowballLanguage::Danish,
            SnowballLanguage::Dutch => PySnowballLanguage::Dutch,
            SnowballLanguage::English => PySnowballLanguage::English,
            SnowballLanguage::Finnish => PySnowballLanguage::Finnish,
            SnowballLanguage::French => PySnowballLanguage::French,
            SnowballLanguage::German => PySnowballLanguage::German,
            SnowballLanguage::Greek => PySnowballLanguage::Greek,
            SnowballLanguage::Hungarian => PySnowballLanguage::Hungarian,
            SnowballLanguage::Italian => PySnowballLanguage::Italian,
            SnowballLanguage::Norwegian => PySnowballLanguage::Norwegian,
            SnowballLanguage::Portuguese => PySnowballLanguage::Portuguese,
            SnowballLanguage::Romanian => PySnowballLanguage::Romanian,
            SnowballLanguage::Russian => PySnowballLanguage::Russian,
            SnowballLanguage::Spanish => PySnowballLanguage::Spanish,
            SnowballLanguage::Swedish => PySnowballLanguage::Swedish,
            SnowballLanguage::Tamil => PySnowballLanguage::Tamil,
            SnowballLanguage::Turkish => PySnowballLanguage::Turkish,
        }
    }
}

impl From<PySnowballLanguage> for SnowballLanguage {
    fn from(lang: PySnowballLanguage) -> Self {
        match lang {
            PySnowballLanguage::Arabic => SnowballLanguage::Arabic,
            PySnowballLanguage::Armenian => SnowballLanguage::Armenian,
            PySnowballLanguage::Danish => SnowballLanguage::Danish,
            PySnowballLanguage::Dutch => SnowballLanguage::Dutch,
            PySnowballLanguage::English => SnowballLanguage::English,
            PySnowballLanguage::Finnish => SnowballLanguage::Finnish,
            PySnowballLanguage::French => SnowballLanguage::French,
            PySnowballLanguage::German => SnowballLanguage::German,
            PySnowballLanguage::Greek => SnowballLanguage::Greek,
            PySnowballLanguage::Hungarian => SnowballLanguage::Hungarian,
            PySnowballLanguage::Italian => SnowballLanguage::Italian,
            PySnowballLanguage::Norwegian => SnowballLanguage::Norwegian,
            PySnowballLanguage::Portuguese => SnowballLanguage::Portuguese,
            PySnowballLanguage::Romanian => SnowballLanguage::Romanian,
            PySnowballLanguage::Russian => SnowballLanguage::Russian,
            PySnowballLanguage::Spanish => SnowballLanguage::Spanish,
            PySnowballLanguage::Swedish => SnowballLanguage::Swedish,
            PySnowballLanguage::Tamil => SnowballLanguage::Tamil,
            PySnowballLanguage::Turkish => SnowballLanguage::Turkish,
        }
    }
}
