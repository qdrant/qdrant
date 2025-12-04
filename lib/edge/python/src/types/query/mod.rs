use bytemuck::{TransparentWrapper, TransparentWrapperAlloc as _};
use derive_more::Into;
use ordered_float::OrderedFloat;
use pyo3::prelude::*;
use segment::data_types::vectors::{NamedQuery, VectorInternal};
use segment::vector_storage::query::*;
use shard::query::query_enum::QueryEnum;

use crate::types::*;

#[derive(Clone, Debug, Into)]
pub struct PyQuery(pub QueryEnum);

impl FromPyObject<'_, '_> for PyQuery {
    type Error = PyErr;

    fn extract(query: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        let query = match query.extract()? {
            PyQueryInterface::Nearest { query, using } => QueryEnum::Nearest(NamedQuery {
                query: VectorInternal::from(query),
                using,
            }),

            PyQueryInterface::RecommendBestScore { query, using } => {
                QueryEnum::RecommendBestScore(NamedQuery {
                    query: RecoQuery::from(query),
                    using,
                })
            }

            PyQueryInterface::RecommendSumScores { query, using } => {
                QueryEnum::RecommendSumScores(NamedQuery {
                    query: RecoQuery::from(query),
                    using,
                })
            }

            PyQueryInterface::Discover { query, using } => QueryEnum::Discover(NamedQuery {
                query: DiscoveryQuery::from(query),
                using,
            }),

            PyQueryInterface::Context { query, using } => QueryEnum::Context(NamedQuery {
                query: ContextQuery::from(query),
                using,
            }),

            PyQueryInterface::FeedbackSimple { query, using } => {
                QueryEnum::FeedbackSimple(NamedQuery {
                    query: FeedbackQueryInternal::from(query),
                    using,
                })
            }
        };

        Ok(Self(query))
    }
}

impl<'py> IntoPyObject<'py> for PyQuery {
    type Target = PyQueryInterface;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr; // Infallible?

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        let query = match self.0 {
            QueryEnum::Nearest(NamedQuery { query, using }) => PyQueryInterface::Nearest {
                query: PyNamedVectorInternal(query),
                using,
            },

            QueryEnum::RecommendBestScore(NamedQuery { query, using }) => {
                PyQueryInterface::RecommendBestScore {
                    query: PyRecommendQuery(query),
                    using,
                }
            }

            QueryEnum::RecommendSumScores(NamedQuery { query, using }) => {
                PyQueryInterface::RecommendSumScores {
                    query: PyRecommendQuery(query),
                    using,
                }
            }

            QueryEnum::Discover(NamedQuery { query, using }) => PyQueryInterface::Discover {
                query: PyDiscoverQuery(query),
                using,
            },

            QueryEnum::Context(NamedQuery { query, using }) => PyQueryInterface::Context {
                query: PyContextQuery(query),
                using,
            },

            QueryEnum::FeedbackSimple(NamedQuery { query, using }) => {
                PyQueryInterface::FeedbackSimple {
                    query: PyFeedbackSimpleQuery(query),
                    using,
                }
            }
        };

        Bound::new(py, query)
    }
}

#[pyclass(name = "Query")]
#[derive(Clone, Debug)]
pub enum PyQueryInterface {
    #[pyo3(constructor = (query, using = None))]
    Nearest {
        query: PyNamedVectorInternal,
        using: Option<String>,
    },

    #[pyo3(constructor = (query, using = None))]
    RecommendBestScore {
        query: PyRecommendQuery,
        using: Option<String>,
    },

    #[pyo3(constructor = (query, using = None))]
    RecommendSumScores {
        query: PyRecommendQuery,
        using: Option<String>,
    },

    #[pyo3(constructor = (query, using = None))]
    Discover {
        query: PyDiscoverQuery,
        using: Option<String>,
    },

    #[pyo3(constructor = (query, using = None))]
    Context {
        query: PyContextQuery,
        using: Option<String>,
    },

    #[pyo3(constructor = (query, using = None))]
    FeedbackSimple {
        query: PyFeedbackSimpleQuery,
        using: Option<String>,
    },
}

#[pyclass(name = "RecommendationQuery")]
#[derive(Clone, Debug, Into)]
pub struct PyRecommendQuery(RecoQuery<VectorInternal>);

#[pymethods]
impl PyRecommendQuery {
    #[new]
    pub fn new(
        positives: Vec<PyNamedVectorInternal>,
        negatives: Vec<PyNamedVectorInternal>,
    ) -> Self {
        Self(RecoQuery {
            positives: PyNamedVectorInternal::peel_vec(positives),
            negatives: PyNamedVectorInternal::peel_vec(negatives),
        })
    }

    #[getter]
    pub fn positives(&self) -> &[PyNamedVectorInternal] {
        PyNamedVectorInternal::wrap_slice(&self.0.positives)
    }

    #[getter]
    pub fn negatives(&self) -> &[PyNamedVectorInternal] {
        PyNamedVectorInternal::wrap_slice(&self.0.negatives)
    }
}

#[pyclass(name = "DiscoveryQuery")]
#[derive(Clone, Debug, Into)]
pub struct PyDiscoverQuery(DiscoveryQuery<VectorInternal>);

#[pymethods]
impl PyDiscoverQuery {
    #[new]
    pub fn new(target: PyNamedVectorInternal, pairs: Vec<PyContextPair>) -> Self {
        Self(DiscoveryQuery {
            target: VectorInternal::from(target),
            pairs: PyContextPair::peel_vec(pairs),
        })
    }

    #[getter]
    pub fn target(&self) -> &PyNamedVectorInternal {
        PyNamedVectorInternal::wrap_ref(&self.0.target)
    }

    #[getter]
    pub fn pairs(&self) -> &[PyContextPair] {
        PyContextPair::wrap_slice(&self.0.pairs)
    }
}

#[pyclass(name = "ContextQuery")]
#[derive(Clone, Debug, Into)]
pub struct PyContextQuery(ContextQuery<VectorInternal>);

#[pymethods]
impl PyContextQuery {
    #[new]
    pub fn new(pairs: Vec<PyContextPair>) -> Self {
        Self(ContextQuery {
            pairs: PyContextPair::peel_vec(pairs),
        })
    }

    #[getter]
    pub fn pairs(&self) -> &[PyContextPair] {
        PyContextPair::wrap_slice(&self.0.pairs)
    }
}

#[pyclass(name = "ContextPair")]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyContextPair(ContextPair<VectorInternal>);

#[pymethods]
impl PyContextPair {
    #[new]
    pub fn new(positive: PyNamedVectorInternal, negative: PyNamedVectorInternal) -> Self {
        Self(ContextPair {
            positive: VectorInternal::from(positive),
            negative: VectorInternal::from(negative),
        })
    }

    #[getter]
    pub fn positive(&self) -> &PyNamedVectorInternal {
        PyNamedVectorInternal::wrap_ref(&self.0.positive)
    }

    #[getter]
    pub fn negative(&self) -> &PyNamedVectorInternal {
        PyNamedVectorInternal::wrap_ref(&self.0.negative)
    }
}

impl<'py> IntoPyObject<'py> for &PyContextPair {
    type Target = PyContextPair;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr; // Infallible

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        IntoPyObject::into_pyobject(self.clone(), py)
    }
}

#[pyclass(name = "FeedbackSimpleQuery")]
#[derive(Clone, Debug, Into)]
pub struct PyFeedbackSimpleQuery(FeedbackQueryInternal<VectorInternal, SimpleFeedbackStrategy>);

#[pymethods]
impl PyFeedbackSimpleQuery {
    #[new]
    pub fn new(
        target: PyNamedVectorInternal,
        feedback: Vec<PyFeedbackItem>,
        strategy: PySimpleFeedbackStrategy,
    ) -> Self {
        Self(FeedbackQueryInternal {
            target: VectorInternal::from(target),
            feedback: PyFeedbackItem::peel_vec(feedback),
            strategy: SimpleFeedbackStrategy::from(strategy),
        })
    }

    #[getter]
    pub fn target(&self) -> &PyNamedVectorInternal {
        PyNamedVectorInternal::wrap_ref(&self.0.target)
    }

    #[getter]
    pub fn feedback(&self) -> &[PyFeedbackItem] {
        PyFeedbackItem::wrap_slice(&self.0.feedback)
    }

    #[getter]
    pub fn strategy(&self) -> PySimpleFeedbackStrategy {
        PySimpleFeedbackStrategy(self.0.strategy)
    }
}

#[pyclass(name = "FeedbackItem")]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyFeedbackItem(FeedbackItem<VectorInternal>);

#[pymethods]
impl PyFeedbackItem {
    #[new]
    pub fn new(vector: PyNamedVectorInternal, score: f32) -> Self {
        Self(FeedbackItem {
            vector: VectorInternal::from(vector),
            score: OrderedFloat(score),
        })
    }

    #[getter]
    pub fn vector(&self) -> &PyNamedVectorInternal {
        PyNamedVectorInternal::wrap_ref(&self.0.vector)
    }

    #[getter]
    pub fn score(&self) -> f32 {
        self.0.score.into_inner()
    }
}

impl<'py> IntoPyObject<'py> for &PyFeedbackItem {
    type Target = PyFeedbackItem;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr; // Infallible

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        IntoPyObject::into_pyobject(self.clone(), py)
    }
}

#[pyclass(name = "SimpleFeedbackStrategy")]
#[derive(Copy, Clone, Debug, Into)]
pub struct PySimpleFeedbackStrategy(SimpleFeedbackStrategy);

#[pymethods]
impl PySimpleFeedbackStrategy {
    #[new]
    pub fn new(a: f32, b: f32, c: f32) -> Self {
        Self(SimpleFeedbackStrategy {
            a: OrderedFloat(a),
            b: OrderedFloat(b),
            c: OrderedFloat(c),
        })
    }

    #[getter]
    pub fn a(&self) -> f32 {
        self.0.a.into_inner()
    }

    #[getter]
    pub fn b(&self) -> f32 {
        self.0.b.into_inner()
    }

    #[getter]
    pub fn c(&self) -> f32 {
        self.0.c.into_inner()
    }
}
