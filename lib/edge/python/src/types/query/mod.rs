use bytemuck::{TransparentWrapper, TransparentWrapperAlloc as _};
use derive_more::Into;
use ordered_float::OrderedFloat;
use pyo3::prelude::*;
use segment::data_types::vectors::{NamedQuery, VectorInternal};
use segment::vector_storage::query::*;
use shard::query::query_enum::QueryEnum;

use crate::types::*;

#[derive(Clone, Debug, Into)]
pub struct PyQuery(QueryEnum);

impl FromPyObject<'_, '_> for PyQuery {
    type Error = PyErr;

    fn extract(query: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        let query = match query.extract()? {
            PyQueryInterface::Nearest { query, using } => QueryEnum::Nearest(NamedQuery {
                query: VectorInternal::try_from(query)?,
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
                query: PyNamedVector::from(query),
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
        query: PyNamedVector,
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
    pub fn new(positives: Vec<PyNamedVector>, negatives: Vec<PyNamedVector>) -> PyResult<Self> {
        let positives = positives
            .into_iter()
            .map(VectorInternal::try_from)
            .collect::<Result<_, _>>()?;

        let negatives = negatives
            .into_iter()
            .map(VectorInternal::try_from)
            .collect::<Result<_, _>>()?;

        Ok(Self(RecoQuery {
            positives,
            negatives,
        }))
    }
}

#[pyclass(name = "DiscoveryQuery")]
#[derive(Clone, Debug, Into)]
pub struct PyDiscoverQuery(DiscoveryQuery<VectorInternal>);

#[pymethods]
impl PyDiscoverQuery {
    #[new]
    pub fn new(target: PyNamedVector, pairs: Vec<PyContextPair>) -> PyResult<Self> {
        Ok(Self(DiscoveryQuery {
            target: VectorInternal::try_from(target)?,
            pairs: PyContextPair::peel_vec(pairs),
        }))
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
}

#[pyclass(name = "ContextPair")]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyContextPair(ContextPair<VectorInternal>);

#[pymethods]
impl PyContextPair {
    #[new]
    pub fn new(positive: PyNamedVector, negative: PyNamedVector) -> PyResult<Self> {
        Ok(Self(ContextPair {
            positive: VectorInternal::try_from(positive)?,
            negative: VectorInternal::try_from(negative)?,
        }))
    }
}

#[pyclass(name = "FeedbackSimpleQuery")]
#[derive(Clone, Debug, Into)]
pub struct PyFeedbackSimpleQuery(FeedbackQueryInternal<VectorInternal, SimpleFeedbackStrategy>);

#[pymethods]
impl PyFeedbackSimpleQuery {
    #[new]
    pub fn new(
        target: PyNamedVector,
        feedback: Vec<PyFeedbackItem>,
        strategy: PySimpleFeedbackStrategy,
    ) -> PyResult<Self> {
        Ok(Self(FeedbackQueryInternal {
            target: VectorInternal::try_from(target)?,
            feedback: PyFeedbackItem::peel_vec(feedback),
            strategy: SimpleFeedbackStrategy::from(strategy),
        }))
    }
}

#[pyclass(name = "FeedbackItem")]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyFeedbackItem(FeedbackItem<VectorInternal>);

#[pymethods]
impl PyFeedbackItem {
    #[new]
    pub fn new(vector: PyNamedVector, score: f32) -> PyResult<Self> {
        Ok(Self(FeedbackItem {
            vector: VectorInternal::try_from(vector)?,
            score: OrderedFloat(score),
        }))
    }
}

#[pyclass(name = "SimpleFeedbackStrategy")]
#[derive(Clone, Debug, Into)]
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
}
