pub mod with_payload;
pub mod with_vector;

use std::fmt;

use bytemuck::{TransparentWrapper, TransparentWrapperAlloc as _};
use derive_more::Into;
use ordered_float::OrderedFloat;
use pyo3::PyTypeInfo;
use pyo3::inspect::PyStaticExpr;
use pyo3::prelude::*;
use segment::data_types::vectors::{NamedQuery, VectorInternal};
use segment::vector_storage::query::*;
use shard::query::query_enum::QueryEnum;

pub use self::with_payload::*;
pub use self::with_vector::*;
use crate::repr::*;
use crate::types::*;

#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyQuery(pub QueryEnum);

impl FromPyObject<'_, '_> for PyQuery {
    type Error = PyErr;
    const INPUT_TYPE: PyStaticExpr = PyQueryInterface::TYPE_HINT;

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
                query: DiscoverQuery::from(query),
                using,
            }),

            PyQueryInterface::Context { query, using } => QueryEnum::Context(NamedQuery {
                query: ContextQuery::from(query),
                using,
            }),

            PyQueryInterface::FeedbackNaive { query, using } => {
                QueryEnum::FeedbackNaive(NamedQuery {
                    query: NaiveFeedbackQuery::from(query),
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
    const OUTPUT_TYPE: PyStaticExpr = PyQueryInterface::TYPE_HINT;

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

            QueryEnum::FeedbackNaive(NamedQuery { query, using }) => {
                PyQueryInterface::FeedbackNaive {
                    query: PyFeedbackNaiveQuery(query),
                    using,
                }
            }
        };

        Bound::new(py, query)
    }
}

impl<'py> IntoPyObject<'py> for &PyQuery {
    type Target = PyQueryInterface;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr; // Infallible
    const OUTPUT_TYPE: PyStaticExpr = PyQueryInterface::TYPE_HINT;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        IntoPyObject::into_pyobject(self.clone(), py)
    }
}

impl Repr for PyQuery {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let (repr, query, using): (_, &dyn Repr, _) = match &self.0 {
            QueryEnum::Nearest(NamedQuery { query, using }) => {
                ("Nearest", PyNamedVectorInternal::wrap_ref(query), using)
            }
            QueryEnum::RecommendBestScore(NamedQuery { query, using }) => (
                "RecommendBestScore",
                PyRecommendQuery::wrap_ref(query),
                using,
            ),
            QueryEnum::RecommendSumScores(NamedQuery { query, using }) => (
                "RecommendSumScores",
                PyRecommendQuery::wrap_ref(query),
                using,
            ),
            QueryEnum::Discover(NamedQuery { query, using }) => {
                ("Discover", PyDiscoverQuery::wrap_ref(query), using)
            }
            QueryEnum::Context(NamedQuery { query, using }) => {
                ("Context", PyContextQuery::wrap_ref(query), using)
            }
            QueryEnum::FeedbackNaive(NamedQuery { query, using }) => (
                "FeedbackNaive",
                PyFeedbackNaiveQuery::wrap_ref(query),
                using,
            ),
        };

        f.complex_enum::<PyQueryInterface>(repr, &[("query", query), ("using", using)])
    }
}

/// Query types for vector search.
#[pyclass(name = "Query", from_py_object)]
#[derive(Clone, Debug)]
pub enum PyQueryInterface {
    /// Create a nearest neighbor query.
    #[pyo3(constructor = (query, using = None))]
    Nearest {
        query: PyNamedVectorInternal,
        using: Option<String>,
    },

    /// Create a recommend query using best score.
    #[pyo3(constructor = (query, using = None))]
    RecommendBestScore {
        query: PyRecommendQuery,
        using: Option<String>,
    },

    /// Create a recommend query using sum of scores.
    #[pyo3(constructor = (query, using = None))]
    RecommendSumScores {
        query: PyRecommendQuery,
        using: Option<String>,
    },

    /// Create a discover query.
    #[pyo3(constructor = (query, using = None))]
    Discover {
        query: PyDiscoverQuery,
        using: Option<String>,
    },

    /// Create a context query.
    #[pyo3(constructor = (query, using = None))]
    Context {
        query: PyContextQuery,
        using: Option<String>,
    },

    /// Create a feedback naive query.
    #[pyo3(constructor = (query, using = None))]
    FeedbackNaive {
        query: PyFeedbackNaiveQuery,
        using: Option<String>,
    },
}

#[pymethods]
impl PyQueryInterface {
    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl Repr for PyQueryInterface {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let (repr, query, using): (_, &dyn Repr, _) = match self {
            PyQueryInterface::Nearest { query, using } => ("Nearest", query, using),
            PyQueryInterface::RecommendBestScore { query, using } => {
                ("RecommendBestScore", query, using)
            }
            PyQueryInterface::RecommendSumScores { query, using } => {
                ("RecommendSumScores", query, using)
            }
            PyQueryInterface::Discover { query, using } => ("Discover", query, using),
            PyQueryInterface::Context { query, using } => ("Context", query, using),
            PyQueryInterface::FeedbackNaive { query, using } => ("FeedbackNaive", query, using),
        };

        f.complex_enum::<Self>(repr, &[("query", query), ("using", using)])
    }
}

/// Query for recommendation based on positive and negative examples.
///
/// Create a RecommendQuery.
///
/// Args:
///     positives: Positive example vectors.
///     negatives: Negative example vectors.
#[pyclass(name = "RecommendQuery", from_py_object)]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyRecommendQuery(RecoQuery<VectorInternal>);

#[pyclass_repr]
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

    /// Positive examples.
    #[getter]
    pub fn positives(&self) -> &[PyNamedVectorInternal] {
        PyNamedVectorInternal::wrap_slice(&self.0.positives)
    }

    /// Negative examples.
    #[getter]
    pub fn negatives(&self) -> &[PyNamedVectorInternal] {
        PyNamedVectorInternal::wrap_slice(&self.0.negatives)
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyRecommendQuery {
    fn _getters(self) {
        // Every field should have a getter method
        let RecoQuery {
            positives: _,
            negatives: _,
        } = self.0;
    }
}

/// Query for discovery using a target and context pairs.
///
/// Create a DiscoverQuery.
///
/// Args:
///     target: Target vector.
///     pairs: Context pairs.
#[pyclass(name = "DiscoverQuery", from_py_object)]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyDiscoverQuery(DiscoverQuery<VectorInternal>);

#[pyclass_repr]
#[pymethods]
impl PyDiscoverQuery {
    #[new]
    pub fn new(target: PyNamedVectorInternal, pairs: Vec<PyContextPair>) -> Self {
        Self(DiscoverQuery {
            target: VectorInternal::from(target),
            pairs: PyContextPair::peel_vec(pairs),
        })
    }

    /// Target vector.
    #[getter]
    pub fn target(&self) -> &PyNamedVectorInternal {
        PyNamedVectorInternal::wrap_ref(&self.0.target)
    }

    /// Context pairs.
    #[getter]
    pub fn pairs(&self) -> &[PyContextPair] {
        PyContextPair::wrap_slice(&self.0.pairs)
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyDiscoverQuery {
    fn _getters(self) {
        // Every field should have a getter method
        let DiscoverQuery {
            target: _,
            pairs: _,
        } = self.0;
    }
}

/// Query based on context pairs only.
///
/// Create a ContextQuery.
///
/// Args:
///     pairs: Context pairs.
#[pyclass(name = "ContextQuery", from_py_object)]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyContextQuery(ContextQuery<VectorInternal>);

#[pyclass_repr]
#[pymethods]
impl PyContextQuery {
    #[new]
    pub fn new(pairs: Vec<PyContextPair>) -> Self {
        Self(ContextQuery {
            pairs: PyContextPair::peel_vec(pairs),
        })
    }

    /// Context pairs.
    #[getter]
    pub fn pairs(&self) -> &[PyContextPair] {
        PyContextPair::wrap_slice(&self.0.pairs)
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyContextQuery {
    fn _getters(self) {
        // Every field should have a getter method
        let ContextQuery { pairs: _ } = self.0;
    }
}

/// A positive/negative pair for context-based queries.
///
/// Create a ContextPair.
///
/// Args:
///     positive: Positive example.
///     negative: Negative example.
#[pyclass(name = "ContextPair", from_py_object)]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyContextPair(ContextPair<VectorInternal>);

#[pyclass_repr]
#[pymethods]
impl PyContextPair {
    #[new]
    pub fn new(positive: PyNamedVectorInternal, negative: PyNamedVectorInternal) -> Self {
        Self(ContextPair {
            positive: VectorInternal::from(positive),
            negative: VectorInternal::from(negative),
        })
    }

    /// Positive example.
    #[getter]
    pub fn positive(&self) -> &PyNamedVectorInternal {
        PyNamedVectorInternal::wrap_ref(&self.0.positive)
    }

    /// Negative example.
    #[getter]
    pub fn negative(&self) -> &PyNamedVectorInternal {
        PyNamedVectorInternal::wrap_ref(&self.0.negative)
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyContextPair {
    fn _getters(self) {
        // Every field should have a getter method
        let ContextPair {
            positive: _,
            negative: _,
        } = self.0;
    }
}

impl<'py> IntoPyObject<'py> for &PyContextPair {
    type Target = PyContextPair;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr; // Infallible
    const OUTPUT_TYPE: PyStaticExpr = PyContextPair::TYPE_HINT;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        IntoPyObject::into_pyobject(self.clone(), py)
    }
}

/// Query using naive feedback approach.
///
/// Create a FeedbackNaiveQuery.
///
/// Args:
///     target: Target vector.
///     feedback: Feedback items with scores.
///     strategy: Feedback coefficients.
#[pyclass(name = "FeedbackNaiveQuery", from_py_object)]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyFeedbackNaiveQuery(NaiveFeedbackQuery<VectorInternal>);

#[pyclass_repr]
#[pymethods]
impl PyFeedbackNaiveQuery {
    #[new]
    pub fn new(
        target: PyNamedVectorInternal,
        feedback: Vec<PyFeedbackItem>,
        strategy: PyNaiveFeedbackCoefficients,
    ) -> Self {
        Self(NaiveFeedbackQuery {
            target: VectorInternal::from(target),
            feedback: PyFeedbackItem::peel_vec(feedback),
            coefficients: NaiveFeedbackCoefficients::from(strategy),
        })
    }

    /// Target vector.
    #[getter]
    pub fn target(&self) -> &PyNamedVectorInternal {
        PyNamedVectorInternal::wrap_ref(&self.0.target)
    }

    /// Feedback items.
    #[getter]
    pub fn feedback(&self) -> &[PyFeedbackItem] {
        PyFeedbackItem::wrap_slice(&self.0.feedback)
    }

    /// Coefficients.
    #[getter]
    pub fn coefficients(&self) -> PyNaiveFeedbackCoefficients {
        PyNaiveFeedbackCoefficients(self.0.coefficients)
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyFeedbackNaiveQuery {
    fn _getters(self) {
        // Every field should have a getter method
        let NaiveFeedbackQuery {
            target: _,
            feedback: _,
            coefficients: _,
        } = self.0;
    }
}

/// A feedback item with vector and score.
///
/// Create a FeedbackItem.
///
/// Args:
///     vector: Feedback vector.
///     score: Feedback score.
#[pyclass(name = "FeedbackItem", from_py_object)]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyFeedbackItem(FeedbackItem<VectorInternal>);

#[pyclass_repr]
#[pymethods]
impl PyFeedbackItem {
    #[new]
    pub fn new(vector: PyNamedVectorInternal, score: f32) -> Self {
        Self(FeedbackItem {
            vector: VectorInternal::from(vector),
            score: OrderedFloat(score),
        })
    }

    /// Feedback vector.
    #[getter]
    pub fn vector(&self) -> &PyNamedVectorInternal {
        PyNamedVectorInternal::wrap_ref(&self.0.vector)
    }

    /// Feedback score.
    #[getter]
    pub fn score(&self) -> f32 {
        self.0.score.into_inner()
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyFeedbackItem {
    fn _getters(self) {
        // Every field should have a getter method
        let FeedbackItem {
            vector: _,
            score: _,
        } = self.0;
    }
}

impl<'py> IntoPyObject<'py> for &PyFeedbackItem {
    type Target = PyFeedbackItem;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr; // Infallible
    const OUTPUT_TYPE: PyStaticExpr = PyFeedbackItem::TYPE_HINT;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        IntoPyObject::into_pyobject(self.clone(), py)
    }
}

/// Coefficients for naive feedback query.
///
/// Create NaiveFeedbackStrategy coefficients.
///
/// Args:
///     a: Coefficient a.
///     b: Coefficient b.
///     c: Coefficient c.
#[pyclass(name = "NaiveFeedbackStrategy", from_py_object)]
#[derive(Copy, Clone, Debug, Into)]
pub struct PyNaiveFeedbackCoefficients(NaiveFeedbackCoefficients);

#[pyclass_repr]
#[pymethods]
impl PyNaiveFeedbackCoefficients {
    #[new]
    pub fn new(a: f32, b: f32, c: f32) -> Self {
        Self(NaiveFeedbackCoefficients {
            a: OrderedFloat(a),
            b: OrderedFloat(b),
            c: OrderedFloat(c),
        })
    }

    /// Coefficient a.
    #[getter]
    pub fn a(&self) -> f32 {
        self.0.a.into_inner()
    }

    /// Coefficient b.
    #[getter]
    pub fn b(&self) -> f32 {
        self.0.b.into_inner()
    }

    /// Coefficient c.
    #[getter]
    pub fn c(&self) -> f32 {
        self.0.c.into_inner()
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyNaiveFeedbackCoefficients {
    fn _getters(self) {
        // Every field should have a getter method
        let NaiveFeedbackCoefficients { a: _, b: _, c: _ } = self.0;
    }
}
