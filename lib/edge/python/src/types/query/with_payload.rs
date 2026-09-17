use bytemuck::{TransparentWrapper, TransparentWrapperAlloc as _};
use derive_more::Into;
use pyo3::inspect::PyStaticExpr;
use pyo3::prelude::*;
use pyo3::{IntoPyObjectExt as _, PyTypeInfo};
use segment::types::{
    PayloadSelector, PayloadSelectorExclude, PayloadSelectorInclude, WithPayloadInterface,
};

use crate::repr::*;
use crate::type_hint::Alias;
use crate::types::PyJsonPath;

#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyWithPayload(WithPayloadInterface);

pub const WITH_PAYLOAD: Alias = Alias {
    name: "WithPayloadType",
    definition: WithPayloadHelper::INPUT_TYPE,
};

#[derive(FromPyObject)]
enum WithPayloadHelper {
    Bool(bool),
    Fields(Vec<PyJsonPath>),
    Selector(PyPayloadSelector),
}

impl FromPyObject<'_, '_> for PyWithPayload {
    type Error = PyErr;
    const INPUT_TYPE: PyStaticExpr = WITH_PAYLOAD.hint();

    fn extract(with_payload: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        fn _variants(with_payload: WithPayloadInterface) {
            match with_payload {
                WithPayloadInterface::Bool(_) => {}
                WithPayloadInterface::Fields(_) => {}
                WithPayloadInterface::Selector(_) => {}
            }
        }

        let with_payload = match with_payload.extract()? {
            WithPayloadHelper::Bool(bool) => WithPayloadInterface::Bool(bool),
            WithPayloadHelper::Fields(fields) => {
                WithPayloadInterface::Fields(PyJsonPath::peel_vec(fields))
            }
            WithPayloadHelper::Selector(selector) => {
                WithPayloadInterface::Selector(PayloadSelector::from(selector))
            }
        };

        Ok(Self(with_payload))
    }
}

impl<'py> IntoPyObject<'py> for PyWithPayload {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr; // Infallible?
    const OUTPUT_TYPE: PyStaticExpr = WITH_PAYLOAD.hint();

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        IntoPyObject::into_pyobject(&self, py)
    }
}

impl<'py> IntoPyObject<'py> for &PyWithPayload {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr; // Infallible?
    const OUTPUT_TYPE: PyStaticExpr = WITH_PAYLOAD.hint();

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        match &self.0 {
            WithPayloadInterface::Bool(bool) => bool.into_bound_py_any(py),
            WithPayloadInterface::Fields(fields) => {
                PyJsonPath::wrap_slice(fields).into_bound_py_any(py)
            }
            WithPayloadInterface::Selector(selector) => PyPayloadSelector::wrap_ref(selector)
                .clone()
                .into_bound_py_any(py),
        }
    }
}

impl Repr for PyWithPayload {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            WithPayloadInterface::Bool(bool) => bool.fmt(f),
            WithPayloadInterface::Fields(fields) => PyJsonPath::wrap_slice(fields).fmt(f),
            WithPayloadInterface::Selector(selector) => {
                PyPayloadSelector::wrap_ref(selector).fmt(f)
            }
        }
    }
}

#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyPayloadSelector(PayloadSelector);

impl FromPyObject<'_, '_> for PyPayloadSelector {
    type Error = PyErr;
    const INPUT_TYPE: PyStaticExpr = PyPayloadSelectorInterface::TYPE_HINT;

    fn extract(selector: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        let selector = match selector.extract()? {
            PyPayloadSelectorInterface::Include { keys } => {
                PayloadSelector::Include(PayloadSelectorInclude {
                    include: PyJsonPath::peel_vec(keys),
                })
            }
            PyPayloadSelectorInterface::Exclude { keys } => {
                PayloadSelector::Exclude(PayloadSelectorExclude {
                    exclude: PyJsonPath::peel_vec(keys),
                })
            }
        };

        Ok(Self(selector))
    }
}

impl<'py> IntoPyObject<'py> for PyPayloadSelector {
    type Target = PyPayloadSelectorInterface;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr; // Infallible?
    const OUTPUT_TYPE: PyStaticExpr = PyPayloadSelectorInterface::TYPE_HINT;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        let selector = match self.0 {
            PayloadSelector::Include(PayloadSelectorInclude { include }) => {
                PyPayloadSelectorInterface::Include {
                    keys: PyJsonPath::wrap_vec(include),
                }
            }
            PayloadSelector::Exclude(PayloadSelectorExclude { exclude }) => {
                PyPayloadSelectorInterface::Exclude {
                    keys: PyJsonPath::wrap_vec(exclude),
                }
            }
        };

        Bound::new(py, selector)
    }
}

impl Repr for PyPayloadSelector {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let (repr, keys) = match &self.0 {
            PayloadSelector::Include(PayloadSelectorInclude { include }) => {
                ("Include", PyJsonPath::wrap_slice(include))
            }
            PayloadSelector::Exclude(PayloadSelectorExclude { exclude }) => {
                ("Exclude", PyJsonPath::wrap_slice(exclude))
            }
        };

        f.complex_enum::<PyPayloadSelectorInterface>(repr, &[("keys", &keys)])
    }
}

#[pyclass(name = "PayloadSelector", from_py_object)]
#[derive(Clone, Debug)]
pub enum PyPayloadSelectorInterface {
    Include { keys: Vec<PyJsonPath> },
    Exclude { keys: Vec<PyJsonPath> },
}

impl Repr for PyPayloadSelectorInterface {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let (repr, keys) = match self {
            PyPayloadSelectorInterface::Include { keys } => ("Include", keys),
            PyPayloadSelectorInterface::Exclude { keys } => ("Exclude", keys),
        };

        f.complex_enum::<Self>(repr, &[("keys", keys)])
    }
}
