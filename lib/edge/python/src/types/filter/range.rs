use std::str::FromStr;

use derive_more::Into;
use ordered_float::OrderedFloat;
use pyo3::exceptions::PyTypeError;
use pyo3::{FromPyObject, IntoPyObject, PyErr, pyclass, pymethods};
use segment::types::{DateTimePayloadType, FloatPayloadType, Range, RangeInterface};

#[derive(Clone, Debug, IntoPyObject, FromPyObject)]
pub enum PyRangeInterface {
    Float(PyRangeFloat),
    DateTime(PyRangeDateTime),
}

impl From<PyRangeInterface> for RangeInterface {
    fn from(value: PyRangeInterface) -> Self {
        match value {
            PyRangeInterface::Float(v) => RangeInterface::Float(v.0),
            PyRangeInterface::DateTime(v) => RangeInterface::DateTime(v.0),
        }
    }
}

impl From<RangeInterface> for PyRangeInterface {
    fn from(value: RangeInterface) -> Self {
        match value {
            RangeInterface::Float(v) => PyRangeInterface::Float(PyRangeFloat(v)),
            RangeInterface::DateTime(v) => PyRangeInterface::DateTime(PyRangeDateTime(v)),
        }
    }
}

#[pyclass(name = "RangeFloat")]
#[derive(Clone, Debug, Into)]
pub struct PyRangeFloat(pub Range<OrderedFloat<FloatPayloadType>>);

#[pymethods]
impl PyRangeFloat {
    #[new]
    #[pyo3(signature = (gte=None, gt=None, lte=None, lt=None))]
    pub fn new(
        gte: Option<FloatPayloadType>,
        gt: Option<FloatPayloadType>,
        lte: Option<FloatPayloadType>,
        lt: Option<FloatPayloadType>,
    ) -> Self {
        Self(Range {
            gte: gte.map(OrderedFloat),
            gt: gt.map(OrderedFloat),
            lte: lte.map(OrderedFloat),
            lt: lt.map(OrderedFloat),
        })
    }
}

#[pyclass(name = "RangeDateTime")]
#[derive(Clone, Debug, Into)]
pub struct PyRangeDateTime(pub Range<DateTimePayloadType>);

fn parse_datetime(datetine_string: Option<String>) -> Result<Option<DateTimePayloadType>, PyErr> {
    datetine_string
        .as_ref()
        .map(|s| DateTimePayloadType::from_str(s))
        .transpose()
        .map_err(|e| PyErr::new::<PyTypeError, _>(e.to_string()))
}

#[pymethods]
impl PyRangeDateTime {
    #[new]
    #[pyo3(signature = (gte=None, gt=None, lte=None, lt=None))]
    pub fn new(
        gte: Option<String>,
        gt: Option<String>,
        lte: Option<String>,
        lt: Option<String>,
    ) -> Result<Self, PyErr> {
        let gte_time = parse_datetime(gte)?;
        let gt_time = parse_datetime(gt)?;
        let lte_time = parse_datetime(lte)?;
        let lt_time = parse_datetime(lt)?;

        Ok(Self(Range {
            gte: gte_time,
            gt: gt_time,
            lte: lte_time,
            lt: lt_time,
        }))
    }
}
