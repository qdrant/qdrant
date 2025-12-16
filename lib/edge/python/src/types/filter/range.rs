use std::fmt;

use derive_more::Into;
use ordered_float::OrderedFloat;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use segment::types::*;

use crate::repr::*;

#[derive(Copy, Clone, Debug, FromPyObject, IntoPyObject)]
pub enum PyRange {
    Float(PyRangeFloat),
    DateTime(PyRangeDateTime),
}

impl Repr for PyRange {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            PyRange::Float(float) => float.fmt(f),
            PyRange::DateTime(date_time) => date_time.fmt(f),
        }
    }
}

impl From<RangeInterface> for PyRange {
    fn from(range: RangeInterface) -> Self {
        match range {
            RangeInterface::Float(float) => PyRange::Float(PyRangeFloat(float)),
            RangeInterface::DateTime(date_time) => PyRange::DateTime(PyRangeDateTime(date_time)),
        }
    }
}

impl From<PyRange> for RangeInterface {
    fn from(range: PyRange) -> Self {
        match range {
            PyRange::Float(float) => RangeInterface::Float(float.0),
            PyRange::DateTime(date_time) => RangeInterface::DateTime(date_time.0),
        }
    }
}

#[pyclass(name = "RangeFloat")]
#[derive(Copy, Clone, Debug, Into)]
pub struct PyRangeFloat(pub Range<OrderedFloat<FloatPayloadType>>);

#[pyclass_repr]
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

    #[getter]
    pub fn gte(&self) -> Option<FloatPayloadType> {
        self.0.gte.map(|of| of.into_inner())
    }

    #[getter]
    pub fn gt(&self) -> Option<FloatPayloadType> {
        self.0.gt.map(|of| of.into_inner())
    }

    #[getter]
    pub fn lte(&self) -> Option<FloatPayloadType> {
        self.0.lte.map(|of| of.into_inner())
    }

    #[getter]
    pub fn lt(&self) -> Option<FloatPayloadType> {
        self.0.lt.map(|of| of.into_inner())
    }
}

impl PyRangeFloat {
    fn _getters(self) {
        // Every field should have a getter method
        let Range {
            gte: _,
            gt: _,
            lte: _,
            lt: _,
        } = self.0;
    }
}

#[pyclass(name = "RangeDateTime")]
#[derive(Copy, Clone, Debug, Into)]
pub struct PyRangeDateTime(pub Range<DateTimePayloadType>);

#[pyclass_repr]
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
        Ok(Self(Range {
            gte: parse_datetime_opt(gte.as_deref())?,
            gt: parse_datetime_opt(gt.as_deref())?,
            lte: parse_datetime_opt(lte.as_deref())?,
            lt: parse_datetime_opt(lt.as_deref())?,
        }))
    }

    #[getter]
    pub fn gte(&self) -> Option<String> {
        self.0.gte.map(|dt| dt.to_string())
    }

    #[getter]
    pub fn gt(&self) -> Option<String> {
        self.0.gt.map(|dt| dt.to_string())
    }

    #[getter]
    pub fn lte(&self) -> Option<String> {
        self.0.lte.map(|dt| dt.to_string())
    }

    #[getter]
    pub fn lt(&self) -> Option<String> {
        self.0.lt.map(|dt| dt.to_string())
    }
}

impl PyRangeDateTime {
    fn _getters(self) {
        // Every field should have a getter method
        let Range {
            gte: _,
            gt: _,
            lte: _,
            lt: _,
        } = self.0;
    }
}

fn parse_datetime_opt(date_time: Option<&str>) -> PyResult<Option<DateTimeWrapper>> {
    date_time.map(parse_datetime).transpose()
}

fn parse_datetime(date_time: &str) -> PyResult<DateTimeWrapper> {
    date_time
        .parse()
        .map_err(|err| PyValueError::new_err(format!("failed to parse date-time: {err}")))
}
