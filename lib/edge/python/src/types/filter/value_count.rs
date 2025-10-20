use derive_more::Into;
use pyo3::{pyclass, pymethods};
use segment::types::ValuesCount;

#[pyclass(name = "ValuesCount")]
#[derive(Clone, Debug, Into)]
pub struct PyValuesCount(pub ValuesCount);

#[pymethods]
impl PyValuesCount {
    #[new]
    #[pyo3(signature = (lt=None, gt=None, lte=None, gte=None))]
    pub fn new(
        lt: Option<usize>,
        gt: Option<usize>,
        lte: Option<usize>,
        gte: Option<usize>,
    ) -> Self {
        Self(ValuesCount { lt, gt, lte, gte })
    }
}
