use derive_more::Into;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use segment::data_types::vectors::NamedQuery;
use shard::query::query_enum::QueryEnum;

#[derive(Clone, Debug, Into)]
pub struct PyQuery(QueryEnum);

impl<'py> FromPyObject<'py> for PyQuery {
    fn extract_bound(query: &Bound<'py, PyAny>) -> PyResult<Self> {
        let query = if let Ok(single) = query.extract() {
            QueryEnum::Nearest(NamedQuery::default_dense(single))
        } else {
            return Err(PyValueError::new_err(format!(
                "failed to convert Python object {query} into query"
            )));
        };

        Ok(Self(query))
    }
}
