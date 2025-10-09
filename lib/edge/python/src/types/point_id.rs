use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use segment::types::PointIdType;
use uuid::Uuid;

#[derive(Clone, Debug, IntoPyObject, FromPyObject)]
pub enum PyPointId {
    NumId(u64),
    UuidString(String),
    Uuid(Uuid),
}

impl TryFrom<PyPointId> for PointIdType {
    type Error = PyErr;
    fn try_from(value: PyPointId) -> Result<Self, Self::Error> {
        match value {
            PyPointId::NumId(id) => Ok(PointIdType::NumId(id)),
            PyPointId::UuidString(uuid_string) => {
                let uuid = Uuid::parse_str(&uuid_string).map_err(|_| {
                    PyErr::new::<PyException, _>(format!(
                        "failed to parse string {uuid_string} into UUID for point ID"
                    ))
                })?;
                Ok(PointIdType::Uuid(uuid))
            }
            PyPointId::Uuid(uuid) => Ok(PointIdType::Uuid(uuid)),
        }
    }
}

impl From<PointIdType> for PyPointId {
    fn from(value: PointIdType) -> Self {
        match value {
            PointIdType::NumId(id) => PyPointId::NumId(id),
            PointIdType::Uuid(uuid) => PyPointId::Uuid(uuid),
        }
    }
}
