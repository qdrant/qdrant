use pyo3::{FromPyObject, IntoPyObject};
use segment::types::Condition;

use crate::types::PyFilter;
use crate::types::filter::field_condition::PyFieldCondition;

#[derive(Clone, Debug, IntoPyObject, FromPyObject)]
#[allow(clippy::large_enum_variant)]
pub enum PyCondition {
    Field(PyFieldCondition),
    Filter(PyFilter),
}

impl From<PyCondition> for Condition {
    fn from(value: PyCondition) -> Self {
        match value {
            PyCondition::Field(field_condition) => Condition::Field(field_condition.0),
            PyCondition::Filter(filter) => Condition::Filter(filter.0),
        }
    }
}

impl From<Condition> for PyCondition {
    fn from(value: Condition) -> Self {
        match value {
            Condition::Field(field_condition) => {
                PyCondition::Field(PyFieldCondition(field_condition))
            }
            Condition::IsEmpty(_) => todo!(),
            Condition::IsNull(_) => todo!(),
            Condition::HasId(_) => todo!(),
            Condition::HasVector(_) => todo!(),
            Condition::Nested(_) => todo!(),
            Condition::Filter(filter) => PyCondition::Filter(PyFilter(filter)),
            Condition::CustomIdChecker(_) => todo!(),
        }
    }
}
