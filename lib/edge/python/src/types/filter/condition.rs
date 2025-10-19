use pyo3::{FromPyObject, IntoPyObject};
use segment::types::Condition;
use crate::types::filter::field_condition::PyFieldCondition;

#[derive(Clone, Debug, IntoPyObject, FromPyObject)]
pub enum PyCondition {
    FieldCondition(PyFieldCondition),
}

impl From<PyCondition> for Condition {
    fn from(value: PyCondition) -> Self {
        match value {
            PyCondition::FieldCondition(field_condition) => Condition::Field(field_condition.0),
        }
    }
}

impl From<Condition> for PyCondition {
    fn from(value: Condition) -> Self {
        match value {
            Condition::Field(field_condition) => {
                PyCondition::FieldCondition(PyFieldCondition(field_condition))
            }
            Condition::IsEmpty(_) => todo!(),
            Condition::IsNull(_) => todo!(),
            Condition::HasId(_) => todo!(),
            Condition::HasVector(_) => todo!(),
            Condition::Nested(_) => todo!(),
            Condition::Filter(_) => todo!(),
            Condition::CustomIdChecker(_) => todo!(),
        }
    }
}