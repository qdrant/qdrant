use std::fmt::Debug;

use schemars::JsonSchema;
use serde::Serialize;

use crate::solution::Solution;

/// Type of the issue code
pub type CodeType = String;

/// An issue that can be identified by its code
#[derive(Debug, Serialize, JsonSchema, Clone)]
pub struct Issue {
    pub code: CodeType,
    pub description: String,
    pub solution: Solution,
}

#[derive(Clone)]
pub(crate) struct DummyIssue {
    pub code: String,
}

impl DummyIssue {
    #[cfg(test)]
    pub fn new(code: impl Into<String>) -> Self {
        Self { code: code.into() }
    }
}

impl From<DummyIssue> for Issue {
    fn from(val: DummyIssue) -> Self {
        Issue {
            code: val.code.clone(),

            description: "".to_string(),

            solution: Solution::None,
        }
    }
}
