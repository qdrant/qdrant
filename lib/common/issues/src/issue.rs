use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};

use crate::solution::Solution;

/// Type of the issue code
pub type CodeType = String;

/// An issue that can be hashed by its code
pub trait Issue: Sync + Send {
    fn code(&self) -> CodeType;
    fn description(&self) -> String;
    fn solution(&self) -> Solution;
}

impl Serialize for dyn Issue {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(3))?;
        map.serialize_entry("code", &self.code())?;
        map.serialize_entry("description", &self.description())?;
        map.serialize_entry("solution", &self.solution())?;
        map.end()
    }
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

impl Issue for DummyIssue {
    fn code(&self) -> String {
        self.code.clone()
    }

    fn description(&self) -> String {
        "".to_string()
    }

    fn solution(&self) -> Solution {
        Solution::None
    }
}
