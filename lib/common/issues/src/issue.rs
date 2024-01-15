use std::hash::{Hash, Hasher};

use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};

use crate::solution::Solution;

/// An issue that can be hashed by its code
pub trait Issue: Sync + Send {
    fn code(&self) -> String;
    fn description(&self) -> String;
    fn solution(&self) -> Result<Solution, String>;
}

impl Hash for dyn Issue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.code().hash(state);
    }
}

impl PartialEq for dyn Issue {
    fn eq(&self, other: &Self) -> bool {
        self.code() == other.code()
    }
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

impl Eq for dyn Issue {}

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

    fn solution(&self) -> Result<Solution, String> {
        Ok(Solution {
            message: "".to_string(),
            action: None,
        })
    }
}
