use std::hash::{Hash, Hasher};

use crate::solution::Solution;

/// An issue that can be hashed by its code
pub trait Issue {
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

impl Eq for dyn Issue {}

pub(crate) struct DummyIssue {
    pub code: String,
}

impl Issue for DummyIssue {
    fn code(&self) -> String {
        self.code.to_string()
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
