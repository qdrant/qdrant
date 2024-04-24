use std::fmt::Debug;

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::Serialize;

use crate::dashboard::Code;
use crate::solution::Solution;

pub trait Issue {
    /// Differentiates issues of the same type
    fn distinctive(&self) -> &str;

    /// The codename for all issues of this type
    fn name() -> &'static str;

    /// A human-readable description of the issue
    fn description(&self) -> String;

    /// Actionable solution to the issue
    fn solution(&self) -> Solution;

    /// Internal code for the issue
    fn code(&self) -> Code
    where
        Self: std::marker::Sized + 'static,
    {
        Code::new::<Self>(self.distinctive())
    }

    /// Submits the issue to the dashboard singleton
    fn submit(self) -> bool
    where
        Self: std::marker::Sized + 'static,
    {
        crate::dashboard::submit(self)
    }
}

/// An issue that can be identified by its code
#[derive(Debug, Serialize, JsonSchema, Clone)]
pub struct IssueRecord {
    pub id: String,
    pub description: String,
    pub solution: Solution,
    pub timestamp: DateTime<Utc>,
}

impl<I: Issue> From<I> for IssueRecord {
    fn from(val: I) -> Self {
        let id = format!("{}/{}", I::name(), val.distinctive());
        Self {
            id,
            description: val.description(),
            solution: val.solution(),
            timestamp: Utc::now(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct DummyIssue {
    pub distinctive: String,
}

impl DummyIssue {
    #[cfg(test)]
    pub fn new(distinctive: impl Into<String>) -> Self {
        Self {
            distinctive: distinctive.into(),
        }
    }
}

impl Issue for DummyIssue {
    fn distinctive(&self) -> &str {
        &self.distinctive
    }

    fn name() -> &'static str {
        "DUMMY"
    }

    fn description(&self) -> String {
        "".to_string()
    }

    fn solution(&self) -> Solution {
        Solution::Refactor("".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_issue_record() {
        let issue = DummyIssue::new("test");

        let record = IssueRecord::from(issue);

        assert_eq!(record.id, "DUMMY/test");
    }
}
