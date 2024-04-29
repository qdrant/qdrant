#![doc = r#"# Issues

A logger-like module for logging issues and solutions to them.

## Usage

```
use issues;
use issues::{Issue, Solution};

pub(crate) struct DummyIssue {
    pub distinctive: String,
}

impl DummyIssue {
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

// Submit an issue
issues::submit(DummyIssue::new("issue1"));
assert!(!issues::submit(DummyIssue::new("issue1"))); // can't submit the same issue twice

// Solve an issue
issues::solve(DummyIssue::new("issue1").code()); // returns true if the issue was solved
assert!(issues::submit(DummyIssue::new("issue1"))); // Now we can submit it again
```"#]

pub mod broker;
mod dashboard;
mod issue;
pub mod problems;
mod solution;
pub(crate) mod typemap;

pub use broker::EventBroker;
pub use dashboard::{all_issues, clear, solve, solve_by_filter, submit};
pub use issue::{Issue, IssueRecord};
pub use solution::{Action, ImmediateSolution, Solution};
