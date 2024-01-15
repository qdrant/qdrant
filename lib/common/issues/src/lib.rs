//! # Issues
//!
//! A logger-like module for logging issues and solutions to them.
//!
//! ## Usage
//!
//! ```
//! use issues;
//! use issues::{Issue, Solution};
//!
//! pub(crate) struct DummyIssue {
//!   pub code: String,
//! }
//!
//! impl DummyIssue {
//!     pub fn new(code: impl Into<String>) -> Self {
//!         Self { code: code.into() }
//!     }
//! }
//!
//! impl Issue for DummyIssue {
//!     fn code(&self) -> String {
//!         self.code.clone()
//!     }
//!
//!     fn description(&self) -> String {
//!         "".to_string()
//!     }
//!
//!     fn solution(&self) -> Result<Solution, String> {
//!         Ok(Solution {
//!             message: "".to_string(),
//!             action: None,
//!         })
//!     }
//! }
//!
//! // Submit an issue
//! issues::submit(Box::new(DummyIssue::new("issue1")));
//! assert!(!issues::submit(Box::new(DummyIssue::new("issue1")))); // can't submit the same issue twice
//!
//! // Solve an issue
//! issues::solve("issue1".to_string()); // returns true if the issue was solved
//! assert!(issues::submit(Box::new(DummyIssue::new("issue1")))); // Now we can submit it again
//! ```

mod dashboard;
mod issue;
pub mod problems;
mod solution;

pub use dashboard::{clear, filter_solve, prefix_solve, report, solve, submit};
pub use issue::Issue;
pub use solution::Solution;
