use std::collections::HashSet;

use crate::issue::{DummyIssue, Issue};

#[derive(Default)]
pub struct Dashboard {
    pub issues: HashSet<Box<dyn Issue>>,
}

impl Dashboard {
    pub fn add_issue(&mut self, issue: Box<dyn Issue>) {
        self.issues.insert(issue);
    }

    pub fn remove_issue(&mut self, code: String) {
        let issue: Box<dyn Issue> = Box::new(DummyIssue { code });
        self.issues.remove(&issue);
    }
}
