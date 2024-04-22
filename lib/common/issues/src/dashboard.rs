use std::any::TypeId;
use std::collections::HashSet;
use std::sync::{Arc, OnceLock};

use dashmap::DashMap;

use crate::issue::{CodeType, Issue, IssueRecord};

#[derive(Default)]
struct Dashboard {
    pub issues: DashMap<CodeType, IssueRecord>,
    inverted_index: DashMap<TypeId, HashSet<CodeType>>,
}

impl Dashboard {
    /// Activates an issue, returning true if the issue was not active before
    fn add_issue<I: Issue + 'static>(&self, issue: I) -> bool {
        let code = issue.code();
        if self.issues.contains_key(&code) {
            return false;
        }
        let issue = IssueRecord::from(issue);
        self.inverted_index
            .entry(TypeId::of::<I>())
            .or_default()
            .insert(code.clone());
        self.issues.insert(code, issue).is_none()
    }

    /// Deactivates an issue by its code, returning true if the issue was active before
    fn remove_issue<S: AsRef<str>>(&self, code: S) -> bool {
        if self.issues.contains_key(code.as_ref()) {
            self.inverted_index
                .entry(TypeId::of::<IssueRecord>())
                .and_modify(|codes| {
                    codes.remove(code.as_ref());
                });
            return self.issues.remove(code.as_ref()).is_some();
        }
        false
    }

    /// Returns all issues in the dashboard. This operation clones every issue, so it is more expensive.
    fn get_all_issues(&self) -> Vec<IssueRecord> {
        self.issues.iter().map(|kv| kv.value().clone()).collect()
    }

    fn get_codes<I: 'static>(&self) -> HashSet<CodeType> {
        let type_id = TypeId::of::<I>();
        if let Some(codes) = self.inverted_index.get(&type_id) {
            return codes.clone();
        }
        Default::default()
    }
}

fn dashboard() -> Arc<Dashboard> {
    static DASHBOARD: OnceLock<Arc<Dashboard>> = OnceLock::new();
    DASHBOARD
        .get_or_init(|| Arc::new(Dashboard::default()))
        .clone()
}

/// Submits an issue to the dashboard, returning true if the issue code was not active before
pub fn submit(issue: impl Issue + 'static) -> bool {
    dashboard().add_issue(issue)
}

/// Solves an issue by its code, returning true if the issue code was active before
pub fn solve<S: AsRef<str>>(code: S) -> bool {
    dashboard().remove_issue(code)
}

pub fn all_issues() -> Vec<IssueRecord> {
    dashboard().get_all_issues()
}

/// Clears all issues from the dashboard
pub fn clear() {
    dashboard().issues.clear();
}

/// Solves all issues that match the given predicate
pub fn solve_by_filter<I: Issue + 'static, F: Fn(&CodeType) -> bool>(filter: F) {
    let codes = dashboard().get_codes::<I>();
    for code in codes {
        if filter(&code) {
            solve(code);
        }
    }
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use super::*;
    use crate::issue::DummyIssue;

    #[test]
    fn test_dashboard() {
        let dashboard = Dashboard::default();
        let issue = DummyIssue {
            code: "test".to_string(),
        };
        assert!(dashboard.add_issue(issue.clone()));
        assert!(!dashboard.add_issue(issue.clone()));
        assert!(dashboard.remove_issue("test"));
        assert!(!dashboard.remove_issue("test"));
    }

    #[test]
    #[serial]
    fn test_singleton() -> std::thread::Result<()> {
        clear();

        let handle1 = std::thread::spawn(|| {
            submit(DummyIssue::new("issue1"));
            submit(DummyIssue::new("issue2"));
            submit(DummyIssue::new("issue3"));
        });

        let handle2 = std::thread::spawn(|| {
            submit(DummyIssue::new("issue4"));
            submit(DummyIssue::new("issue5"));
            submit(DummyIssue::new("issue6"));
        });

        handle1.join()?;
        handle2.join()?;

        assert_eq!(all_issues().len(), 6);
        assert!(solve("issue1"));
        assert!(solve("issue2"));
        assert!(solve("issue3"));
        assert!(solve("issue4"));
        assert!(solve("issue5"));
        assert!(solve("issue6"));

        clear();
        Ok(())
    }

    #[test]
    #[serial]
    fn test_solve_by_filter() {
        crate::clear();

        submit(DummyIssue::new("DUMMY:my_collection:issue1"));
        submit(DummyIssue::new("DUMMY:my_collection:issue2"));
        submit(DummyIssue::new("DUMMY:my_collection:issue3"));
        submit(DummyIssue::new("DUMMY_2:issue2"));
        submit(DummyIssue::new("DUMMY_2:issue2"));
        submit(DummyIssue::new("issue2"));
        submit(DummyIssue::new("issue3"));

        // Solve all dummy issues that contain "my_collection"
        solve_by_filter::<DummyIssue, _>(|code| code.contains("my_collection"));
        assert_eq!(all_issues().len(), 3);
        assert!(solve("issue2"));
    }
}
