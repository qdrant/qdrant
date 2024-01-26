use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, OnceLock};

use serde::{Serialize, Serializer};

use crate::issue::{CodeType, Issue};

#[derive(Default, Serialize)]
struct Dashboard {
    pub issues: HashMap<CodeType, Issue>,
}

impl Dashboard {
    /// Activates an issue, returning true if the issue was not active before
    fn add_issue(&mut self, issue: impl Into<Issue>) -> bool {
        let issue = issue.into();
        self.issues.insert(issue.code.clone(), issue).is_none()
    }

    /// Deactivates an issue by its code, returning true if the issue was active before
    fn remove_issue<S: AsRef<str>>(&mut self, code: S) -> bool {
        self.issues.remove(code.as_ref()).is_some()
    }
}

fn dashboard<'a>() -> MutexGuard<'a, Dashboard> {
    static DASHBOARD: OnceLock<Mutex<Dashboard>> = OnceLock::new();
    match DASHBOARD
        .get_or_init(|| Mutex::new(Dashboard::default()))
        // this is a blocking attempt to get the dashboard
        .lock()
    {
        Ok(guard) => guard,
        Err(e) => {
            log::warn!(
                "Mutex to the issue dashboard is poisoned, restarting dashboard: {}",
                e
            );

            // clear the dashboard
            let mut guard = e.into_inner();
            guard.issues.clear();

            guard
        }
    }
}

/// Submits an issue to the dashboard, returning true if the issue code was not active before
pub fn submit(issue: impl Into<Issue>) -> bool {
    dashboard().add_issue(issue)
}

/// Solves an issue by its code, returning true if the issue code was active before
pub fn solve<S: AsRef<str>>(code: S) -> bool {
    dashboard().remove_issue(code)
}

/// Generates serialized report of the issues in the dashboard, using the provided serializer
pub fn report<S: Serializer>(serializer: S) -> Result<S::Ok, S::Error> {
    dashboard().serialize(serializer)
}

pub fn all_issues() -> Vec<Issue> {
    dashboard().issues.values().cloned().collect()
}

/// Clears all issues from the dashboard
pub fn clear() {
    dashboard().issues.clear();
}

/// Solves (clears) all issues with the given prefix in their code
pub fn prefix_solve(prefix: &str) {
    dashboard()
        .issues
        .retain(|code, _issue| !code.starts_with(prefix));
}

/// Solves all issues whose code satisfy the predicate
pub fn filter_solve(f: impl Fn(&str) -> bool) {
    dashboard().issues.retain(|code, _issue| !f(code));
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use super::*;
    use crate::issue::DummyIssue;

    #[test]
    fn test_dashboard() {
        let mut dashboard = Dashboard::default();
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

            std::thread::sleep(std::time::Duration::from_millis(50));

            assert!(!submit(DummyIssue::new("issue4")));
        });

        let handle2 = std::thread::spawn(|| {
            submit(DummyIssue::new("issue4"));
            submit(DummyIssue::new("issue5"));
            submit(DummyIssue::new("issue6"));

            std::thread::sleep(std::time::Duration::from_millis(50));

            assert!(!submit(DummyIssue::new("issue1")));

            std::thread::sleep(std::time::Duration::from_millis(40));

            assert!(solve("issue1"));
            assert!(solve("issue2"));
            assert!(solve("issue3"));
            assert!(solve("issue4"));
            assert!(solve("issue5"));
            assert!(solve("issue6"));
        });

        std::thread::sleep(std::time::Duration::from_millis(140));

        handle1.join()?;
        handle2.join()?;
        Ok(())
    }

    #[test]
    #[serial]
    fn test_prefix_solve() {
        clear();
        submit(DummyIssue::new("issue1"));
        submit(DummyIssue::new("issue2"));
        submit(DummyIssue::new("other_prefix_issue3"));

        // solve all the issues starting with "issue"
        prefix_solve("issue");

        assert_eq!(dashboard().issues.len(), 1);
        assert!(dashboard()
            .issues
            .keys()
            .all(|code| !code.starts_with("issue")));
        clear();
    }

    #[test]
    #[serial]
    fn test_filter_solve() {
        clear();
        submit(DummyIssue::new("issue1"));
        submit(DummyIssue::new("issue2"));
        submit(DummyIssue::new("issue3"));

        filter_solve(|code| code.contains('2'));

        assert_eq!(dashboard().issues.len(), 2);
        assert!(dashboard().issues.keys().all(|code| !code.contains('2')));
        clear();
    }
}
