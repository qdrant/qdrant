use std::collections::HashSet;
use std::sync::{Mutex, MutexGuard, OnceLock};

use serde::{Serialize, Serializer};

use crate::issue::{DummyIssue, Issue};

#[derive(Default, Serialize)]
struct Dashboard {
    pub issues: HashSet<Box<dyn Issue>>,
}

impl Dashboard {
    /// Activates an issue, returning true if the issue was not active before
    fn add_issue(&mut self, issue: Box<dyn Issue>) -> bool {
        self.issues.insert(issue)
    }

    /// Deactivates an issue by its code, returning true if the issue was active or not
    fn remove_issue(&mut self, code: String) -> bool {
        let issue: Box<dyn Issue> = Box::new(DummyIssue { code });
        self.issues.remove(&issue)
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
pub fn submit(issue: Box<dyn Issue>) -> bool {
    dashboard().add_issue(issue)
}

/// Solves an issue by its code, returning true if the issue code was active before
pub fn solve(code: String) -> bool {
    dashboard().remove_issue(code)
}

/// Generates serialized report of the issues in the dashboard, using the provided serializer
pub fn report<S: Serializer>(serializer: S) -> Result<S::Ok, S::Error> {
    dashboard().serialize(serializer)
}

/// Clears all issues from the dashboard
pub fn clear() {
    dashboard().issues.clear();
}

/// Solves (clears) all issues with the given prefix in their code
pub fn prefix_solve(prefix: &str) {
    dashboard()
        .issues
        .retain(|issue| !issue.code().starts_with(prefix));
}

/// Solves all issues whose code satisfy the predicate
pub fn filter_solve(f: impl Fn(&str) -> bool) {
    dashboard().issues.retain(|issue| !f(&issue.code()));
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use super::*;

    #[test]
    fn test_dashboard() {
        let mut dashboard = Dashboard::default();
        let issue = Box::new(DummyIssue {
            code: "test".to_string(),
        });
        assert!(dashboard.add_issue(issue.clone()));
        assert!(!dashboard.add_issue(issue.clone()));
        assert!(dashboard.remove_issue("test".to_string()));
        assert!(!dashboard.remove_issue("test".to_string()));
    }

    #[test]
    #[serial]
    fn test_singleton() -> std::thread::Result<()> {
        clear();
        let handle1 = std::thread::spawn(|| {
            submit(Box::new(DummyIssue::new("issue1")));
            submit(Box::new(DummyIssue::new("issue2")));
            submit(Box::new(DummyIssue::new("issue3")));

            std::thread::sleep(std::time::Duration::from_millis(50));

            assert!(!submit(Box::new(DummyIssue::new("issue4"))));
        });

        let handle2 = std::thread::spawn(|| {
            submit(Box::new(DummyIssue::new("issue4")));
            submit(Box::new(DummyIssue::new("issue5")));
            submit(Box::new(DummyIssue::new("issue6")));

            std::thread::sleep(std::time::Duration::from_millis(50));

            assert!(!submit(Box::new(DummyIssue::new("issue1"))));

            std::thread::sleep(std::time::Duration::from_millis(10));

            assert!(solve("issue1".to_string()));
            assert!(solve("issue2".to_string()));
            assert!(solve("issue3".to_string()));
            assert!(solve("issue4".to_string()));
            assert!(solve("issue5".to_string()));
            assert!(solve("issue6".to_string()));
        });

        std::thread::sleep(std::time::Duration::from_millis(70));

        handle1.join()?;
        handle2.join()?;
        Ok(())
    }

    #[test]
    #[serial]
    fn test_prefix_solve() {
        clear();
        submit(Box::new(DummyIssue::new("issue1")));
        submit(Box::new(DummyIssue::new("issue2")));
        submit(Box::new(DummyIssue::new("other_prefix_issue3")));

        // solve all the issues starting with "issue"
        prefix_solve("issue");

        assert_eq!(dashboard().issues.len(), 1);
        assert!(dashboard()
            .issues
            .iter()
            .all(|issue| !issue.code().starts_with("issue")));
        clear();
    }

    #[test]
    #[serial]
    fn test_filter_solve() {
        clear();
        submit(Box::new(DummyIssue::new("issue1")));
        submit(Box::new(DummyIssue::new("issue2")));
        submit(Box::new(DummyIssue::new("issue3")));

        filter_solve(|code| code.contains('2'));

        assert_eq!(dashboard().issues.len(), 2);
        assert!(dashboard()
            .issues
            .iter()
            .all(|issue| !issue.code().contains('2')));
        clear();
    }
}
