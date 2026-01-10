use std::sync::Arc;

/// A counter to track the number of active processes.
///
/// Conceptually, similar to a simple integer counter, e.g.:
/// ```ignore
/// let mut counter: usize = 0;
/// counter += 1; // on process start
/// counter -= 1; // on process end
/// ```
/// but decrements on drop, making it impossible to forget decreasing the counter.
/// ```ignore
/// let mut counter = ProcessCounter::default();
/// let handle = counter.inc(); // on process start
/// drop(handle); // on process end
/// ```
#[derive(Debug, Default)] // No `Clone` as it will break the counting logic.
pub struct ProcessCounter(Arc<()>);

/// A handle that decrements [`ProcessCounter`] when dropped.
#[derive(Debug)] // No `Clone` as it will break the counting logic.
#[must_use = "Dropping this handle will immediately decrease the process count"]
#[expect(dead_code, reason = "The inner value is used to maintain ref count")]
pub struct CountedProcessHandle(Arc<()>);

impl ProcessCounter {
    /// The current counter value.
    pub fn count(&self) -> usize {
        Arc::strong_count(&self.0).saturating_sub(1)
    }

    /// Increments the counter and returns a handle.
    /// When the handle is dropped, the counter decrements automatically.
    pub fn inc(&mut self) -> CountedProcessHandle {
        // NOTE: `&mut` is not required, but deliberately added to forbid
        // calling it from `RwLockReadGuard`.
        CountedProcessHandle(Arc::clone(&self.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_counter() {
        let mut counter = ProcessCounter::default();
        assert_eq!(counter.count(), 0);

        let p1 = counter.inc();
        assert_eq!(counter.count(), 1);

        let p2 = counter.inc();
        assert_eq!(counter.count(), 2);

        let p3 = counter.inc();
        assert_eq!(counter.count(), 3);
        _ = std::panic::catch_unwind(move || {
            let _p3 = p3;
            panic!();
        });
        assert_eq!(counter.count(), 2);

        drop(p1);
        assert_eq!(counter.count(), 1);

        drop(p2);
        assert_eq!(counter.count(), 0);
    }
}
