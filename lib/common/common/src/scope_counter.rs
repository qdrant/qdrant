use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// An RAII-style counter that tracks the number of active scopes.
/// Internally uses a reference-counted value, allowing it to be freely cloned.
#[derive(Default, Debug, Clone)]
pub struct ScopeCounter {
    inner: Arc<AtomicUsize>,
}

impl ScopeCounter {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Measures the scope, the counter should keep track of.
    /// Must always be bound to a variable, to not get dropped prematurely!
    #[must_use]
    pub fn measure_scope(&self) -> ScopeCounterGuard {
        ScopeCounterGuard::measure(self)
    }

    /// Get the current value of the counter.
    pub fn get(&self, ordering: Ordering) -> usize {
        self.inner.load(ordering)
    }
}

const COUNT_SIZE: usize = 1;

/// Guard type for [`ScopeCounter`], that must be hold for the entire duration of a scope.
/// This type is in charge of correctly counting the passed counter.
pub struct ScopeCounterGuard {
    scope_counter: ScopeCounter,
}

impl ScopeCounterGuard {
    #[must_use]
    fn measure(scope_counter: &ScopeCounter) -> Self {
        let scope_counter = scope_counter.clone();
        scope_counter.inner.fetch_add(COUNT_SIZE, Ordering::SeqCst);
        Self { scope_counter }
    }
}

impl Drop for ScopeCounterGuard {
    fn drop(&mut self) {
        self.scope_counter
            .inner
            .fetch_sub(COUNT_SIZE, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread::{self, JoinHandle};
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_scope_counter() {
        let counter = ScopeCounter::new();

        {
            let _measure_guard = counter.measure_scope();
            assert_eq!(counter.get(Ordering::SeqCst), 1);
        }

        assert_eq!(counter.get(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_scope_counter_loop() {
        let counter = ScopeCounter::new();

        for _ in 0..100 {
            let _measure_guard = counter.measure_scope();
            assert_eq!(counter.get(Ordering::SeqCst), 1);
        }

        assert_eq!(counter.get(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_scope_counter_threads() {
        let counter = ScopeCounter::new();

        let run = Arc::new(AtomicBool::new(true));
        let mut handles: Vec<JoinHandle<()>> = vec![];
        let started_threads = Arc::new(AtomicUsize::new(0));

        const LEN: usize = 20;

        for _ in 0..LEN {
            let counter_clone = counter.clone();
            let run_clone = run.clone();
            let started_threads_clone = started_threads.clone();
            let handle = thread::spawn(move || {
                let _guard = counter_clone.measure_scope();

                started_threads_clone.fetch_add(1, Ordering::Relaxed);

                while run_clone.load(Ordering::Relaxed) {
                    thread::sleep(Duration::from_secs(1));
                }
            });
            handles.push(handle);
        }

        // Wait until all threads have started.
        // To prevent this test becoming flaky by waiting a constant amount of time, we use an atomic counter here.
        while started_threads.load(Ordering::Relaxed) < LEN {
            thread::sleep(Duration::from_secs(1));
        }

        assert_eq!(counter.get(Ordering::SeqCst), LEN);

        // Stop spawned threads
        run.store(false, Ordering::Release);

        // Wait for them to gracefully finish.
        for handle in handles {
            handle.join().unwrap();
        }
    }
}
