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
    use std::sync::atomic::Ordering;

    use super::*;

    #[test]
    fn test_scope_counter() {
        let counter = ScopeCounter::new();

        {
            let _measure_guard = counter.measure_scope();
        }

        assert_eq!(counter.get(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_scope_counter_loop() {
        let counter = ScopeCounter::new();

        const LEN: usize = 100;

        for _ in 0..LEN {
            let _measure_guard = counter.measure_scope();
        }

        assert_eq!(counter.get(Ordering::SeqCst), LEN);
    }
}
