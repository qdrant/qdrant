use std::sync::atomic::{AtomicU64, Ordering};

const NONE_VALUE: u64 = 0;

/// Atomic Option<u64>. The max value we can store is u64::MAX-1 because 0 is preserved to represent `None`.
pub struct AtomicOptionU64 {
    inner: AtomicU64,
}

impl AtomicOptionU64 {
    /// Creates a new Atomic Option<u64>, with None as initial value.
    pub fn new() -> Self {
        Self::default()
    }

    /// Stores a value into the atomic.
    ///
    /// Note: Since this wrapper only supports values < u64::MAX, values higher than this limit will be saturated, and in debug this will panic.
    pub fn store(&self, value: Option<u64>, ordering: Ordering) {
        if let Some(value) = value {
            if value == u64::MAX {
                debug_assert!(
                    false,
                    "Value for AtomicOptionU64 must be smaller than u64::MAX!"
                );
            }

            // 0 is preserved for `None` so we increment original value by an offset of 1.
            let inner_val = value.saturating_add(1);
            self.inner.store(inner_val, ordering);
        } else {
            self.inner.store(NONE_VALUE, ordering);
        }
    }

    pub fn load(&self, ordering: Ordering) -> Option<u64> {
        let real_val = self.inner.load(ordering);
        if real_val == NONE_VALUE {
            None
        } else {
            Some(real_val - 1)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_atomic_option_u64() {
        let val = AtomicOptionU64::new();

        // Assert that ::new() creates an atomic option u64 with `None` as value.
        assert_eq!(val.load(Ordering::Relaxed), None);

        // Assert that setting to `None` keeps this value.
        val.store(None, Ordering::Relaxed);
        assert_eq!(val.load(Ordering::Relaxed), None);

        // Assert setting value.
        val.store(Some(42), Ordering::Relaxed);
        assert_eq!(val.load(Ordering::Relaxed), Some(42));

        // Assert setting to none with Value.
        val.store(None, Ordering::Relaxed);
        assert_eq!(val.load(Ordering::Relaxed), None);
    }
}

impl std::fmt::Debug for AtomicOptionU64 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.load(Ordering::Relaxed))
    }
}

impl Default for AtomicOptionU64 {
    fn default() -> Self {
        Self {
            inner: AtomicU64::new(NONE_VALUE),
        }
    }
}
