use std::sync::atomic::{AtomicU8, Ordering};

const MAX_RECENCY: u8 = 3;

/// Entry stored in the small and main queues.
#[derive(Debug)]
pub struct Entry<K, V> {
    pub key: K,
    pub value: V,
    recency: AtomicU8,
}

impl<K, V> Entry<K, V> {
    pub fn new(key: K, value: V) -> Self {
        Self {
            key,
            value,
            recency: AtomicU8::new(0),
        }
    }

    pub fn clone(&self) -> Self
    where
        K: Copy,
        V: Clone,
    {
        Self {
            key: self.key,
            value: self.value.clone(),
            recency: AtomicU8::new(self.recency.load(Ordering::Relaxed)),
        }
    }

    #[inline]
    pub fn recency(&self) -> u8 {
        self.recency.load(Ordering::Relaxed)
    }

    /// Increment recency by 1, saturating at `MAX_RECENCY`
    #[inline]
    pub fn incr_recency(&self) {
        let current = self.recency.load(Ordering::Relaxed);
        if current < MAX_RECENCY {
            self.recency.store(current + 1, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn decr_recency(&self) {
        let current = self.recency.load(Ordering::Relaxed);
        if current > 0 {
            self.recency.store(current - 1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_recency() {
        let entry = Entry::new(1u64, "test".to_string());

        assert_eq!(entry.recency(), 0);
        entry.incr_recency();
        assert_eq!(entry.recency(), 1);
        entry.incr_recency();
        entry.incr_recency();
        entry.incr_recency(); // Should saturate at 3
        assert_eq!(entry.recency(), 3);

        entry.decr_recency();
        assert_eq!(entry.recency(), 2);
    }
}
