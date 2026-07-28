use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
pub struct PayloadBudget {
    pub max_bytes: usize,
    used_bytes: AtomicUsize,
}

impl PayloadBudget {
    pub fn new(max_bytes: usize) -> Self { Self { max_bytes, used_bytes: AtomicUsize::new(0) } }
    pub fn add(&self, bytes: usize) -> bool {
        let prev = self.used_bytes.fetch_add(bytes, Ordering::Relaxed);
        prev.saturating_add(bytes) <= self.max_bytes
    }
    pub fn used(&self) -> usize { self.used_bytes.load(Ordering::Relaxed) }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn budget_addition_bounds() {
        let b = PayloadBudget::new(1024);
        assert!(b.add(512));
        assert!(!b.add(600));
        assert!(b.used() >= 1112);
    }
}
