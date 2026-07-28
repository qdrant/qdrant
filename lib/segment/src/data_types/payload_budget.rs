use std::cell::RefCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::common::operation_error::OperationError;

#[derive(Debug)]
pub struct PayloadBudget {
    pub max_bytes: usize,
    used_bytes: AtomicUsize,
}

impl PayloadBudget {
    pub fn new(max_bytes: usize) -> Self {
        Self { max_bytes, used_bytes: AtomicUsize::new(0) }
    }

    /// Try to account for `bytes`. Returns true if within budget, false if exceeded.
    pub fn add(&self, bytes: usize) -> bool {
        let prev = self.used_bytes.fetch_add(bytes, Ordering::Relaxed);
        prev.saturating_add(bytes) <= self.max_bytes
    }

    pub fn used(&self) -> usize { self.used_bytes.load(Ordering::Relaxed) }
}

thread_local! {
    static TLS_BUDGET: RefCell<Option<Arc<PayloadBudget>>> = RefCell::new(None);
}

/// Set per-thread payload budget (used during filter evaluation).
pub fn set_thread_payload_budget(budget: Option<Arc<PayloadBudget>>) {
    TLS_BUDGET.with(|c| *c.borrow_mut() = budget);
}

/// Try to consume `bytes` from the current thread's budget, if present.
pub fn try_consume(bytes: usize) -> Result<(), OperationError> {
    TLS_BUDGET.with(|c| {
        if let Some(b) = c.borrow().as_ref() {
            if !b.add(bytes) {
                return Err(OperationError::service_error_light("payload memory budget exceeded"));
            }
        }
        Ok(())
    })
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
