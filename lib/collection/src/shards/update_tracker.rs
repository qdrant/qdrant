use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Clone, Debug, Default)]
pub struct UpdateTracker {
    update_operations: Arc<AtomicUsize>,
}

impl UpdateTracker {
    pub fn is_update_in_progress(&self) -> bool {
        self.update_operations.load(Ordering::Relaxed) > 0
    }

    pub fn update(&self) -> UpdateGuard {
        UpdateGuard::new(self.update_operations.clone())
    }
}

#[derive(Debug)]
pub struct UpdateGuard {
    update_operations: Arc<AtomicUsize>,
}

impl UpdateGuard {
    fn new(update_operations: Arc<AtomicUsize>) -> Self {
        update_operations.fetch_add(1, Ordering::Relaxed);
        Self { update_operations }
    }
}

impl Drop for UpdateGuard {
    fn drop(&mut self) {
        self.update_operations.fetch_sub(1, Ordering::Relaxed);
    }
}
