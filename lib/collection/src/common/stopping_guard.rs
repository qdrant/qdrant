use std::sync::atomic::AtomicBool;
use std::sync::Arc;

/// Structure that ensures that `is_stopped` flag is set to `true` when dropped.
pub struct StoppingGuard {
    is_stopped: Arc<AtomicBool>,
}

impl StoppingGuard {
    /// Creates a new `StopGuard` instance.
    pub fn new() -> Self {
        Self {
            is_stopped: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn check_is_stopped(&self) -> bool {
        self.is_stopped.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn stop(&self) {
        self.is_stopped
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn get_is_stopped(&self) -> Arc<AtomicBool> {
        self.is_stopped.clone()
    }
}

impl Default for StoppingGuard {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for StoppingGuard {
    fn drop(&mut self) {
        self.is_stopped
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
}
