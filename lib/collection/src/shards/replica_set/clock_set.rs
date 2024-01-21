use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Clone, Debug, Default)]
pub struct ClockSet {
    clocks: Vec<Arc<Clock>>,
}

impl ClockSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_clock(&mut self) -> ClockGuard {
        for (id, clock) in self.clocks.iter().enumerate() {
            if clock.lock() {
                return ClockGuard::new(id, clock.clone());
            }
        }

        let id = self.clocks.len();
        let clock = Arc::new(Clock::new_locked());

        self.clocks.push(clock.clone());

        ClockGuard::new(id, clock)
    }
}

#[derive(Debug)]
pub struct ClockGuard {
    id: usize,
    clock: Arc<Clock>,
}

impl ClockGuard {
    fn new(id: usize, clock: Arc<Clock>) -> Self {
        Self { id, clock }
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn tick_once(&mut self) -> u64 {
        self.clock.tick_once()
    }

    pub fn release(self) {
        // Do not call `Clock::release` explicitly!
        //
        // `Drop` will be called when `self` goes out of scope at the end of this method
        // and it will call `Clock::release`.
        //
        // If call `Clock::release` explicitly, then `ClockGuard::release` will call
        // `Clock::release` *twice*! (Which is a bug! :D)
    }
}

impl Drop for ClockGuard {
    fn drop(&mut self) {
        self.clock.release();
    }
}

#[derive(Debug)]
struct Clock {
    clock: AtomicU64,
    available: AtomicBool,
}

impl Clock {
    pub fn new_locked() -> Self {
        Self {
            clock: AtomicU64::new(0),
            available: AtomicBool::new(false),
        }
    }

    pub fn tick_once(&self) -> u64 {
        self.clock.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub fn lock(&self) -> bool {
        self.available.swap(false, Ordering::Relaxed)
    }

    pub fn release(&self) {
        self.available.store(true, Ordering::Relaxed);
    }
}
