use std::cmp;
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

    /// Get the first available clock from this set.
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

    pub fn advance_to(&mut self, new_tick: u64) -> u64 {
        self.clock.advance_to(new_tick)
    }

    pub fn release(self) {
        // Do not call `self.clock.release()` here!
        //
        // `Drop` trait will automatically trigger `self.clock.release()`, when `self` goes out of
        // scope at the end of the method.
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

    pub fn advance_to(&self, new_tick: u64) -> u64 {
        let current_tick = self.clock.fetch_max(new_tick, Ordering::Relaxed);
        cmp::max(current_tick, new_tick)
    }

    pub fn lock(&self) -> bool {
        self.available.swap(false, Ordering::Relaxed)
    }

    pub fn release(&self) {
        self.available.store(true, Ordering::Relaxed);
    }
}
