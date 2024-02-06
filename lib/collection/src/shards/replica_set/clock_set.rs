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

    /// Get the first available clock from the set, or create a new one.
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

    /// Advance clock by a single tick and return current tick.
    #[must_use = "new clock value must be used"]
    pub fn tick_once(&mut self) -> u64 {
        self.clock.tick_once()
    }

    /// Advance clock to `new_tick`, if `new_tick` is newer than current tick.
    pub fn advance_to(&mut self, new_tick: u64) {
        self.clock.advance_to(new_tick)
    }
}

impl Drop for ClockGuard {
    fn drop(&mut self) {
        self.clock.release();
    }
}

#[derive(Debug)]
struct Clock {
    /// Tracks the *next* clock tick
    next_tick: AtomicU64,
    available: AtomicBool,
}

impl Clock {
    fn new_locked() -> Self {
        Self {
            next_tick: AtomicU64::new(0),
            available: AtomicBool::new(false),
        }
    }

    /// Advance clock by a single tick and return current tick
    #[must_use = "new clock tick value must be used"]
    fn tick_once(&self) -> u64 {
        // `Clock` tracks *next* tick, so we increment `next_tick` by 1 and return *previous* value
        // of `next_tick` (which is *exactly* what `fetch_add(1)` does)
        self.next_tick.fetch_add(1, Ordering::Relaxed)
    }

    /// Advance clock to `new_tick`, if `new_tick` is newer than current tick
    fn advance_to(&self, new_tick: u64) {
        // `Clock` tracks *next* tick, so if we want to advance *current* tick to `new_tick`,
        // we have to advance `next_tick` to `new_tick + 1`
        self.next_tick.fetch_max(new_tick + 1, Ordering::Relaxed);
    }

    fn lock(&self) -> bool {
        self.available.swap(false, Ordering::Relaxed)
    }

    fn release(&self) {
        self.available.store(true, Ordering::Relaxed);
    }
}
