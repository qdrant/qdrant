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
            if clock.try_lock() {
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
            next_tick: 0.into(),
            available: false.into(),
        }
    }

    /// Advance clock by a single tick and return current tick.
    ///
    /// # Thread safety
    ///
    /// Clock *has to* be locked (using [`Clock::lock`]) before calling `tick_once`!
    #[must_use = "new clock tick value must be used"]
    fn tick_once(&self) -> u64 {
        // `Clock` tracks *next* tick, so we increment `next_tick` by 1 and return *previous* value
        // of `next_tick` (which is *exactly* what `fetch_add(1)` does)
        let current_tick = self.next_tick.fetch_add(1, Ordering::Relaxed);

        // If `current_tick` is `0`, then "revert" `next_tick` back to `0`.
        // We expect that `advance_to` would be used to advance "past" the initial `0`.
        //
        // Executing multiple atomic operations sequentially is not strictly thread-safe,
        // but we expect that `Clock` would be "locked" before calling `tick_once`.
        if current_tick == 0 {
            self.next_tick.store(0, Ordering::Relaxed);
        }

        current_tick
    }

    /// Advance clock to `new_tick`, if `new_tick` is newer than current tick.
    fn advance_to(&self, new_tick: u64) {
        // `Clock` tracks *next* tick, so if we want to advance *current* tick to `new_tick`,
        // we have to advance `next_tick` to `new_tick + 1`
        self.next_tick.fetch_max(new_tick + 1, Ordering::Relaxed);
    }

    /// Try to acquire exclusive lock over this clock.
    ///
    /// Returns `true` if the lock was successfully acquired, or `false` if the clock is already
    /// locked.
    fn try_lock(&self) -> bool {
        self.available.swap(false, Ordering::Relaxed)
    }

    /// Release the exclusive lock over this clock.
    ///
    /// No-op if the clock is not locked.
    fn release(&self) {
        self.available.store(true, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tick a single clock, it should increment after we advance it from 0 (or higher).
    #[test]
    fn test_clock_set_single_tick() {
        let mut clock_set = ClockSet::new();

        // Don't tick from 0 unless we explicitly advance
        assert_eq!(clock_set.get_clock().tick_once(), 0);
        assert_eq!(clock_set.get_clock().tick_once(), 0);
        assert_eq!(clock_set.get_clock().tick_once(), 0);

        clock_set.get_clock().advance_to(0);

        // Following ticks should increment
        assert_eq!(clock_set.get_clock().tick_once(), 1);
        assert_eq!(clock_set.get_clock().tick_once(), 2);
        assert_eq!(clock_set.get_clock().tick_once(), 3);
        assert_eq!(clock_set.get_clock().tick_once(), 4);
    }
}
