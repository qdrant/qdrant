use std::cell::Cell;

/// A simple and efficient counter which doesn't need to be mutable for counting.
///
/// It however cannot be shared across threads safely and thus doesn't implement `Sync` or `Send`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CounterCell {
    counter: Cell<usize>,
}

impl CounterCell {
    /// Creates a new `CounterCell` with 0 as initial value.
    pub fn new() -> Self {
        Self::new_with(0)
    }

    /// Creates a new `CounterCell` with a custom initial value.
    pub fn new_with(init: usize) -> Self {
        Self {
            counter: Cell::new(init),
        }
    }

    /// Returns the current value of the counter.
    #[inline]
    pub fn get(&self) -> usize {
        self.counter.get()
    }

    /// Sets the value of the counter to `new_value`.
    #[inline]
    pub fn set(&self, new_value: usize) {
        self.counter.set(new_value);
    }

    /// Increases the counter by 1.
    /// If you have mutable access to the counter, prefer `incr_mut` over this method.
    #[inline]
    pub fn incr(&self) {
        self.incr_delta(1);
    }

    /// Increases the counter by `delta`.
    /// If you have mutable access to the counter, prefer `incr_delta_mut` over this method.
    #[inline]
    pub fn incr_delta(&self, delta: usize) {
        self.set(self.get() + delta);
    }

    /// Multiply the counters value by `amount`.
    #[inline]
    pub fn multiplied(&self, amount: usize) {
        self.set(self.get() * amount)
    }

    /// Resets the counter to 0.
    pub fn clear(&self) {
        self.counter.set(0);
    }

    /// Takes the value of the counter, leaving 0 in its place.
    pub fn take(&self) -> usize {
        self.counter.take()
    }
}
