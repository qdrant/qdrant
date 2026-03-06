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

    /// Creates a write-back counter for best performance possible.
    /// For more information on when and why to use, see [`WriteBackCounterCell`]
    #[inline]
    pub fn write_back_counter(&self) -> WritebackCounterCell<'_> {
        WritebackCounterCell::new(self)
    }
}

pub struct OptionalCounterCell<'a> {
    counter: Option<&'a CounterCell>,
}

impl<'a> OptionalCounterCell<'a> {
    #[inline]
    pub fn new(counter: Option<&'a CounterCell>) -> Self {
        Self { counter }
    }

    /// Returns the current value of the counter.
    #[inline]
    pub fn get(&self) -> usize {
        self.counter.map_or(0, |i| i.get())
    }

    /// Sets the value of the counter to `new_value`.
    #[inline]
    pub fn set(&self, new_value: usize) {
        if let Some(counter) = self.counter {
            counter.set(new_value);
        }
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
}

/// Performance optimized counter to measure hot-paths relieably. It accumulates it's current measurements
/// inside a `usize` and writes the result back into the original counter on drop.
///
///
/// ## Why and when should I use this instead of [`CounterCell`]?
///
/// The `CounterCell::incr_delta()` function is around twice as slow as counting a `usize` integer.
/// This is because we have to copy the cells value, do the arithmetic and write the value back.
/// Usually this is not a problem because it is still a very fast operation. In loops or hot-paths however
/// it can become a considerable overhead we want to avoid as much as possible.
///
/// You should always prefer this over manually counting a loop because we might lose values when returning
/// on an error, early-return or add such code in future and forget to adjust.
///
///
/// ## When to *not* use this?
///
/// Because this writeback counter only writes its values into the original cell when dropped, this is
/// not suitable if you directly need to read from a `CounterCell` within the same scope. This should
/// however avoided as much as possible, because these structures are for collecting measurements and should
/// be read from the initial [`HwMeasurementAcc`].
pub struct WritebackCounterCell<'a> {
    cell: &'a CounterCell,
    counter: usize,
}

impl Drop for WritebackCounterCell<'_> {
    #[inline]
    fn drop(&mut self) {
        self.cell.incr_delta(self.counter);
    }
}

impl<'a> WritebackCounterCell<'a> {
    #[inline]
    fn new(cell: &'a CounterCell) -> Self {
        Self { cell, counter: 0 }
    }

    #[inline]
    pub fn incr_delta(&mut self, delta: usize) {
        self.counter += delta;
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_write_back_counter() {
        let cell = CounterCell::new();

        {
            let mut wb_counter = cell.write_back_counter();
            wb_counter.incr_delta(4);
            assert_eq!(cell.get(), 0);
        }

        assert_eq!(cell.get(), 4);
    }
}
