use std::cell::Cell;

use crate::counter::counter_cell::CounterCell;

/// Collection of different types of hardware measurements.
#[derive(Debug)]
pub struct HardwareCounterCell {
    cpu_counter: CounterCell,
    checked: Cell<bool>,
}

impl HardwareCounterCell {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_with(cpu: usize) -> Self {
        Self {
            cpu_counter: CounterCell::new_with(cpu),
            checked: Cell::new(true),
        }
    }

    /// Don't check wether counter got full consumed on dropping.
    pub fn unchecked(mut self) -> Self {
        *self.checked.get_mut() = false;
        self
    }

    pub fn set_checked(&self, checked: bool) {
        self.checked.set(checked)
    }

    #[inline]
    pub fn cpu_counter(&self) -> &CounterCell {
        &self.cpu_counter
    }

    #[inline]
    pub fn cpu_counter_mut(&mut self) -> &mut CounterCell {
        &mut self.cpu_counter
    }

    /// Accumulates the measurements from `other` into this counter.
    /// This consumes `other`, leaving it with zeroed metrics.
    pub fn apply_from(&self, other: &HardwareCounterCell) {
        let HardwareCounterCell {
            cpu_counter,
            checked: _,
        } = other;

        self.cpu_counter.incr_delta(cpu_counter.get());

        other.cpu_counter.set(0);
    }

    /// Sets the currents counter values to the ones in `other`.
    /// This consumes `other`, leaving it with zeroed metrics.
    pub fn set_from(&self, other: &HardwareCounterCell) {
        let HardwareCounterCell {
            cpu_counter,
            checked: _,
        } = other;

        self.cpu_counter.set(cpu_counter.get());
    }

    /// Returns `true` if at least one metric has non-consumed values.
    pub fn has_values(&self) -> bool {
        let HardwareCounterCell {
            cpu_counter,
            checked: _,
        } = self;

        cpu_counter.get() > 0
    }

    /// Resets all measurements in this hardware counter.
    pub fn clear(&self) {
        let HardwareCounterCell {
            cpu_counter,
            checked: _,
        } = self;

        cpu_counter.clear();
    }

    pub fn take(&self) -> HardwareCounterCell {
        let new_counter = HardwareCounterCell {
            cpu_counter: self.cpu_counter.clone(),
            checked: self.checked.clone(),
        };

        self.clear();

        new_counter
    }

    /// Drops the hardware counter controlled and discards all values if any.
    /// Since we only chechk in tests+debug mode for correct consumption of the values,
    /// this is a no-op in release mode.
    pub fn drop_and_discard_results(self) {
        #[cfg(any(debug_assertions, test))]
        self.clear();
    }

    /// Sets the status of this hardware counter to consumed to not panic on drop in debug build or tests.
    /// This is currently equal to calling `.clear()` should be preferred if the goal is to prevent
    /// panics in tests eg. after manually 'consuming' the counted values.
    pub fn discard_results(&self) {
        self.clear();
    }
}

impl Default for HardwareCounterCell {
    fn default() -> Self {
        Self {
            cpu_counter: Default::default(),
            checked: Cell::new(true),
        }
    }
}

#[cfg(any(debug_assertions, test))] // We want this to fail in both, release and debug tests
impl Drop for HardwareCounterCell {
    fn drop(&mut self) {
        if self.checked.get() && self.has_values() {
            panic!("Checked HardwareCounterCell dropped without consuming all values!");
        }
    }
}
