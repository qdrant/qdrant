use std::cell::Cell;

use crate::counter::counter_cell::CounterCell;

/// Collection of different types of hardware measurements.
///
/// To ensure we don't miss consuming measurements, this struct will cause a panic on drop in tests and debug mode
/// if it still holds values and checking is not disabled using eg. `unchecked()`.
/// In release mode it'll only log a warning in this case.
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

    /// Don't check whether counter got fully consumed on drop.
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

impl Drop for HardwareCounterCell {
    fn drop(&mut self) {
        if self.checked.get() && self.has_values() {
            #[cfg(any(debug_assertions, test))] // We want this to fail in both, release and debug tests
            panic!("Checked HardwareCounterCell dropped without consuming all values!");

            #[cfg(not(any(debug_assertions, test)))]
            log::warn!("Hardware measurements not processed!")
        }
    }
}
