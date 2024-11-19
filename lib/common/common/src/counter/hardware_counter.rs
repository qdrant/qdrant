use crate::counter::counter_cell::CounterCell;

/// Collection of different types of hardware measurements.
///
/// To ensure we don't miss consuming measurements, this struct will cause a panic on drop in tests and debug mode
/// if it still holds values and checking is not disabled using eg. `unchecked()`.
/// In release mode it'll only log a warning in this case.
#[derive(Debug, Default)]
pub struct HardwareCounterCell {
    pub(super) cpu_counter: CounterCell,
}

impl HardwareCounterCell {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_with(cpu: usize) -> Self {
        Self {
            cpu_counter: CounterCell::new_with(cpu),
        }
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
    pub fn apply_from(&self, other: HardwareCounterCell) {
        let HardwareCounterCell { ref cpu_counter } = other;

        self.cpu_counter.incr_delta(cpu_counter.get());

        other.clear();
    }

    /// Returns `true` if at least one metric has non-consumed values.
    pub fn has_values(&self) -> bool {
        let HardwareCounterCell { cpu_counter } = self;

        cpu_counter.get() > 0
    }

    /// Resets all measurements in this hardware counter.
    pub fn clear(&self) {
        let HardwareCounterCell { cpu_counter } = self;

        cpu_counter.clear();
    }

    pub fn take(&self) -> HardwareCounterCell {
        let new_counter = HardwareCounterCell {
            cpu_counter: self.cpu_counter.clone(),
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

impl Drop for HardwareCounterCell {
    fn drop(&mut self) {
        if self.has_values() {
            // We want this to fail in both, release and debug tests
            #[cfg(any(debug_assertions, test))]
            if !std::thread::panicking() {
                panic!("Checked HardwareCounterCell dropped without consuming all values!");
            }

            log::warn!("Hardware measurements not processed!")
        }
    }
}

impl<T> From<Option<T>> for HardwareCounterCell
where
    T: Into<HardwareCounterCell>,
{
    fn from(value: Option<T>) -> Self {
        value.map(|i| i.into()).unwrap_or_default()
    }
}
