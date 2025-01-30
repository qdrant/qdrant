use super::counter_cell::CounterCell;
use super::hardware_accumulator::HwMeasurementAcc;

/// Collection of different types of hardware measurements.
///
/// To ensure we don't miss consuming measurements, this struct will cause a panic on drop in tests and debug mode
/// if it still holds values and checking is not disabled using eg. `unchecked()`.
/// In release mode it'll only log a warning in this case.
#[derive(Debug)]
pub struct HardwareCounterCell {
    cpu_multiplier: usize,
    pub(super) cpu_counter: CounterCell,
    pub(super) payload_io_read_counter: CounterCell,
    pub(super) payload_io_write_counter: CounterCell,
    pub(super) vector_io_write_counter: CounterCell,
    pub(super) accumulator: HwMeasurementAcc,
}

impl HardwareCounterCell {
    #[cfg(feature = "testing")]
    pub fn new() -> Self {
        Self {
            cpu_multiplier: 1,
            cpu_counter: CounterCell::new(),
            payload_io_read_counter: CounterCell::new(),
            payload_io_write_counter: CounterCell::new(),
            vector_io_write_counter: CounterCell::new(),
            accumulator: HwMeasurementAcc::new(),
        }
    }

    /// Create a new `HardwareCounterCell` that doesn't report usage anywhere.
    /// WARNING: This is intended for specific internal operations only.
    /// Do not use it tests or if you don't know what you're doing.
    pub fn disposable() -> Self {
        Self {
            cpu_multiplier: 1,
            cpu_counter: CounterCell::new(),
            payload_io_read_counter: CounterCell::new(),
            payload_io_write_counter: CounterCell::new(),
            vector_io_write_counter: CounterCell::new(),
            accumulator: HwMeasurementAcc::disposable(),
        }
    }

    #[cfg(feature = "testing")]
    pub fn new_with(
        cpu: usize,
        payload_io_read: usize,
        payload_io_write: usize,
        vector_io_write: usize,
    ) -> Self {
        Self {
            cpu_multiplier: 1,
            cpu_counter: CounterCell::new_with(cpu),
            payload_io_read_counter: CounterCell::new_with(payload_io_read),
            payload_io_write_counter: CounterCell::new_with(payload_io_write),
            vector_io_write_counter: CounterCell::new_with(vector_io_write),
            accumulator: HwMeasurementAcc::new(),
        }
    }

    pub fn new_with_accumulator(accumulator: HwMeasurementAcc) -> Self {
        Self {
            cpu_multiplier: 1,
            cpu_counter: CounterCell::new(),
            payload_io_read_counter: CounterCell::new(),
            payload_io_write_counter: CounterCell::new(),
            vector_io_write_counter: CounterCell::new(),
            accumulator,
        }
    }

    /// Create a copy of the current counter cell with the same accumulator and config,
    /// but with empty counter.
    /// Allows independent counting within different segments.
    pub fn fork(&self) -> Self {
        Self {
            cpu_multiplier: self.cpu_multiplier,
            cpu_counter: CounterCell::new(),
            payload_io_read_counter: CounterCell::new(),
            payload_io_write_counter: CounterCell::new(),
            vector_io_write_counter: CounterCell::new(),
            accumulator: self.accumulator.clone(),
        }
    }

    pub fn set_cpu_multiplier(&mut self, multiplier: usize) {
        self.cpu_multiplier = multiplier;
    }

    #[inline]
    pub fn cpu_counter(&self) -> &CounterCell {
        &self.cpu_counter
    }

    #[inline]
    pub fn cpu_counter_mut(&mut self) -> &mut CounterCell {
        &mut self.cpu_counter
    }

    #[inline]
    pub fn payload_io_read_counter(&self) -> &CounterCell {
        &self.payload_io_read_counter
    }

    #[inline]
    pub fn payload_io_read_counter_mut(&mut self) -> &mut CounterCell {
        &mut self.payload_io_read_counter
    }

    #[inline]
    pub fn payload_io_write_counter(&self) -> &CounterCell {
        &self.payload_io_write_counter
    }

    #[inline]
    pub fn payload_io_write_mut(&mut self) -> &mut CounterCell {
        &mut self.payload_io_write_counter
    }

    #[inline]
    pub fn vector_io_write_counter(&self) -> &CounterCell {
        &self.vector_io_write_counter
    }

    #[inline]
    pub fn vector_io_write_mut(&mut self) -> &mut CounterCell {
        &mut self.vector_io_write_counter
    }

    fn merge_to_accumulator(&self) {
        let HardwareCounterCell {
            cpu_multiplier,
            cpu_counter,
            payload_io_read_counter,
            payload_io_write_counter,
            vector_io_write_counter,
            accumulator,
        } = self;

        let cpu_value = cpu_counter.get() * cpu_multiplier;
        accumulator.accumulate(
            cpu_value,
            payload_io_read_counter.get(),
            payload_io_write_counter.get(),
            vector_io_write_counter.get(),
        );
    }
}

#[cfg(feature = "testing")]
impl Default for HardwareCounterCell {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for HardwareCounterCell {
    fn drop(&mut self) {
        self.merge_to_accumulator();
    }
}

#[cfg(test)]
mod test {
    use crate::counter::hardware_accumulator::HwMeasurementAcc;

    #[test]
    fn test_hw_counter_drain() {
        let accumulator = HwMeasurementAcc::new();

        {
            let draining_cell = accumulator.get_counter_cell();
            draining_cell.cpu_counter().incr(); // Dropping here means we drain the values to `atomic` instead of panicking
        }

        assert_eq!(accumulator.get_cpu(), 1);
    }
}
