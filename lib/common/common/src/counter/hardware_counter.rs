use super::counter_cell::CounterCell;
use super::hardware_accumulator::HwMeasurementAcc;
use super::hardware_data::HardwareData;

/// Collection of different types of hardware measurements.
///
/// To ensure we don't miss consuming measurements, this struct will cause a panic on drop in tests and debug mode
/// if it still holds values and checking is not disabled using eg. `unchecked()`.
/// In release mode it'll only log a warning in this case.
#[derive(Debug)]
pub struct HardwareCounterCell {
    vector_io_read_multiplier: usize,
    cpu_multiplier: usize,
    cpu_counter: CounterCell,
    pub(super) payload_io_read_counter: CounterCell,
    pub(super) payload_io_write_counter: CounterCell,
    pub(super) payload_index_io_read_counter: CounterCell,
    pub(super) payload_index_io_write_counter: CounterCell,
    pub(super) vector_io_read_counter: CounterCell,
    pub(super) vector_io_write_counter: CounterCell,
    pub(super) accumulator: HwMeasurementAcc,
}

#[cfg(feature = "testing")]
impl std::fmt::Display for HardwareCounterCell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "HardwareCounterCell {{ cpu: {}, payload_io_read: {}, payload_io_write: {}, payload_index_io_read: {}, vector_io_read: {}, vector_io_write: {} }}",
            self.cpu_counter.get(),
            self.payload_io_read_counter.get(),
            self.payload_io_write_counter.get(),
            self.payload_index_io_read_counter.get(),
            self.vector_io_read_counter.get(),
            self.vector_io_write_counter.get()
        )
    }
}

impl HardwareCounterCell {
    #[cfg(feature = "testing")]
    pub fn new() -> Self {
        Self {
            vector_io_read_multiplier: 1,
            cpu_multiplier: 1,
            cpu_counter: CounterCell::new(),
            payload_io_read_counter: CounterCell::new(),
            payload_io_write_counter: CounterCell::new(),
            payload_index_io_read_counter: CounterCell::new(),
            payload_index_io_write_counter: CounterCell::new(),
            vector_io_read_counter: CounterCell::new(),
            vector_io_write_counter: CounterCell::new(),
            accumulator: HwMeasurementAcc::new(),
        }
    }

    /// Create a new `HardwareCounterCell` that doesn't report usage anywhere.
    /// WARNING: This is intended for specific internal operations only.
    /// Do not use it tests or if you don't know what you're doing.
    pub fn disposable() -> Self {
        Self {
            vector_io_read_multiplier: 1,
            cpu_multiplier: 1,
            cpu_counter: CounterCell::new(),
            payload_io_read_counter: CounterCell::new(),
            payload_io_write_counter: CounterCell::new(),
            payload_index_io_read_counter: CounterCell::new(),
            payload_index_io_write_counter: CounterCell::new(),
            vector_io_read_counter: CounterCell::new(),
            vector_io_write_counter: CounterCell::new(),
            accumulator: HwMeasurementAcc::disposable(),
        }
    }

    pub fn new_with_accumulator(accumulator: HwMeasurementAcc) -> Self {
        Self {
            vector_io_read_multiplier: 1,
            cpu_multiplier: 1,
            cpu_counter: CounterCell::new(),
            payload_io_read_counter: CounterCell::new(),
            payload_io_write_counter: CounterCell::new(),
            payload_index_io_read_counter: CounterCell::new(),
            payload_index_io_write_counter: CounterCell::new(),
            vector_io_read_counter: CounterCell::new(),
            vector_io_write_counter: CounterCell::new(),
            accumulator,
        }
    }

    pub fn new_accumulator(&self) -> HwMeasurementAcc {
        self.accumulator.clone()
    }

    /// Create a copy of the current counter cell with the same accumulator and config,
    /// but with empty counter.
    /// Allows independent counting within different segments.
    pub fn fork(&self) -> Self {
        Self {
            vector_io_read_multiplier: self.vector_io_read_multiplier,
            cpu_multiplier: self.cpu_multiplier,
            cpu_counter: CounterCell::new(),
            payload_io_read_counter: CounterCell::new(),
            payload_io_write_counter: CounterCell::new(),
            payload_index_io_read_counter: CounterCell::new(),
            payload_index_io_write_counter: CounterCell::new(),
            vector_io_read_counter: CounterCell::new(),
            vector_io_write_counter: CounterCell::new(),
            accumulator: self.accumulator.clone(),
        }
    }

    pub fn set_cpu_multiplier(&mut self, multiplier: usize) {
        self.cpu_multiplier = multiplier;
    }

    pub fn set_vector_io_read_multiplier(&mut self, multiplier: usize) {
        self.vector_io_read_multiplier = multiplier;
    }

    /// Returns the CPU counter that can be used for counting.
    /// Should *never* be used for reading CPU measurements! Use `.get_cpu()` for this.
    #[inline]
    pub fn cpu_counter(&self) -> &CounterCell {
        &self.cpu_counter
    }

    #[inline]
    pub fn payload_io_read_counter(&self) -> &CounterCell {
        &self.payload_io_read_counter
    }

    #[inline]
    pub fn payload_index_io_read_counter(&self) -> &CounterCell {
        &self.payload_index_io_read_counter
    }

    #[inline]
    pub fn payload_index_io_write_counter(&self) -> &CounterCell {
        &self.payload_index_io_write_counter
    }

    #[inline]
    pub fn payload_io_write_counter(&self) -> &CounterCell {
        &self.payload_io_write_counter
    }

    #[inline]
    pub fn vector_io_read(&self) -> &CounterCell {
        &self.vector_io_read_counter
    }

    #[inline]
    pub fn vector_io_write_counter(&self) -> &CounterCell {
        &self.vector_io_write_counter
    }

    /// Returns a copy of the current measurements made by this counter. Ignores all values from the parent accumulator.
    pub fn get_hw_data(&self) -> HardwareData {
        let HardwareCounterCell {
            vector_io_read_multiplier,
            cpu_multiplier,
            cpu_counter, // We use .get_cpu() to calculate the real CPU value.
            payload_io_read_counter,
            payload_io_write_counter,
            payload_index_io_read_counter,
            payload_index_io_write_counter,
            vector_io_read_counter,
            vector_io_write_counter,
            accumulator: _,
        } = self;

        HardwareData {
            cpu: cpu_counter.get() * cpu_multiplier,
            payload_io_read: payload_io_read_counter.get(),
            payload_io_write: payload_io_write_counter.get(),
            payload_index_io_read: payload_index_io_read_counter.get(),
            payload_index_io_write: payload_index_io_write_counter.get(),
            vector_io_read: vector_io_read_counter.get() * vector_io_read_multiplier,
            vector_io_write: vector_io_write_counter.get(),
        }
    }

    fn merge_to_accumulator(&self) {
        self.accumulator.accumulate(self.get_hw_data());
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

impl From<&HardwareCounterCell> for HardwareData {
    fn from(value: &HardwareCounterCell) -> Self {
        let counter_values = value.get_hw_data();
        let acc_values = value.accumulator.hw_data();
        counter_values + acc_values
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

            {
                let mut hw_cell_wb = draining_cell.cpu_counter().write_back_counter();
                hw_cell_wb.incr_delta(1);
            }
        }

        assert_eq!(accumulator.get_cpu(), 2);
    }

    #[test]
    fn test_hw_counter_new_accumulator() {
        let accumulator = HwMeasurementAcc::new();

        {
            let counter = accumulator.get_counter_cell();

            {
                let acc = counter.new_accumulator();
                {
                    let cell = acc.get_counter_cell();
                    cell.cpu_counter().incr_delta(42);
                }
            }

            let mut wb_counter = counter.cpu_counter().write_back_counter();
            wb_counter.incr_delta(1);

            counter.cpu_counter().incr_delta(26);
        }

        assert_eq!(accumulator.get_cpu(), 69);
    }
}
