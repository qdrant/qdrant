use crate::counter::counter_cell::CounterCell;

/// Collection of different types of hardware measurements.
#[derive(Clone, Debug, Default)]
pub struct HardwareCounterCell {
    cpu_counter: CounterCell,
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

    pub fn apply_from(&self, other: &HardwareCounterCell) {
        let HardwareCounterCell { cpu_counter } = other;

        self.cpu_counter.incr_delta(cpu_counter.get());
    }

    pub fn set_from(&self, other: &HardwareCounterCell) {
        let HardwareCounterCell { cpu_counter } = other;

        self.cpu_counter.set(cpu_counter.get());
    }
}
