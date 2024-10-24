use super::hardware_counter::HardwareCounterCell;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

/// A "slow" but thread-safe accumulator for measurement results of `HardwareCounterCell` values.
#[derive(Clone)]
pub struct AtomicHardwareAccumulator {
    cpu_counter: Arc<AtomicUsize>,
}

impl AtomicHardwareAccumulator {
    pub fn new() -> Self {
        Self {
            cpu_counter: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn get_cpu(&self) -> usize {
        self.cpu_counter.load(Ordering::Relaxed)
    }

    pub fn clear(&self) {
        let AtomicHardwareAccumulator { cpu_counter } = self;

        cpu_counter.store(0, Ordering::Relaxed);
    }

    /// Consumes and accumulates the values from `hw_counter_cell` into the accumulator.
    pub fn apply_from_cell(&self, hw_counter_cell: &HardwareCounterCell) {
        let HardwareCounterCell {
            cpu_counter,
            checked: _,
        } = hw_counter_cell;

        self.cpu_counter.store(cpu_counter.get(), Ordering::Relaxed);

        // Clear the cells measurements to 'consume' it.
        cpu_counter.set(0);
    }
}
