use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::hardware_counter::HardwareCounterCell;

/// A "slow" but thread-safe accumulator for measurement results of `HardwareCounterCell` values.
/// This type is completely reference counted and clones of this type will read/write the same values as their origin structure.
#[derive(Clone)]
pub struct HwMeasurementAcc {
    cpu_counter: Arc<AtomicUsize>,
}

impl HwMeasurementAcc {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_with_values(cpu: usize) -> Self {
        Self {
            cpu_counter: Arc::new(AtomicUsize::new(cpu)),
        }
    }

    pub fn get_cpu(&self) -> usize {
        self.cpu_counter.load(Ordering::Relaxed)
    }

    pub fn clear(&self) {
        let HwMeasurementAcc { cpu_counter } = self;

        cpu_counter.store(0, Ordering::Relaxed);
    }

    /// Consumes and accumulates the values from `hw_counter_cell` into the accumulator.
    pub fn merge_from_cell(&self, hw_counter_cell: impl Into<HardwareCounterCell>) {
        let HardwareCounterCell { ref cpu_counter } = hw_counter_cell.into();

        self.cpu_counter
            .fetch_add(cpu_counter.take(), Ordering::Relaxed);
    }
}

impl Default for HwMeasurementAcc {
    fn default() -> Self {
        Self {
            cpu_counter: Arc::new(AtomicUsize::new(0)),
        }
    }
}
