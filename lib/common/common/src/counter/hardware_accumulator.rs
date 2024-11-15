use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::hardware_counter::HardwareCounterCell;

/// A "slow" but thread-safe accumulator for measurement results of `HardwareCounterCell` values.
/// This type is completely reference counted and clones of this type will read/write the same values as their origin structure.
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

    pub fn new_collector(&self) -> HwMeasurementCollector {
        HwMeasurementCollector::new(self.cpu_counter.clone())
    }

    pub fn deep_copy(&self) -> Self {
        Self::new_with_values(self.get_cpu())
    }

    pub fn get_cpu(&self) -> usize {
        self.cpu_counter.load(Ordering::Relaxed)
    }

    pub fn clear(&self) {
        let HwMeasurementAcc { cpu_counter } = self;

        cpu_counter.store(0, Ordering::Relaxed);
    }

    pub fn discard(&self) {
        self.clear()
    }

    /// Consumes and accumulates the values from `other` into the accumulator.
    pub fn merge(&self, other: Self) {
        let HwMeasurementAcc { ref cpu_counter } = other;
        let cpu = cpu_counter.swap(0, Ordering::Relaxed);
        self.cpu_counter.fetch_add(cpu, Ordering::Relaxed);
    }

    /// Consumes and accumulates the values from `hw_counter_cell` into the accumulator.
    pub fn merge_from_cell(&self, hw_counter_cell: impl Into<HardwareCounterCell>) {
        let HardwareCounterCell { ref cpu_counter } = hw_counter_cell.into();

        self.cpu_counter
            .fetch_add(cpu_counter.take(), Ordering::Relaxed);
    }

    pub fn is_zero(&self) -> bool {
        let HwMeasurementAcc { ref cpu_counter } = self;
        cpu_counter.load(Ordering::Relaxed) == 0
    }

    pub fn take(&self) -> HwMeasurementAcc {
        let HwMeasurementAcc { ref cpu_counter } = self;
        let cpu = cpu_counter.swap(0, Ordering::Relaxed);
        Self::new_with_values(cpu)
    }
}

impl Drop for HwMeasurementAcc {
    // `HwMeasurementAcc` holds collected hardware measurements for certain operations. To not accidentally lose measured values, we have
    // this custom drop() function, panicking if it gets dropped while still holding values (in debug/test builds).
    //
    // If you encountered it panicking here, it means that you probably don't have propagated or handled some collected measurements properly,
    // or didn't discard them when unneeded using `discard()`.
    //
    // You can apply values by utilizing `merge_from_cell(other_cell)` or `merge(other)`, consuming the other counter, which then can be dropped safely.
    fn drop(&mut self) {
        #[cfg(any(debug_assertions, test))] // Fail in both, release and debug tests.
        {
            if !self.is_zero() {
                panic!("HwMeasurementAcc dropped while still holding values!")
            }
        }
    }
}

impl Default for HwMeasurementAcc {
    fn default() -> Self {
        Self {
            cpu_counter: Arc::new(AtomicUsize::new(0)),
        }
    }
}

/// A type collecting hardware measuerements in multi threaded scenarios where multiple instances of `HwMeasurementAcc` would be needed.
/// Because `HwMeasurementAcc` purposely doesn't implement Clone, we need this type to allow having an owned type we can move
/// between threads/async closures.
///
/// This collector automatically applies all measurements to the `HwMeasurementAcc` that was used when creating this collector on drop.
/// To ensure all values get applied, before reading from a `HwMeasurementAcc`, this type must be dropped first.
pub struct HwMeasurementCollector {
    cpu_counter: Arc<AtomicUsize>,
    collector: HwMeasurementAcc,
}

impl HwMeasurementCollector {
    fn new(cpu_counter: Arc<AtomicUsize>) -> Self {
        let collector = HwMeasurementAcc::new();
        Self {
            cpu_counter,
            collector,
        }
    }
}

impl Deref for HwMeasurementCollector {
    type Target = HwMeasurementAcc;

    fn deref(&self) -> &Self::Target {
        &self.collector
    }
}

impl Drop for HwMeasurementCollector {
    fn drop(&mut self) {
        let HwMeasurementAcc { cpu_counter } = &self.collector;
        let cpu = cpu_counter.swap(0, Ordering::Relaxed);

        self.cpu_counter.fetch_add(cpu, Ordering::Relaxed);
    }
}
