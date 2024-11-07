#[cfg(any(debug_assertions, test))]
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use serde::Serialize;

use super::hardware_counter::HardwareCounterCell;

/// A "slow" but thread-safe accumulator for measurement results of `HardwareCounterCell` values.
/// This type is completely reference counted and clones of this type will read/write the same values as their origin structure.
#[derive(Clone, Debug, Serialize)]
pub struct HwMeasurementAcc {
    cpu_counter: Arc<AtomicUsize>,

    #[cfg(any(debug_assertions, test))]
    applied: Arc<AtomicBool>,
}

impl HwMeasurementAcc {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_unchecked() -> Self {
        Self {
            cpu_counter: Arc::new(AtomicUsize::new(0)),

            #[cfg(any(debug_assertions, test))]
            applied: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn new_with_values(cpu: usize) -> Self {
        Self {
            cpu_counter: Arc::new(AtomicUsize::new(cpu)),

            #[cfg(any(debug_assertions, test))]
            applied: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn get_cpu(&self) -> usize {
        self.cpu_counter.load(Ordering::Relaxed)
    }

    pub fn clear(&self) {
        let HwMeasurementAcc {
            cpu_counter,

            #[cfg(any(debug_assertions, test))]
                applied: _,
        } = self;

        cpu_counter.store(0, Ordering::Relaxed);
    }

    /// Consumes and accumulates the values from `hw_counter_cell` into the accumulator.
    pub fn merge(&self, hw_counter: Self) {
        hw_counter.set_applied();

        let Self {
            ref cpu_counter,

            #[cfg(any(debug_assertions, test))]
                applied: _,
        } = hw_counter;

        self.cpu_counter
            .fetch_add(cpu_counter.load(Ordering::Relaxed), Ordering::Relaxed);
    }

    /// Consumes and accumulates the values from `hw_counter_cell` into the accumulator.
    pub fn merge_from_cell(&self, hw_counter_cell: impl Into<HardwareCounterCell>) {
        let HardwareCounterCell { ref cpu_counter } = hw_counter_cell.into();

        self.cpu_counter
            .fetch_add(cpu_counter.take(), Ordering::Relaxed);
    }

    pub fn has_values(&self) -> bool {
        let Self {
            ref cpu_counter,

            #[cfg(any(debug_assertions, test))]
                applied: _,
        } = self;
        cpu_counter.load(Ordering::Relaxed) > 0
    }

    pub fn set_applied(&self) {
        #[cfg(any(debug_assertions, test))]
        self.applied.store(true, Ordering::Relaxed);
    }
}

impl Default for HwMeasurementAcc {
    fn default() -> Self {
        Self {
            cpu_counter: Arc::new(AtomicUsize::new(0)),

            #[cfg(any(debug_assertions, test))]
            applied: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl Drop for HwMeasurementAcc {
    fn drop(&mut self) {
        #[cfg(any(debug_assertions, test))] // We want this to fail in both, release and debug tests
        {
            if !self.applied.load(Ordering::Relaxed)
            && self.has_values()
            // We don't create weak references so checking for strong count only is fine!
            && Arc::strong_count(&self.cpu_counter) == 1
            {
                panic!("Hw Counter not applied")
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_panic_hw_measurement_acc_applied_field() {
        let HwMeasurementAcc {
            cpu_counter: _,
            applied, // In tests we need this field always to be available!
        } = &HwMeasurementAcc::new_with_values(1);
        assert!(!applied.load(Ordering::Relaxed));
        applied.store(true, Ordering::Relaxed);
    }

    #[test]
    #[should_panic]
    fn test_panic_hw_measurement_acc() {
        HwMeasurementAcc::new_with_values(1); // With a value, this will panic on drop
    }

    #[test]
    fn test_panic_hw_measurement_acc_clone() {
        let acc = HwMeasurementAcc::new_with_values(1);

        let acc_clone = acc.clone();
        let result = std::panic::catch_unwind(move || {
            drop(acc_clone);
        });
        // No panic here, since it's a clone!
        assert!(result.is_ok());

        let result = std::panic::catch_unwind(move || {
            drop(acc);
        });
        // Panic here as it's the "last" instance
        assert!(result.is_err());
    }
}
