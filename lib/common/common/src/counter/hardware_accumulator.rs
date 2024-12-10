use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Data structure, that routes hardware measurement counters to specific location.
/// Shared drain MUST NOT create its own counters, but only hold a reference to the existing one,
/// as it doesn't provide any checks on drop.
#[derive(Debug)]
pub struct HwSharedDrain {
    pub(crate) cpu_counter: Arc<AtomicUsize>,
}

impl HwSharedDrain {
    pub fn get_cpu(&self) -> usize {
        self.cpu_counter.load(Ordering::Relaxed)
    }

    fn new(cpu: Arc<AtomicUsize>) -> Self {
        Self { cpu_counter: cpu }
    }
}

impl Clone for HwSharedDrain {
    fn clone(&self) -> Self {
        Self::new(self.cpu_counter.clone())
    }
}
impl Default for HwSharedDrain {
    fn default() -> Self {
        Self {
            cpu_counter: Arc::new(AtomicUsize::new(0)),
        }
    }
}

/// A "slow" but thread-safe accumulator for measurement results of `HardwareCounterCell` values.
/// This type is completely reference counted and clones of this type will read/write the same values as their origin structure.
#[derive(Debug)]
pub struct HwMeasurementAcc {
    request_drain: HwSharedDrain,
    metrics_drain: HwSharedDrain,
}

impl HwMeasurementAcc {
    #[cfg(feature = "testing")]
    pub fn new() -> Self {
        Self {
            request_drain: HwSharedDrain::default(),
            metrics_drain: HwSharedDrain::default(),
        }
    }

    /// Create a disposable accumulator, which will not accumulate any values.
    /// WARNING: This is intended for specific internal use-cases only.
    /// DO NOT use it in tests or if you don't know what you're doing.
    pub fn disposable() -> Self {
        Self {
            request_drain: HwSharedDrain::default(),
            metrics_drain: HwSharedDrain::default(),
        }
    }

    pub fn new_with_metrics_drain(metrics_drain: HwSharedDrain) -> Self {
        Self {
            request_drain: HwSharedDrain::default(),
            metrics_drain,
        }
    }

    pub fn accumulate(&self, cpu: usize) {
        let HwSharedDrain { cpu_counter } = &self.request_drain;
        cpu_counter.fetch_add(cpu, Ordering::Relaxed);

        let HwSharedDrain { cpu_counter } = &self.metrics_drain;
        cpu_counter.fetch_add(cpu, Ordering::Relaxed);
    }

    /// Accumulate usage values for request drain only
    /// This is useful if we want to report usage, which happened on another machine
    /// So we don't want to accumulate the same usage on the current machine second time
    pub fn accumulate_request(&self, cpu: usize) {
        let HwSharedDrain { cpu_counter } = &self.request_drain;
        cpu_counter.fetch_add(cpu, Ordering::Relaxed);
    }

    pub fn get_cpu(&self) -> usize {
        self.request_drain.get_cpu()
    }
}

#[cfg(feature = "testing")]
impl Default for HwMeasurementAcc {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for HwMeasurementAcc {
    fn clone(&self) -> Self {
        Self {
            request_drain: self.request_drain.clone(),
            metrics_drain: self.metrics_drain.clone(),
        }
    }
}
