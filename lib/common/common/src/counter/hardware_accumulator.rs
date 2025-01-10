use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Data structure, that routes hardware measurement counters to specific location.
/// Shared drain MUST NOT create its own counters, but only hold a reference to the existing one,
/// as it doesn't provide any checks on drop.
#[derive(Debug)]
pub struct HwSharedDrain {
    pub(crate) cpu_counter: Arc<AtomicUsize>,
    pub(crate) io_read_counter: Arc<AtomicUsize>,
    pub(crate) io_write_counter: Arc<AtomicUsize>,
}

impl HwSharedDrain {
    pub fn get_cpu(&self) -> usize {
        self.cpu_counter.load(Ordering::Relaxed)
    }

    pub fn get_io_read(&self) -> usize {
        self.io_read_counter.load(Ordering::Relaxed)
    }

    pub fn get_io_write(&self) -> usize {
        self.io_write_counter.load(Ordering::Relaxed)
    }

    fn new(cpu: Arc<AtomicUsize>, io_read: Arc<AtomicUsize>, io_write: Arc<AtomicUsize>) -> Self {
        Self {
            cpu_counter: cpu,
            io_read_counter: io_read,
            io_write_counter: io_write,
        }
    }
}

impl Clone for HwSharedDrain {
    fn clone(&self) -> Self {
        Self::new(
            self.cpu_counter.clone(),
            self.io_read_counter.clone(),
            self.io_write_counter.clone(),
        )
    }
}
impl Default for HwSharedDrain {
    fn default() -> Self {
        Self {
            cpu_counter: Arc::new(AtomicUsize::new(0)),
            io_read_counter: Arc::new(AtomicUsize::new(0)),
            io_write_counter: Arc::new(AtomicUsize::new(0)),
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

    pub fn accumulate(&self, cpu: usize, io_read: usize, io_write: usize) {
        let HwSharedDrain {
            cpu_counter,
            io_read_counter,
            io_write_counter,
        } = &self.request_drain;
        cpu_counter.fetch_add(cpu, Ordering::Relaxed);
        io_read_counter.fetch_add(io_read, Ordering::Relaxed);
        io_write_counter.fetch_add(io_write, Ordering::Relaxed);

        let HwSharedDrain {
            cpu_counter,
            io_read_counter,
            io_write_counter,
        } = &self.metrics_drain;
        cpu_counter.fetch_add(cpu, Ordering::Relaxed);
        io_read_counter.fetch_add(io_read, Ordering::Relaxed);
        io_write_counter.fetch_add(io_write, Ordering::Relaxed);
    }

    /// Accumulate usage values for request drain only
    /// This is useful if we want to report usage, which happened on another machine
    /// So we don't want to accumulate the same usage on the current machine second time
    pub fn accumulate_request(&self, cpu: usize, io_read: usize, io_write: usize) {
        let HwSharedDrain {
            cpu_counter,
            io_read_counter,
            io_write_counter,
        } = &self.request_drain;
        cpu_counter.fetch_add(cpu, Ordering::Relaxed);
        io_read_counter.fetch_add(io_read, Ordering::Relaxed);
        io_write_counter.fetch_add(io_write, Ordering::Relaxed);
    }

    pub fn get_cpu(&self) -> usize {
        self.request_drain.get_cpu()
    }

    pub fn get_io_read(&self) -> usize {
        self.request_drain.get_io_read()
    }

    pub fn get_io_write(&self) -> usize {
        self.request_drain.get_io_write()
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
