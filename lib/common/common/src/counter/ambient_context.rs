use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use strum::EnumCount;

use super::hardware_data::{HardwareData, HwMetric};
use crate::cpu_utilization::CpuUtilization;

/// Thread-safe counters, shared as the per-collection drain of multiple [`AmbientContext`]s.
#[derive(Debug, Default)]
pub struct HwSharedDrain([AtomicUsize; HwMetric::COUNT]);

impl HwSharedDrain {
    pub fn load(&self) -> HardwareData {
        HardwareData(self.0.each_ref().map(|c| c.load(Ordering::Relaxed)))
    }

    fn add(&self, src: HardwareData) {
        for (counter, value) in self.0.iter().zip(src.0) {
            counter.fetch_add(value, Ordering::Relaxed);
        }
    }
}

/// One per request; [`super::hw`] scopes flush into it.
/// Reference-counted: clones read and write the same counters.
#[derive(Clone, Debug)]
pub struct AmbientContext(Arc<Inner>);

#[derive(Debug)]
pub(super) struct Inner {
    request: HwSharedDrain,
    collection: Option<Arc<HwSharedDrain>>,
    cpu_utilization: CpuUtilization,
}

impl AmbientContext {
    #[cfg(feature = "testing")]
    #[expect(clippy::new_without_default)]
    pub fn new() -> Self {
        Self::with_collection(None)
    }

    pub fn new_with_metrics_drain(metrics_drain: Arc<HwSharedDrain>) -> Self {
        Self::with_collection(Some(metrics_drain))
    }

    fn with_collection(collection: Option<Arc<HwSharedDrain>>) -> Self {
        Self(Arc::new(Inner {
            request: HwSharedDrain::default(),
            collection,
            cpu_utilization: CpuUtilization::new(),
        }))
    }

    pub fn cpu_utilization(&self) -> CpuUtilization {
        self.0.cpu_utilization.clone()
    }

    pub fn accumulate(&self, src: HardwareData) {
        self.0.accumulate(src);
    }

    /// Accumulate usage values for request drain only.
    /// This is useful if we want to report usage, which happened on another machine
    /// So we don't want to accumulate the same usage on the current machine second time
    pub fn accumulate_request(&self, src: HardwareData) {
        self.0.accumulate_request(src);
    }

    pub fn hw_data(&self) -> HardwareData {
        self.0.request.load()
    }

    pub(super) fn inner_ptr(&self) -> NonNull<Inner> {
        NonNull::new(Arc::as_ptr(&self.0).cast_mut()).expect("Arc::as_ptr is never null")
    }

    /// # Safety
    /// `ptr` comes from [`Self::inner_ptr`] of a context that is still alive.
    pub(super) unsafe fn from_inner_ptr(ptr: NonNull<Inner>) -> Self {
        unsafe {
            Arc::increment_strong_count(ptr.as_ptr());
            Self(Arc::from_raw(ptr.as_ptr()))
        }
    }
}

impl Inner {
    pub(super) fn accumulate(&self, src: HardwareData) {
        self.request.add(src);
        if let Some(collection) = &self.collection {
            collection.add(src);
        }
    }

    pub(super) fn accumulate_request(&self, src: HardwareData) {
        self.request.add(src);
    }
}
