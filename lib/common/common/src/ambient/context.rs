use std::ptr::NonNull;
use std::sync::Arc;

use super::hw::{HardwareData, HwSharedDrain};
use crate::cpu_utilization::CpuUtilization;

/// One per request; [`super::hw`] scopes flush into it.
/// Reference-counted: clones read and write the same counters.
#[derive(Clone, Debug)]
pub struct AmbientContext(Arc<Root>);

#[derive(Debug)]
pub(super) struct Root {
    hw: HwSharedDrain,
    collection: Option<Arc<HwSharedDrain>>,
    cpu_utilization: CpuUtilization,
}

impl AmbientContext {
    #[cfg(feature = "testing")]
    #[expect(clippy::new_without_default)]
    pub fn new() -> Self {
        Self::with_collection(None)
    }

    pub fn request(collection: Arc<HwSharedDrain>) -> Self {
        Self::with_collection(Some(collection))
    }

    fn with_collection(collection: Option<Arc<HwSharedDrain>>) -> Self {
        Self(Arc::new(Root {
            hw: HwSharedDrain::default(),
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
        self.0.hw.load()
    }

    pub(super) fn as_ptr(&self) -> NonNull<Root> {
        NonNull::new(Arc::as_ptr(&self.0).cast_mut()).expect("Arc::as_ptr is never null")
    }

    /// # Safety
    /// `ptr` comes from [`Self::as_ptr`] of a context that is still alive.
    pub(super) unsafe fn from_ptr(ptr: NonNull<Root>) -> Self {
        unsafe {
            Arc::increment_strong_count(ptr.as_ptr());
            Self(Arc::from_raw(ptr.as_ptr()))
        }
    }
}

impl Root {
    pub(super) fn accumulate(&self, src: HardwareData) {
        self.hw.add(src);
        if let Some(collection) = &self.collection {
            collection.add(src);
        }
    }

    pub(super) fn accumulate_request(&self, src: HardwareData) {
        self.hw.add(src);
    }
}
