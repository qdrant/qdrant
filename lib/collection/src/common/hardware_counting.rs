use common::counter::hardware_accumulator::HwMeasurementAcc;

use crate::collection::Collection;

/// A wrapper around `HwMeasurementAcc` that encforces by design that all collected
/// hardware metrics get also to applied to the corresponding collection.
pub struct RequestHardwareAcc(HwMeasurementAcc);

impl RequestHardwareAcc {
    pub fn new() -> Self {
        Self::default()
    }

    // Create a new unchecked and empty `CollectionAppliedHardwareAcc`. Unchecked means we don't
    // panic in tests/debug mode if the accumulated values get dropped without consuming.
    // In release mode this function is semantical equivalent to `new()`.
    pub fn new_unchecked() -> Self {
        Self(HwMeasurementAcc::new_unchecked())
    }

    pub fn set_applied(&self) {
        self.0.set_applied();
    }

    fn apply(&self, src: HwMeasurementAcc, collection_counter: &HwMeasurementAcc) {
        self.0.merge(src.clone());
        collection_counter.merge(src);
    }
}

impl Collection {
    pub fn accumulate_hw_counter(&self, src: HwMeasurementAcc, out: &RequestHardwareAcc) {
        out.apply(src, &self.hardware_usage);
    }
}

impl Default for RequestHardwareAcc {
    fn default() -> Self {
        Self(HwMeasurementAcc::new())
    }
}

impl From<RequestHardwareAcc> for HwMeasurementAcc {
    fn from(value: RequestHardwareAcc) -> Self {
        value.0
    }
}
