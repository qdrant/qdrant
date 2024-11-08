use common::counter::hardware_accumulator::HwMeasurementAcc;

use crate::collection::Collection;

#[derive(Clone)]
pub struct CollectionAppliedHardwareAcc(pub(crate) HwMeasurementAcc);

impl CollectionAppliedHardwareAcc {
    pub fn new_unchecked() -> Self {
        Self(HwMeasurementAcc::new_unchecked())
    }

    pub fn new() -> Self {
        Self(HwMeasurementAcc::new())
    }

    pub fn into_hw_measurement_acc(self) -> HwMeasurementAcc {
        self.0
    }

    fn apply(&self, src: HwMeasurementAcc, collection_counter: &HwMeasurementAcc) {
        self.0.merge(src.clone());
        collection_counter.merge(src);
    }
}

impl Collection {
    pub(crate) fn accumulate_hw_counter(
        &self,
        src: HwMeasurementAcc,
        out: &CollectionAppliedHardwareAcc,
    ) {
        out.apply(src, &self.hardware_usage);
    }
}
