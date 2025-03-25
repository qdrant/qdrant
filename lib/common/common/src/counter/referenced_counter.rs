use std::ops::Deref;

use super::counter_cell::CounterCell;
use super::hardware_counter::HardwareCounterCell;

/// Referenced hw counter for a single metric of a `HardwareCounterCell`. Can be used to pass a single metric type (eg. only cpu)
/// to a function that needs measurement but depending on the context might measure a different metric.
/// This is currently the case in GridStore as it is used for payloads and sparse vectors.
#[derive(Copy, Clone)]
pub struct HwMetricRefCounter<'a> {
    counter: &'a CounterCell,
}

impl<'a> HwMetricRefCounter<'a> {
    fn new(counter: &'a CounterCell) -> Self {
        Self { counter }
    }
}

impl Deref for HwMetricRefCounter<'_> {
    type Target = CounterCell;

    fn deref(&self) -> &Self::Target {
        self.counter
    }
}

// Implement referenced functions here to prevent exposing `HwMetricRefCounter::new()`.
impl HardwareCounterCell {
    #[inline]
    pub fn ref_payload_io_write_counter(&self) -> HwMetricRefCounter {
        HwMetricRefCounter::new(&self.payload_io_write_counter)
    }

    #[inline]
    pub fn ref_payload_io_read_counter(&self) -> HwMetricRefCounter {
        HwMetricRefCounter::new(&self.payload_io_read_counter)
    }

    #[inline]
    pub fn ref_vector_io_write_counter(&self) -> HwMetricRefCounter {
        HwMetricRefCounter::new(&self.vector_io_write_counter)
    }

    #[inline]
    pub fn ref_payload_index_io_write_counter(&self) -> HwMetricRefCounter {
        HwMetricRefCounter::new(&self.payload_index_io_write_counter)
    }
}
