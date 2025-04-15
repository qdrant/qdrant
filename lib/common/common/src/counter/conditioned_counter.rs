use super::counter_cell::{CounterCell, OptionalCounterCell};
use super::hardware_accumulator::HwMeasurementAcc;
use super::hardware_counter::HardwareCounterCell;

/// A counter that measures or disposes measurements based on a condition.
/// This is needed in places where we need to decide at runtime whether to measure or not.
#[derive(Copy, Clone)]
pub struct ConditionedCounter<'a> {
    parent: Option<&'a HardwareCounterCell>,
}

impl<'a> ConditionedCounter<'a> {
    pub fn new(condition: bool, parent: &'a HardwareCounterCell) -> Self {
        if condition {
            Self::always(parent)
        } else {
            Self::never()
        }
    }

    /// Never measure hardware.
    pub fn never() -> Self {
        Self { parent: None }
    }

    /// Always measure hardware.
    pub fn always(parent: &'a HardwareCounterCell) -> Self {
        Self {
            parent: Some(parent),
        }
    }

    #[inline]
    fn make_optional_counter<C>(&self, c: C) -> OptionalCounterCell<'_>
    where
        C: Fn(&HardwareCounterCell) -> &CounterCell,
    {
        OptionalCounterCell::new(self.parent.map(c))
    }

    #[inline]
    pub fn cpu_counter(&self) -> OptionalCounterCell<'_> {
        self.make_optional_counter(|i| i.cpu_counter())
    }

    #[inline]
    pub fn payload_io_read_counter(&self) -> OptionalCounterCell<'_> {
        self.make_optional_counter(|i| i.payload_io_read_counter())
    }

    #[inline]
    pub fn payload_index_io_read_counter(&self) -> OptionalCounterCell<'_> {
        self.make_optional_counter(|i| i.payload_index_io_read_counter())
    }

    #[inline]
    pub fn payload_index_io_write_counter(&self) -> OptionalCounterCell<'_> {
        self.make_optional_counter(|i| i.payload_index_io_write_counter())
    }

    #[inline]
    pub fn payload_io_write_counter(&self) -> OptionalCounterCell<'_> {
        self.make_optional_counter(|i| i.payload_io_write_counter())
    }

    #[inline]
    pub fn vector_io_read(&self) -> OptionalCounterCell<'_> {
        self.make_optional_counter(|i| i.vector_io_read())
    }

    #[inline]
    pub fn vector_io_write_counter(&self) -> OptionalCounterCell<'_> {
        self.make_optional_counter(|i| i.vector_io_write_counter())
    }

    #[inline]
    pub fn new_accumulator(&self) -> HwMeasurementAcc {
        match self.parent {
            Some(p) => p.new_accumulator(),
            None => HwMeasurementAcc::disposable(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_conditioned_counter_empty() {
        let parent = HardwareCounterCell::new();

        {
            let condition = false;
            let cc = ConditionedCounter::new(condition, &parent);
            cc.cpu_counter().incr_delta(5);
        }

        assert_eq!(parent.cpu_counter().get(), 0);
        assert_eq!(parent.accumulator.get_cpu(), 0);
    }

    #[test]
    fn test_conditioned_counter_enabled() {
        let parent = HardwareCounterCell::new();

        {
            let cc = ConditionedCounter::always(&parent);
            cc.cpu_counter().incr_delta(5);
        }

        assert_eq!(parent.cpu_counter().get(), 5);

        let parent_acc = parent.accumulator.clone();
        drop(parent); // Parents accumulator gets written after `parent` drops.
        assert_eq!(parent_acc.get_cpu(), 5);
    }

    #[test]
    fn test_conditioned_counter_enabled_into_hw_acc() {
        let parent = HardwareCounterCell::new();

        {
            let cc = ConditionedCounter::always(&parent);

            // Indirect counting, a possible scenario.
            cc.new_accumulator() // Cell->Acc
                .get_counter_cell() // Acc->Cell
                .cpu_counter()
                .incr_delta(5);
        }

        assert_eq!(parent.cpu_counter().get(), 0); // Parents accumulator gets written, not the counter cell!
        assert_eq!(parent.accumulator.get_cpu(), 5);
    }

    #[test]
    fn test_conditioned_counter_enabled_parent_hw_acc() {
        let parent = HwMeasurementAcc::new();

        {
            let cell = parent.get_counter_cell();
            let cc = ConditionedCounter::always(&cell);

            // Indirect counting, a possible scenario.
            cc.new_accumulator() // Cell->Acc
                .get_counter_cell() // Acc->Cell
                .cpu_counter()
                .incr_delta(5);
        }

        assert_eq!(parent.get_cpu(), 5);
    }
}
