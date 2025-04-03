use std::ops::Deref;

use super::hardware_counter::HardwareCounterCell;
use super::hardware_data::HardwareData;

/// A counter that measures or disposes measurements based on a condition.
/// This is needed in places where we need to decide at runtime whether to measure or not.
/// Implements `Deref<Target=HardwareCounterCell>` so it can be directly passed to functions
/// that need a `HardwareCounterCell` as parameter.
pub struct ConditionedCounter<'a> {
    condition: bool,
    parent: &'a HardwareCounterCell,
    tmp: HardwareCounterCell,
}

impl<'a> ConditionedCounter<'a> {
    pub fn new(condition: bool, parent: &'a HardwareCounterCell) -> Self {
        Self {
            condition,
            parent,
            tmp: HardwareCounterCell::disposable(), // We manually accumulate collected values!
        }
    }
}

impl Deref for ConditionedCounter<'_> {
    type Target = HardwareCounterCell;

    fn deref(&self) -> &Self::Target {
        &self.tmp
    }
}

impl Drop for ConditionedCounter<'_> {
    fn drop(&mut self) {
        if self.condition {
            self.parent
                .accumulator
                .accumulate(HardwareData::from(&self.tmp));
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::counter::hardware_accumulator::HwMeasurementAcc;

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
            let condition = true;
            let cc = ConditionedCounter::new(condition, &parent);
            cc.cpu_counter().incr_delta(5);
        }

        assert_eq!(parent.cpu_counter().get(), 0); // Parents accumulator gets written, not the counter cell!
        assert_eq!(parent.accumulator.get_cpu(), 5);
    }

    #[test]
    fn test_conditioned_counter_enabled_into_hw_acc() {
        let parent = HardwareCounterCell::new();

        {
            let condition = true;
            let cc = ConditionedCounter::new(condition, &parent);

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
            let condition = true;
            let cell = parent.get_counter_cell();
            let cc = ConditionedCounter::new(condition, &cell);

            // Indirect counting, a possible scenario.
            cc.new_accumulator() // Cell->Acc
                .get_counter_cell() // Acc->Cell
                .cpu_counter()
                .incr_delta(5);
        }

        assert_eq!(parent.get_cpu(), 5);
    }
}
