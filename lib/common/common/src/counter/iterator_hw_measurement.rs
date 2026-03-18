use super::conditioned_counter::ConditionedCounter;
use super::counter_cell::{CounterCell, OptionalCounterCell};
use super::hardware_accumulator::HwMeasurementAcc;
use super::hardware_counter::HardwareCounterCell;
use crate::iterator_ext::on_final_count::OnFinalCount;

pub trait HwMeasurementIteratorExt: Iterator {
    /// Measures the hardware usage of an iterator.
    ///
    /// # Arguments
    /// - `hw_acc`: accumulator holding a counter cell
    /// - `multiplier`: multiplies the number of iterations by this factor.
    /// - `f`: Closure to get the specific counter to increase from the cell inside the accumulator.
    fn measure_hw_with_acc<R>(
        self,
        hw_acc: HwMeasurementAcc,
        multiplier: usize,
        mut f: R,
    ) -> OnFinalCount<Self, impl FnMut(usize)>
    where
        Self: Sized,
        R: FnMut(&HardwareCounterCell) -> &CounterCell,
    {
        OnFinalCount::new(self, move |total_count| {
            let hw_counter = hw_acc.get_counter_cell();
            f(&hw_counter).incr_delta(total_count * multiplier);
        })
    }

    /// Measures the hardware usage of an iterator.
    ///
    /// # Arguments
    /// - `cc`: Condition counter to write the measurements into.
    /// - `multiplier`: multiplies the number of iterations by this factor.
    /// - `f`: Closure to get the specific counter to increase from the cell inside the accumulator.
    fn measure_hw_with_condition_cell<R>(
        self,
        cc: ConditionedCounter,
        multiplier: usize,
        mut f: R,
    ) -> OnFinalCount<Self, impl FnMut(usize)>
    where
        Self: Sized,
        R: for<'a> FnMut(&'a ConditionedCounter<'a>) -> OptionalCounterCell<'a>,
    {
        OnFinalCount::new(self, move |total_count| {
            f(&cc).incr_delta(total_count * multiplier);
        })
    }

    /// Measures the hardware usage of an iterator.
    ///
    /// # Arguments
    /// - `hw_cell`: counter cell
    /// - `multiplier`: multiplies the number of iterations by this factor.
    /// - `f`: Closure to get the specific counter to increase from `hw_cell`.
    fn measure_hw_with_cell<R>(
        self,
        hw_cell: &HardwareCounterCell,
        multiplier: usize,
        mut f: R,
    ) -> OnFinalCount<Self, impl FnMut(usize)>
    where
        Self: Sized,
        R: FnMut(&HardwareCounterCell) -> &CounterCell,
    {
        OnFinalCount::new(self, move |total_count| {
            f(hw_cell).incr_delta(total_count * multiplier);
        })
    }

    /// Measures the hardware usage of an iterator with the size of a single value being represented as a fraction.
    fn measure_hw_with_acc_and_fraction<R>(
        self,
        hw_acc: HwMeasurementAcc,
        fraction: usize,
        mut f: R,
    ) -> OnFinalCount<Self, impl FnMut(usize)>
    where
        Self: Sized,
        R: FnMut(&HardwareCounterCell) -> &CounterCell,
    {
        OnFinalCount::new(self, move |total_count| {
            let hw_counter = hw_acc.get_counter_cell();
            f(&hw_counter).incr_delta(total_count / fraction);
        })
    }

    /// Measures the hardware usage of an iterator with the size of a single value being represented as a fraction.
    fn measure_hw_with_cell_and_fraction<R>(
        self,
        hw_cell: &HardwareCounterCell,
        fraction: usize,
        mut f: R,
    ) -> OnFinalCount<Self, impl FnMut(usize)>
    where
        Self: Sized,
        R: FnMut(&HardwareCounterCell) -> &CounterCell,
    {
        OnFinalCount::new(self, move |total_count| {
            f(hw_cell).incr_delta(total_count / fraction);
        })
    }
}

impl<I: Iterator> HwMeasurementIteratorExt for I {}
