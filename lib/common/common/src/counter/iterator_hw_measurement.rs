use super::hw::HwMetric;
use crate::iterator_ext::on_final_count::OnFinalCount;

pub trait HwMeasurementIteratorExt: Iterator {
    /// 🤖 Measures the hardware usage of an iterator: bumps `metric` by `multiplier` per
    /// 🤖 iterated item, once the iterator is dropped.
    fn measure_hw(
        self,
        metric: HwMetric,
        multiplier: usize,
    ) -> OnFinalCount<Self, impl FnMut(usize)>
    where
        Self: Sized,
    {
        OnFinalCount::new(self, move |total_count| {
            metric.bump(total_count * multiplier);
        })
    }

    /// 🤖 Same as [`Self::measure_hw`], with the size of a single item being represented as a
    /// 🤖 fraction.
    fn measure_hw_fraction(
        self,
        metric: HwMetric,
        fraction: usize,
    ) -> OnFinalCount<Self, impl FnMut(usize)>
    where
        Self: Sized,
    {
        OnFinalCount::new(self, move |total_count| {
            metric.bump(total_count / fraction);
        })
    }
}

impl<I: Iterator> HwMeasurementIteratorExt for I {}
