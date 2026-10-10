use std::ops::{Add, Index, IndexMut};
use std::sync::atomic::{AtomicUsize, Ordering};

use strum::{EnumCount, EnumIter, IntoEnumIterator};

use crate::ambient::slot;

#[derive(Clone, Copy, Debug, Eq, PartialEq, EnumCount, EnumIter)]
pub enum HwMetric {
    Cpu = 0,
    PayloadIoRead = 1,
    PayloadIoWrite = 2,
    PayloadIndexIoRead = 3,
    PayloadIndexIoWrite = 4,
    VectorIoRead = 5,
    VectorIoWrite = 6,
}

impl HwMetric {
    #[inline]
    pub fn bump(self, delta: usize) {
        slot::bump(self, delta);
    }
}

/// Multipliers for `cpu` and `vector_io_read` bumps of a scorer-like object.
#[derive(Clone, Copy, Debug)]
pub struct HwScale {
    pub cpu: usize,
    pub vector_io_read: usize,
}

impl HwScale {
    #[inline]
    pub fn cpu(self, delta: usize) {
        HwMetric::Cpu.bump(delta * self.cpu);
    }

    #[inline]
    pub fn vector_io_read(self, delta: usize) {
        HwMetric::VectorIoRead.bump(delta * self.vector_io_read);
    }
}

/// Contains all hardware metrics. Only serves as value holding structure without any semantics.
#[derive(Copy, Clone, Default)]
pub struct HardwareData(pub(crate) [usize; HwMetric::COUNT]);

impl HardwareData {
    pub fn from_fn(mut f: impl FnMut(HwMetric) -> usize) -> Self {
        let mut data = Self::default();
        for metric in HwMetric::iter() {
            data[metric] = f(metric);
        }
        data
    }
}

impl Index<HwMetric> for HardwareData {
    type Output = usize;

    fn index(&self, metric: HwMetric) -> &usize {
        &self.0[metric as usize]
    }
}

impl IndexMut<HwMetric> for HardwareData {
    fn index_mut(&mut self, metric: HwMetric) -> &mut usize {
        &mut self.0[metric as usize]
    }
}

impl Add for HardwareData {
    type Output = HardwareData;

    fn add(self, rhs: Self) -> Self::Output {
        Self(std::array::from_fn(|i| self.0[i] + rhs.0[i]))
    }
}

/// Thread-safe counters, shared as the per-collection drain of multiple
/// [`AmbientContext`](crate::ambient::AmbientContext)s.
#[derive(Debug, Default)]
pub struct HwSharedDrain([AtomicUsize; HwMetric::COUNT]);

impl HwSharedDrain {
    pub fn load(&self) -> HardwareData {
        HardwareData(self.0.each_ref().map(|c| c.load(Ordering::Relaxed)))
    }

    pub(crate) fn add(&self, src: HardwareData) {
        for (counter, value) in self.0.iter().zip(src.0) {
            counter.fetch_add(value, Ordering::Relaxed);
        }
    }
}
