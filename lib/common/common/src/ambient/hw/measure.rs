use super::{HardwareData, HwMetric};
use crate::ambient::{AmbientContext, HwScope, current, slot};
use crate::cpu_utilization::CpuUtilization;

impl AmbientContext {
    /// Run `f`, measuring everything it does on this thread into `self`.
    pub fn measure<R>(&self, f: impl FnOnce() -> R) -> R {
        let _scope = self.measure_guard();
        f()
    }

    /// Guard version of [`Self::measure`]. Don't hold it across `.await`.
    pub fn measure_guard(&self) -> HwScope<'_> {
        slot::enter_measured(self)
    }

    /// Like [`Self::measure_guard`], but keeps the context alive by owning it.
    pub fn measure_guard_owned(self) -> HwScope<'static> {
        slot::enter_measured_owned(self)
    }
}

/// Whether the current scope is measured.
pub fn is_measured() -> bool {
    slot::is_measured()
}

/// CPU utilization of the current context; a fresh one when unmeasured.
pub fn cpu_utilization() -> CpuUtilization {
    current().cpu_utilization()
}

/// [`AmbientContext::accumulate_request`] on the current context, if measured.
pub fn accumulate_request(src: HardwareData) {
    slot::accumulate_request(src);
}

/// Measurements of the current scope, not yet flushed into its context.
#[cfg(any(test, feature = "testing"))]
pub fn pending() -> HardwareData {
    slot::pending()
}

/// Run `f`, counting its `Cpu` bumps `multiplier` times.
pub fn scale_cpu<R>(multiplier: usize, f: impl FnOnce() -> R) -> R {
    let before = slot::pending_metric(HwMetric::Cpu);
    let result = f();
    let delta = slot::pending_metric(HwMetric::Cpu).wrapping_sub(before);
    HwMetric::Cpu.bump(delta.wrapping_mul(multiplier.wrapping_sub(1)));
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ambient::{current, test, test_guard};

    #[test]
    fn test_nested_scopes() {
        let outer = AmbientContext::new();
        let inner = AmbientContext::new();
        outer.measure(|| {
            HwMetric::Cpu.bump(1);
            inner.measure(|| {
                HwMetric::Cpu.bump(10);
                test(|| HwMetric::Cpu.bump(100));
                scale_cpu(3, || {
                    HwMetric::Cpu.bump(1000);
                    HwMetric::VectorIoRead.bump(5);
                    HwMetric::Cpu.bump(1);
                });
            });
            HwMetric::Cpu.bump(2);
        });
        assert_eq!(outer.hw_data()[HwMetric::Cpu], 3);
        assert_eq!(inner.hw_data()[HwMetric::Cpu], 3013);
        assert_eq!(inner.hw_data()[HwMetric::VectorIoRead], 5);
    }

    #[test]
    fn test_guards_and_pending() {
        let ctx = AmbientContext::new();
        {
            let _outer = ctx.measure_guard();
            HwMetric::VectorIoWrite.bump(4);
            {
                let _inner = test_guard();
                HwMetric::VectorIoWrite.bump(100);
            }
            assert_eq!(pending()[HwMetric::VectorIoWrite], 4);
            assert_eq!(ctx.hw_data()[HwMetric::VectorIoWrite], 0);
        }
        assert_eq!(ctx.hw_data()[HwMetric::VectorIoWrite], 4);
    }

    #[test]
    fn test_current() {
        let ctx = AmbientContext::new();
        ctx.measure(|| {
            current()
                .context()
                .unwrap()
                .accumulate(HardwareData::from_fn(|m| usize::from(m == HwMetric::Cpu)));
            accumulate_request(HardwareData::from_fn(|m| {
                if m == HwMetric::Cpu { 10 } else { 0 }
            }));
            assert!(is_measured());
            test(|| {
                assert!(current().context().is_none());
                assert!(!is_measured());
            });
        });
        assert_eq!(ctx.hw_data()[HwMetric::Cpu], 11);
    }
}
