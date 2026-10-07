use super::{HardwareData, HwMetric};
use crate::ambient::{AmbientContext, Handoff, Scope, slot};
use crate::cpu_utilization::CpuUtilization;

impl AmbientContext {
    /// Run `f`, measuring everything it does on this thread into `self`.
    pub fn measure<R>(&self, f: impl FnOnce() -> R) -> R {
        slot::measure(self, f)
    }

    /// Guard version of [`Self::measure`]. Don't hold it across `.await`.
    pub fn measure_guard(&self) -> Scope {
        Handoff::Measured(self.clone()).into_scope()
    }
}

/// Whether the current scope is measured.
pub fn is_measured() -> bool {
    slot::with_measured(|ctx| ctx.is_some())
}

/// CPU utilization of the current context; a fresh one when unmeasured.
pub fn cpu_utilization() -> CpuUtilization {
    slot::with_measured(|ctx| ctx.map_or_else(CpuUtilization::new, AmbientContext::cpu_utilization))
}

/// [`AmbientContext::accumulate_request`] on the current context, if measured.
pub fn accumulate_request(src: HardwareData) {
    slot::with_measured(|ctx| {
        if let Some(ctx) = ctx {
            ctx.accumulate_request(src);
        }
    });
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
    fn test_accumulate() {
        let cpu = |value| HardwareData::from_fn(|m| if m == HwMetric::Cpu { value } else { 0 });
        let ctx = AmbientContext::new();
        ctx.measure(|| {
            current().measured().unwrap().accumulate(cpu(1));
            accumulate_request(cpu(10));
            test(|| accumulate_request(cpu(100)));
        });
        assert_eq!(ctx.hw_data()[HwMetric::Cpu], 11);
    }
}
