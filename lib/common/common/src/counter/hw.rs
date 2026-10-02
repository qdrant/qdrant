//! 🤖 Ambient, per-thread hardware usage context.
//!
//! 🤖 Code that does measurable work calls [`HwMetric::bump`] without passing any counter around.
//! 🤖 Where the measurements go is decided by the innermost scope on the current thread, entered
//! 🤖 with [`AmbientContext::measure`] / [`unmeasured`]. Bumps outside of any scope panic in
//! 🤖 debug builds, and are dropped in release builds.
//!
//! 🤖 Scopes restore the outer state on exit. Closure scopes can't span an `.await`; guard scopes
//! 🤖 are `!Send`, so a spawned future can't hold them across an `.await` either. Use
//! 🤖 [`HwFutureExt`] to enter a scope on every poll of a future.
//! 🤖 To continue a scope on another thread or task, pass the [`HwHandoff`] from [`current`] and
//! 🤖 enter it there with [`HwHandoff::enter`].
//! 🤖 Wrap rayon calls into [`parallel`]: while inside, jobs stolen by this thread that didn't
//! 🤖 enter their own scope are not attributed to the current one.
//! 🤖 Scopes exited out of order lose their measurements (debug builds panic).

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use pin_project_lite::pin_project;

use super::ambient_context::AmbientContext;
use super::hardware_data::HardwareData;
pub use super::hardware_data::HwMetric;
use super::hw_slot;
pub use super::hw_slot::HwScope;
use crate::cpu_utilization::CpuUtilization;
use crate::reason::Reason;

impl HwMetric {
    #[inline]
    pub fn bump(self, delta: usize) {
        hw_slot::bump(self, delta);
    }
}

impl AmbientContext {
    /// 🤖 Run `f`, measuring everything it does on this thread into `self`.
    pub fn measure<R>(&self, f: impl FnOnce() -> R) -> R {
        let _scope = self.measure_guard();
        f()
    }

    /// 🤖 Guard version of [`Self::measure`]. Don't hold it across `.await`.
    pub fn measure_guard(&self) -> HwScope<'_> {
        hw_slot::enter_measured(self)
    }

    /// 🤖 Like [`Self::measure_guard`], but keeps the context alive by owning it.
    pub fn measure_guard_owned(self) -> HwScope<'static> {
        hw_slot::enter_measured_owned(self)
    }
}

/// 🤖 Run `f` without measuring it. Use for internal operations, which are not attributed to any
/// 🤖 request.
pub fn unmeasured<R>(_: Reason, f: impl FnOnce() -> R) -> R {
    let _scope = hw_slot::enter_unmeasured();
    f()
}

/// 🤖 Guard version of [`unmeasured`]. Don't hold it across `.await`.
pub fn unmeasured_guard(_: Reason) -> HwScope<'static> {
    hw_slot::enter_unmeasured()
}

/// 🤖 [`unmeasured`] for tests, to keep [`unmeasured`] for internal operations.
#[cfg(any(test, feature = "testing"))]
pub fn test<R>(f: impl FnOnce() -> R) -> R {
    unmeasured(
        crate::reason::reason("🤖 Tests aren't attributed to any request"),
        f,
    )
}

/// 🤖 Guard version of [`test`].
#[cfg(any(test, feature = "testing"))]
pub fn test_guard() -> HwScope<'static> {
    unmeasured_guard(crate::reason::reason(
        "🤖 Tests aren't attributed to any request",
    ))
}

/// 🤖 A context to enter, taken from [`current`] / [`parallel`], or constructed explicitly.
/// 🤖 Not measuring requires spelling out the reason, so it is never a silent default.
#[derive(Clone, Debug)]
pub struct HwHandoff(Option<AmbientContext>);

impl HwHandoff {
    pub fn measured(ctx: AmbientContext) -> Self {
        Self(Some(ctx))
    }

    /// 🤖 Don't measure. Use for internal operations, which are not attributed to any request.
    pub fn unmeasured(_: Reason) -> Self {
        Self(None)
    }

    /// 🤖 Run `f` in this context.
    pub fn enter<R>(&self, f: impl FnOnce() -> R) -> R {
        let _scope = self.enter_guard();
        f()
    }

    /// 🤖 Guard version of [`Self::enter`]. Don't hold it across `.await`.
    pub fn enter_guard(&self) -> HwScope<'_> {
        match &self.0 {
            Some(ctx) => hw_slot::enter_measured(ctx),
            None => hw_slot::enter_unmeasured(),
        }
    }

    pub fn is_measured(&self) -> bool {
        self.0.is_some()
    }

    pub fn context(&self) -> Option<&AmbientContext> {
        self.0.as_ref()
    }

    /// 🤖 CPU utilization of the context; a fresh one when unmeasured.
    pub fn cpu_utilization(&self) -> CpuUtilization {
        self.0
            .as_ref()
            .map_or_else(CpuUtilization::new, AmbientContext::cpu_utilization)
    }
}

/// 🤖 Run rayon (or any other work-stealing) calls.
/// 🤖 Closures passed to rayon must enter the provided context, see [`HwHandoff::enter`].
pub fn parallel<R>(f: impl FnOnce(&HwHandoff) -> R) -> R {
    let ctx = current();
    let _scope = hw_slot::enter_masked();
    f(&ctx)
}

/// 🤖 The context of the current scope, to enter it on another thread or task.
pub fn current() -> HwHandoff {
    HwHandoff(hw_slot::current_ctx())
}

/// 🤖 Whether the current scope is measured.
pub fn is_measured() -> bool {
    hw_slot::is_measured()
}

/// 🤖 CPU utilization of the current context; a fresh one when unmeasured.
pub fn cpu_utilization() -> CpuUtilization {
    current().cpu_utilization()
}

/// 🤖 [`AmbientContext::accumulate_request`] on the current context, if measured.
pub fn accumulate_request(src: HardwareData) {
    hw_slot::accumulate_request(src);
}

/// 🤖 Measurements of the current scope, not yet flushed into its context.
#[cfg(any(test, feature = "testing"))]
pub fn pending() -> HardwareData {
    hw_slot::pending()
}

/// 🤖 Run `f`, counting its `Cpu` bumps `multiplier` times.
pub fn scale_cpu<R>(multiplier: usize, f: impl FnOnce() -> R) -> R {
    let before = hw_slot::pending_metric(HwMetric::Cpu);
    let result = f();
    let delta = hw_slot::pending_metric(HwMetric::Cpu).wrapping_sub(before);
    HwMetric::Cpu.bump(delta.wrapping_mul(multiplier.wrapping_sub(1)));
    result
}

/// 🤖 Multipliers for `cpu` and `vector_io_read` bumps of a scorer-like object.
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

/// 🤖 Future adapters that enter a scope on every poll.
pub trait HwFutureExt: Future + Sized {
    fn measured(self, ctx: AmbientContext) -> HwFuture<Self> {
        self.in_hw(HwHandoff::measured(ctx))
    }

    fn unmeasured(self, reason: Reason) -> HwFuture<Self> {
        self.in_hw(HwHandoff::unmeasured(reason))
    }

    /// 🤖 Enter a context taken from [`current`].
    fn in_hw(self, hw: HwHandoff) -> HwFuture<Self> {
        HwFuture { hw, future: self }
    }

    fn in_current_hw(self) -> HwFuture<Self> {
        self.in_hw(current())
    }
}

impl<F: Future> HwFutureExt for F {}

pin_project! {
    /// 🤖 See [`HwFutureExt`].
    pub struct HwFuture<F> {
        hw: HwHandoff,
        #[pin]
        future: F,
    }
}

impl<F: Future> Future for HwFuture<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let this = self.project();
        let _scope = this.hw.enter_guard();
        this.future.poll(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_context_across_threads() {
        let ctx = AmbientContext::new();
        ctx.measure(|| {
            parallel(|hw| {
                std::thread::scope(|s| {
                    s.spawn(|| hw.enter(|| HwMetric::PayloadIoRead.bump(7)));
                    s.spawn(|| test(|| HwMetric::PayloadIoRead.bump(1000)));
                });
                test(|| HwMetric::PayloadIoRead.bump(1000));
            });
            HwMetric::PayloadIoRead.bump(1);
        });
        assert_eq!(ctx.hw_data()[HwMetric::PayloadIoRead], 8);
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
    #[should_panic(expected = "outside of any hw scope")]
    fn test_unscoped_bump_panics() {
        AmbientContext::new().measure(|| HwMetric::Cpu.bump(1));
        test(|| HwMetric::Cpu.bump(1));
        HwMetric::Cpu.bump(1);
    }

    #[test]
    #[should_panic(expected = "outside of any hw scope")]
    fn test_forgotten_enter_panics() {
        AmbientContext::new().measure(|| {
            parallel(|_ctx| {
                std::thread::scope(|s| {
                    s.spawn(|| HwMetric::Cpu.bump(1))
                        .join()
                        .unwrap_or_else(|err| std::panic::resume_unwind(err))
                })
            })
        });
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

    #[test]
    fn test_measured_future() {
        let ctx = AmbientContext::new();
        let future = async {
            HwMetric::Cpu.bump(1);
            std::future::ready(()).await;
            async { HwMetric::Cpu.bump(10) }.in_current_hw().await;
            test(|| HwMetric::Cpu.bump(100));
        }
        .measured(AmbientContext::clone(&ctx));
        test(|| HwMetric::Cpu.bump(1000)); // 🤖 not polled yet: not attributed
        futures::executor::block_on(future);
        assert_eq!(ctx.hw_data()[HwMetric::Cpu], 11);
    }

    #[test]
    fn test_restored_on_panic() {
        let ctx = AmbientContext::new();
        ctx.measure(|| {
            let _ = std::panic::catch_unwind(|| AmbientContext::new().measure(|| panic!()));
            HwMetric::Cpu.bump(1);
        });
        assert_eq!(ctx.hw_data()[HwMetric::Cpu], 1);
    }

    #[test]
    #[cfg_attr(debug_assertions, should_panic(expected = "exited out of order"))]
    fn test_escaped_guard_is_discarded() {
        let ctx = AmbientContext::new();
        let guard = ctx.measure(|| {
            HwMetric::Cpu.bump(1);
            test_guard()
        });
        drop(ctx);
        drop(guard);
        test(|| assert!(current().context().is_none()));
    }
}
