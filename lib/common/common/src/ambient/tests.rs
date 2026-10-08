use super::hw::HwMetric;
use super::{AmbientContext, HwFutureExt as _, current, parallel, test, test_guard};

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
fn test_measured_future() {
    let ctx = AmbientContext::new();
    let future = async {
        HwMetric::Cpu.bump(1);
        std::future::ready(()).await;
        async { HwMetric::Cpu.bump(10) }.in_current_hw().await;
        test(|| HwMetric::Cpu.bump(100));
    }
    .measured(AmbientContext::clone(&ctx));
    test(|| HwMetric::Cpu.bump(1000)); // not polled yet: not attributed
    futures::executor::block_on(future);
    assert_eq!(ctx.hw_data()[HwMetric::Cpu], 11);
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
