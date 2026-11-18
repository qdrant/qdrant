use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Barrier;
use std::task::{Context, Waker};

use rayon::ThreadPoolBuilder;
use serde_json::json;

use super::hw::{self, HwMetric};
use super::trace::testing::{file_sink, lines, run};
use super::trace::{Sink, mark, span};
use super::{
    self as ambient, AmbientContext, AmbientFutureExt as _, async_test_guard, current, parallel,
    test, test_guard,
};
use crate::reason::reason;

#[test]
fn handoff_enters_on_another_thread() {
    let (events, hw) = run(|| {
        parallel(|handoff| {
            std::thread::scope(|s| {
                s.spawn(|| {
                    handoff.enter(|| {
                        HwMetric::Cpu.bump(7);
                        mark!("in thread");
                    })
                });
                s.spawn(|| test(|| HwMetric::Cpu.bump(1000)));
            });
            test(|| HwMetric::Cpu.bump(1000));
        });
        HwMetric::Cpu.bump(1);
    });
    assert_eq!(hw[HwMetric::Cpu], 8);
    assert_eq!(
        json!(events),
        json!([
            {"kind": "span_start", "id": 1, "parent": 0, "timestamp": "*", "name": "root"},
            {"kind": "mark",                "parent": 1, "timestamp": "*", "text": "in thread"},
            {"kind": "span_end",   "id": 1,              "timestamp": "*"},
        ])
    );
}

#[test]
fn future_enters_on_every_poll() {
    let (_file, sink) = file_sink();
    let ctx = AmbientContext::root("root", None, Some(sink.clone()));
    let mut future = Box::pin(
        async {
            HwMetric::Cpu.bump(1);
            mark!("first poll");
            futures::pending!();
            async { HwMetric::Cpu.bump(10) }.in_current_ambient().await;
            test(|| HwMetric::Cpu.bump(100));
            mark!("second poll");
        }
        .measured(AmbientContext::clone(&ctx)),
    );
    test(|| HwMetric::Cpu.bump(1000)); // not polled yet: not attributed
    for _ in 0..2 {
        future = std::thread::spawn(move || {
            let _ = future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()));
            future
        })
        .join()
        .unwrap();
    }
    assert_eq!(ctx.hw_data()[HwMetric::Cpu], 11);
    drop((ctx, future));
    sink.stop();
    assert_eq!(
        json!(lines(&sink)),
        json!([
            {"kind": "span_start", "id": 1, "parent": 0, "timestamp": "*", "name": "root"},
            {"kind": "mark",                "parent": 1, "timestamp": "*", "text": "first poll"},
            {"kind": "mark",                "parent": 1, "timestamp": "*", "text": "second poll"},
            {"kind": "span_end",   "id": 1,              "timestamp": "*"},
        ])
    );
}

#[test]
fn measured_and_traced_are_orthogonal() {
    let (events, hw) = run(|| {
        ambient::unmeasured(reason("test"), || {
            HwMetric::Cpu.bump(100);
            mark!("unmeasured");
        });
        span!("child").enter(|| {
            HwMetric::Cpu.bump(5);
            mark!("in child");
        });
    });
    assert_eq!(hw[HwMetric::Cpu], 5);
    assert_eq!(
        json!(events),
        json!([
            {"kind": "span_start", "id": 1, "parent": 0, "timestamp": "*", "name": "root"},
            {"kind": "mark",                "parent": 1, "timestamp": "*", "text": "unmeasured"},
            {"kind": "span_start", "id": 2, "parent": 1, "timestamp": "*", "name": "child"},
            {"kind": "mark",                "parent": 2, "timestamp": "*", "text": "in child"},
            {"kind": "span_end",   "id": 2,              "timestamp": "*"},
            {"kind": "span_end",   "id": 1,              "timestamp": "*"},
        ])
    );
}

#[test]
fn current_reflects_the_scope() {
    AmbientContext::new().measure(|| {
        assert!(current().measured().is_some());
        assert!(hw::is_measured());
        test(|| {
            assert!(current().measured().is_none());
            assert!(current().context().is_some());
            assert!(!hw::is_measured());
        });
    });
}

#[test]
fn unmeasured_future_stays_in_the_span() {
    let (events, hw) = run(|| {
        let future = async {
            HwMetric::Cpu.bump(1);
            mark!("unmeasured");
        }
        .unmeasured(reason("test"));
        std::thread::spawn(|| futures::executor::block_on(future))
            .join()
            .unwrap();
    });
    assert_eq!(hw[HwMetric::Cpu], 0);
    assert_eq!(
        json!(events),
        json!([
            {"kind": "span_start", "id": 1, "parent": 0, "timestamp": "*", "name": "root"},
            {"kind": "mark",                "parent": 1, "timestamp": "*", "text": "unmeasured"},
            {"kind": "span_end",   "id": 1,              "timestamp": "*"},
        ])
    );
}

#[test]
fn root_without_started_sink_is_untraced() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let sink = Sink::file(file.path().to_owned()).unwrap();
    assert!(!AmbientContext::root("stopped", None, Some(sink.clone())).is_traced());
    sink.start().unwrap();
    let ctx = AmbientContext::root("started", None, Some(sink.clone()));
    assert!(ctx.is_traced());
    drop(ctx);
    sink.stop();
    assert_eq!(
        json!(lines(&sink)),
        json!([
            {"kind": "span_start", "id": 1, "parent": 0, "timestamp": "*", "name": "started"},
            {"kind": "span_end",   "id": 1,              "timestamp": "*"},
        ])
    );
}

#[test]
#[cfg_attr(
    debug_assertions,
    should_panic(expected = "outside of any ambient scope")
)]
fn unscoped_bump_panics() {
    AmbientContext::new().measure(|| HwMetric::Cpu.bump(1));
    test(|| HwMetric::Cpu.bump(1));
    HwMetric::Cpu.bump(1);
}

#[test]
#[cfg_attr(
    debug_assertions,
    should_panic(expected = "outside of any ambient scope")
)]
fn unscoped_current_panics() {
    drop(current());
}

#[test]
fn async_test_guard_is_outermost() {
    {
        let _test = async_test_guard();
        let _inner = test_guard();
    }
    let _test = async_test_guard();
}

#[test]
#[should_panic(expected = "must be the outermost scope")]
fn async_test_guard_under_a_scope_panics() {
    let _scope = test_guard();
    let _test = async_test_guard();
}

#[tokio::test(flavor = "multi_thread")]
async fn async_test_guard_in_a_task_panics() {
    let _test = async_test_guard();
    let err = tokio::spawn(async { drop(async_test_guard()) })
        .await
        .unwrap_err();
    assert!(err.is_panic());
}

#[test]
pub fn masked_context_prevents_misattributing() {
    assert_eq!(run(|f| parallel(f)), 1); // Good impl
    assert_eq!(run(bad_parallel), 1001); // Bad impl

    fn bad_parallel(f: &dyn Fn(&super::Handoff)) {
        let handoff = current();
        // Bad impl: no `Scope::masked()` guard.
        f(&handoff)
    }

    type ParallelImpl = fn(&dyn Fn(&super::Handoff));
    fn run(parallel_impl: ParallelImpl) -> usize {
        let outer_pool = ThreadPoolBuilder::new().num_threads(1).build().unwrap();
        let barrier = Barrier::new(2);

        outer_pool.scope(|s| {
            s.spawn(|_| {
                // Unrelated job that started from the same `outer_pool`.
                //
                // With good `parallel_impl`, the `.bump()` below would result
                // in a panic because this job did not enter any scope.
                // This panic is intentional: it would help us catch jobs with
                // missing context.
                //
                // With bad `parallel_impl`, the `.bump()` would be incorrectly
                // attributed to the `context`/`scope` below. We do not want
                // such bumps to be silently misattributed.
                _ = catch_unwind(|| HwMetric::Cpu.bump(1000));
                barrier.wait();
            });

            let inner_pool = ThreadPoolBuilder::new().num_threads(1).build().unwrap();
            let context = AmbientContext::new();
            let scope = context.measure_guard();
            parallel_impl(&|handoff| {
                inner_pool.install(|| {
                    let _scope = handoff.enter_guard();
                    // This is the only bump that should be counted.
                    HwMetric::Cpu.bump(1);
                    barrier.wait();
                })
            });
            drop(scope);

            context.hw_data()[HwMetric::Cpu]
        })
    }
}

#[test]
fn forgotten_guard_leaks_instead_of_dangling() {
    std::thread::spawn(|| {
        let ctx = AmbientContext::new();
        std::mem::forget(ctx.measure_guard());
        drop(ctx);
        assert!(current().measured().is_some());
    })
    .join()
    .unwrap();
}

#[test]
fn scope_restored_on_panic() {
    let ctx = AmbientContext::new();
    ctx.measure(|| {
        let _ = std::panic::catch_unwind(|| AmbientContext::new().measure(|| panic!()));
        HwMetric::Cpu.bump(1);
    });
    assert_eq!(ctx.hw_data()[HwMetric::Cpu], 1);
}

#[test]
fn out_of_order_drop_clears_the_context_before_panicking() {
    let ctx = AmbientContext::new();
    let outer = ctx.measure_guard();
    let inner = test_guard();
    HwMetric::Cpu.bump(5);

    let result = catch_unwind(AssertUnwindSafe(|| drop(outer)));
    assert_eq!(result.is_err(), cfg!(debug_assertions));
    assert!(current().context().is_none());
    assert_eq!(hw::pending()[HwMetric::Cpu], 0);
    drop(ctx);
    assert!(current().context().is_none());

    let result = catch_unwind(AssertUnwindSafe(|| drop(inner)));
    assert_eq!(result.is_err(), cfg!(debug_assertions));
    assert!(current().context().is_none());
}
