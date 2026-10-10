use std::panic::{AssertUnwindSafe, catch_unwind};
use std::task::{Context, Waker};

use serde_json::json;

use super::hw::{self, HwMetric};
use super::trace::testing::{file_sink, lines, run};
use super::trace::{Sink, mark, span};
use super::{
    self as ambient, AmbientContext, AmbientFutureExt as _, current, parallel, test, test_guard,
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
#[cfg_attr(
    debug_assertions,
    should_panic(expected = "outside of any ambient scope")
)]
fn parallel_masks_the_scope() {
    let ctx = AmbientContext::new();
    ctx.measure(|| parallel(|_handoff| HwMetric::Cpu.bump(1)));
    assert_eq!(ctx.hw_data()[HwMetric::Cpu], 0);
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
