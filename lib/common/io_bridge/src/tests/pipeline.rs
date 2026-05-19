use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use common::generic_consts::Random;
use common::universal_io::{BorrowedReadPipeline, ReadRange, UniversalIoError};
use tokio::runtime::Runtime;

use super::mock_backend::{
    FailingMockBackend, FailingMockFile, MockFile, StaticMockBackend, build_pipeline_with_backend,
    build_pipeline_with_static_contents,
};
use crate::{AsyncDispatcher, IoBridgeReadPipeline};

#[test]
fn pipeline_returns_scheduled_read_via_wait() {
    let (_runtime, file) = build_pipeline_with_static_contents(b"hello world");

    let mut pipeline: IoBridgeReadPipeline<'_, MockFile, u8, u64> =
        IoBridgeReadPipeline::new().expect("pipeline new");
    pipeline
        .schedule::<Random>(42u64, &file, ReadRange::new(6, 5))
        .expect("schedule");

    let (user_data, bytes) = pipeline
        .wait()
        .expect("wait ok")
        .expect("expected a completed result");
    assert_eq!(user_data, 42);
    assert_eq!(bytes.as_ref(), b"world");

    assert!(
        pipeline.wait().expect("wait ok").is_none(),
        "no more in-flight reads",
    );
}

#[test]
fn pipeline_rejects_schedule_past_configured_max_concurrency() {
    let (_runtime, file) = build_pipeline_with_static_contents(b"abc");

    let mut pipeline: IoBridgeReadPipeline<'_, MockFile, u8, u64> =
        IoBridgeReadPipeline::with_max_concurrency(2).expect("pipeline new");

    pipeline
        .schedule::<Random>(0, &file, ReadRange::new(0, 1))
        .expect("first schedule");
    pipeline
        .schedule::<Random>(1, &file, ReadRange::new(1, 1))
        .expect("second schedule");

    assert!(
        !pipeline.can_schedule(),
        "can_schedule should be false at cap",
    );

    let err = pipeline
        .schedule::<Random>(2, &file, ReadRange::new(2, 1))
        .expect_err("third schedule should fail at cap");
    assert!(
        matches!(err, UniversalIoError::QueueIsFull),
        "expected QueueIsFull, got {err:?}",
    );
}

#[test]
fn pipeline_reads_multi_byte_elements_at_element_offset() {
    let source = vec![1u32, 2u32, 3u32, 4u32];
    let bytes: Vec<u8> = bytemuck::cast_slice(&source).to_vec();
    let (_runtime, file) = build_pipeline_with_static_contents(&bytes);

    let mut pipeline: IoBridgeReadPipeline<'_, MockFile, u32, u64> =
        IoBridgeReadPipeline::new().expect("pipeline new");

    pipeline
        .schedule::<Random>(0, &file, ReadRange::new(4, 2))
        .expect("schedule");
    let (_u, items) = pipeline.wait().expect("wait ok").expect("expected result");
    assert_eq!(items.as_ref(), &[2u32, 3u32]);
}

#[test]
fn pipeline_uses_each_file_dispatcher_in_same_batch() {
    let runtime = Runtime::new().expect("runtime");
    let handle = runtime.handle().clone();
    let first_file = MockFile {
        dispatcher: Arc::new(AsyncDispatcher::new(
            handle.clone(),
            StaticMockBackend::new(b"first!".to_vec()),
        )),
    };
    let second_file = MockFile {
        dispatcher: Arc::new(AsyncDispatcher::new(
            handle,
            StaticMockBackend::new(b"second".to_vec()),
        )),
    };

    let mut pipeline: IoBridgeReadPipeline<'_, MockFile, u8, u64> =
        IoBridgeReadPipeline::new().expect("pipeline new");
    pipeline
        .schedule::<Random>(0, &first_file, ReadRange::new(0, 6))
        .expect("schedule first file");
    pipeline
        .schedule::<Random>(1, &second_file, ReadRange::new(0, 6))
        .expect("schedule second file");

    let mut results = Vec::new();
    while let Some((u, bytes)) = pipeline.wait().expect("wait ok") {
        results.push((u, bytes.into_owned()));
    }
    results.sort_by_key(|(u, _)| *u);

    assert_eq!(
        results,
        vec![(0, b"first!".to_vec()), (1, b"second".to_vec())],
    );
}

#[test]
fn pipeline_propagates_backend_error_to_wait() {
    let runtime = Runtime::new().expect("runtime");
    let handle = runtime.handle().clone();
    let dispatcher = Arc::new(AsyncDispatcher::new(handle, FailingMockBackend));
    let file = FailingMockFile { dispatcher };

    let mut pipeline: IoBridgeReadPipeline<'_, FailingMockFile, u8, u64> =
        IoBridgeReadPipeline::new().expect("pipeline new");
    pipeline
        .schedule::<Random>(0, &file, ReadRange::new(0, 1))
        .expect("schedule");

    let res = pipeline.wait();
    let Err(err) = res else {
        panic!("expected wait() to surface the backend error, got Ok(..)");
    };
    assert!(
        matches!(&err, UniversalIoError::Uninitialized { description } if description.contains("simulated")),
        "expected wrapped backend failure, got {err:?}",
    );
}

#[test]
fn pipeline_succeeds_for_sibling_ranges_when_one_fails() {
    let runtime = Runtime::new().expect("runtime");
    let handle = runtime.handle().clone();

    let backend = StaticMockBackend::new(b"hello".to_vec());
    let (_rt, ok_file) = (
        runtime,
        MockFile {
            dispatcher: Arc::new(AsyncDispatcher::new(handle.clone(), backend)),
        },
    );
    let failing_dispatcher = Arc::new(AsyncDispatcher::new(handle, FailingMockBackend));
    let failing_file = FailingMockFile {
        dispatcher: failing_dispatcher,
    };

    let mut ok_pipeline: IoBridgeReadPipeline<'_, MockFile, u8, u64> =
        IoBridgeReadPipeline::new().expect("pipeline new");
    let mut failing_pipeline: IoBridgeReadPipeline<'_, FailingMockFile, u8, u64> =
        IoBridgeReadPipeline::new().expect("pipeline new");

    ok_pipeline
        .schedule::<Random>(7, &ok_file, ReadRange::new(0, 5))
        .expect("ok schedule");
    failing_pipeline
        .schedule::<Random>(0, &failing_file, ReadRange::new(0, 1))
        .expect("failing schedule");

    let (u, bytes) = ok_pipeline
        .wait()
        .expect("ok pipeline wait")
        .expect("expected ok result");
    assert_eq!(u, 7);
    assert_eq!(bytes.as_ref(), b"hello");

    assert!(
        failing_pipeline.wait().is_err(),
        "failing pipeline still surfaces its error",
    );
}

#[test]
fn dropping_pipeline_mid_flight_does_not_panic() {
    let backend = StaticMockBackend::new(vec![1u8; 100]).with_delay(Duration::from_millis(20));
    let (runtime, file) = build_pipeline_with_backend(backend);

    {
        let mut pipeline: IoBridgeReadPipeline<'_, MockFile, u8, u64> =
            IoBridgeReadPipeline::new().expect("pipeline new");
        for i in 0..4 {
            pipeline
                .schedule::<Random>(i, &file, ReadRange::new(i * 10, 10))
                .expect("schedule");
        }
    }

    std::thread::sleep(Duration::from_millis(60));

    let mut next: IoBridgeReadPipeline<'_, MockFile, u8, u64> =
        IoBridgeReadPipeline::new().expect("pipeline new");
    next.schedule::<Random>(99, &file, ReadRange::new(0, 1))
        .expect("schedule on still-alive dispatcher");
    let (u, _) = next.wait().expect("wait ok").expect("result present");
    assert_eq!(u, 99, "runtime + dispatcher still functional");

    drop(runtime);
}

#[test]
fn pipeline_correlates_user_data_under_out_of_order_completion() {
    let delays = HashMap::from([
        (0u64, Duration::from_millis(60)),
        (3u64, Duration::from_millis(40)),
        (6u64, Duration::from_millis(20)),
    ]);
    let backend = StaticMockBackend::new(b"AAABBBCCC".to_vec()).with_delays_by_offset(delays);
    let (_runtime, file) = build_pipeline_with_backend(backend);

    let mut pipeline: IoBridgeReadPipeline<'_, MockFile, u8, String> =
        IoBridgeReadPipeline::new().expect("pipeline new");

    pipeline
        .schedule::<Random>("first".to_string(), &file, ReadRange::new(0, 3))
        .expect("schedule first");
    pipeline
        .schedule::<Random>("second".to_string(), &file, ReadRange::new(3, 3))
        .expect("schedule second");
    pipeline
        .schedule::<Random>("third".to_string(), &file, ReadRange::new(6, 3))
        .expect("schedule third");

    let mut completion_order: Vec<(String, Vec<u8>)> = Vec::new();
    while let Some((u, bytes)) = pipeline.wait().expect("wait ok") {
        completion_order.push((u, bytes.into_owned()));
    }

    assert_eq!(
        completion_order,
        vec![
            ("third".to_string(), b"CCC".to_vec()),
            ("second".to_string(), b"BBB".to_vec()),
            ("first".to_string(), b"AAA".to_vec()),
        ],
        "completion order should be reverse of schedule order; correlation must still match each U to its bytes",
    );
}

#[test]
fn pipeline_default_concurrency_is_effectively_unbounded() {
    let backend = StaticMockBackend::new(vec![1u8; 256]);
    let (_runtime, file) = build_pipeline_with_backend(backend);

    let mut pipeline: IoBridgeReadPipeline<'_, MockFile, u8, u64> =
        IoBridgeReadPipeline::new().expect("pipeline new");
    for i in 0..100u64 {
        pipeline
            .schedule::<Random>(i, &file, ReadRange::new(0, 1))
            .expect("schedule should not be capped");
    }
    assert!(
        pipeline.can_schedule(),
        "default cap must still permit further schedules at 100 in-flight",
    );

    while pipeline.wait().expect("wait ok").is_some() {}
}

#[test]
fn schedule_returns_error_when_dispatcher_loop_is_dead() {
    let runtime = Runtime::new().expect("runtime");
    let handle = runtime.handle().clone();
    let dispatcher = Arc::new(AsyncDispatcher::new(
        handle,
        StaticMockBackend::new(vec![1, 2, 3]),
    ));
    let file = MockFile {
        dispatcher: Arc::clone(&dispatcher),
    };

    drop(runtime);
    std::thread::sleep(Duration::from_millis(20));

    let mut pipeline: IoBridgeReadPipeline<'_, MockFile, u8, u64> =
        IoBridgeReadPipeline::new().expect("pipeline new");
    let res = pipeline.schedule::<Random>(0, &file, ReadRange::new(0, 1));
    assert!(
        matches!(res, Err(UniversalIoError::Uninitialized { .. })),
        "expected dispatcher-closed error after runtime drop, got {res:?}",
    );
}
