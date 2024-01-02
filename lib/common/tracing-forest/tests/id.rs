//! Tests for retrieving the id when there is none.
#![cfg(feature = "tokio")]
use std::panic;
use tracing::dispatcher::DefaultGuard;
use tracing_forest::{traits::*, util::*};
use tracing_subscriber::Registry;
use uuid::Uuid;

fn init() -> DefaultGuard {
    let layer = ForestLayer::sink();
    let subscriber = Registry::default().with(layer);
    tracing::subscriber::set_default(subscriber)
}

// TODO(Quinn): I think we should switch to using `Result` instead of panicking

#[test]
fn test_panic_get_id_not_in_span() {
    let _guard = init();
    panic::set_hook(Box::new(|_| {}));
    assert!(panic::catch_unwind(tracing_forest::id).is_err());
}

#[test]
fn test_panic_get_id_not_in_subscriber() {
    panic::set_hook(Box::new(|_| {}));
    assert!(panic::catch_unwind(tracing_forest::id).is_err());
}

#[test]
fn test_panic_get_id_after_close() {
    let _guard = init();

    let uuid = Uuid::new_v4();
    info_span!("in a span", %uuid).in_scope(|| {
        let _ = tracing_forest::id();
    });
    panic::set_hook(Box::new(|_| {}));
    assert!(panic::catch_unwind(tracing_forest::id).is_err());
}

#[test]
fn test_consistent_retrieval() {
    let _guard = init();
    info_span!("my_span").in_scope(|| {
        let id1 = tracing_forest::id();
        let id2 = tracing_forest::id();
        assert!(id1 == id2);
    });
}

#[test]
fn test_span_macros() {
    let _guard = init();
    let uuid = Uuid::new_v4();

    trace_span!("my_span", %uuid).in_scope(|| {
        assert_eq!(uuid, tracing_forest::id());
    });
    trace_span!("my_span", %uuid, ans = 42).in_scope(|| {
        assert_eq!(uuid, tracing_forest::id());
    });
    debug_span!("my_span", %uuid).in_scope(|| {
        assert_eq!(uuid, tracing_forest::id());
    });
    debug_span!("my_span", %uuid, ans = 42).in_scope(|| {
        assert_eq!(uuid, tracing_forest::id());
    });
    info_span!("my_span", %uuid).in_scope(|| {
        assert_eq!(uuid, tracing_forest::id());
    });
    info_span!("my_span", %uuid, ans = 42).in_scope(|| {
        assert_eq!(uuid, tracing_forest::id());
    });
    warn_span!("my_span", %uuid).in_scope(|| {
        assert_eq!(uuid, tracing_forest::id());
    });
    warn_span!("my_span", %uuid, ans = 42).in_scope(|| {
        assert_eq!(uuid, tracing_forest::id());
    });
    error_span!("my_span", %uuid).in_scope(|| {
        assert_eq!(uuid, tracing_forest::id());
    });
    error_span!("my_span", %uuid, ans = 42).in_scope(|| {
        assert_eq!(uuid, tracing_forest::id());
    });
}

#[cfg(feature = "tokio")]
#[tokio::test(flavor = "current_thread")]
async fn test_many_tasks() {
    let _guard = init();

    let mut handles = vec![];

    for _ in 0..10 {
        handles.push(tokio::spawn(async {
            let id1 = Uuid::new_v4();
            let id2 = Uuid::new_v4();

            async {
                assert!(id1 == tracing_forest::id());
                async {
                    assert!(id2 == tracing_forest::id());
                }
                .instrument(info_span!("inner", uuid = %id2))
                .await;
                assert!(id1 == tracing_forest::id());
            }
            .instrument(info_span!("outer", uuid = %id1))
            .await;
        }));
    }

    for handle in handles {
        handle.await.expect("failed to join task");
    }
}
