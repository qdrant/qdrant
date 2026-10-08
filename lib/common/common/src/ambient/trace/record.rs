//! Record events into the current span.

use ecow::EcoString;

use super::clock::now;
use super::{Event, Sink, SpanId, global};
use crate::ambient::{AmbientContext, slot};

/// `println!`-like macro to record a [Event::Mark] in the trace.
#[doc(hidden)]
#[macro_export]
macro_rules! __ambient_mark {
    ($($arg:tt)*) => {
        $crate::ambient::trace::record_mark(|| $crate::ambient::trace::__eco_format!($($arg)*))
    };
}

/// `format!`-like macro to start a child span, see [`crate::ambient::Handoff::span`].
#[doc(hidden)]
#[macro_export]
macro_rules! __ambient_span {
    ($($arg:tt)*) => {
        $crate::ambient::current().span(|| $crate::ambient::trace::__eco_format!($($arg)*))
    };
}

pub use __ambient_mark as mark;
pub use __ambient_span as span;

/// Record [Event::Mark].
pub fn record_mark(text: impl FnOnce() -> EcoString) {
    record_here(|parent| Event::Mark {
        parent,
        timestamp: now(),
        text: text(),
    });
}

/// Record [Event::Sections].
pub fn file_sections(path: &str, sections: Vec<(&'static str, u64)>) {
    record_here(|_| Event::Sections {
        path: EcoString::from(path),
        sections,
    });
}

/// Record an event in the current span. Dropped if untraced.
fn record_here(event: impl FnOnce(SpanId) -> Event) {
    with_sink(|sink, parent| sink.send(event(parent)));
}

/// Run `f` on the sink and parent span of the current scope:
/// the span of its context, or the [`global`] sink with no parent.
pub(super) fn with_sink<R>(f: impl FnOnce(&Sink, SpanId) -> R) -> Option<R> {
    slot::with_context("trace", |ctx| match ctx.and_then(AmbientContext::traced) {
        Some((sink, parent)) => Some(f(sink, parent)),
        None => global().map(|sink| f(sink, 0)),
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::ambient;
    use crate::ambient::trace::install;
    use crate::ambient::trace::testing::{file_sink, lines, trace};

    #[test]
    fn mark_and_sections() {
        let events = trace(|| {
            mark!("round {} begin", 0);
            file_sections("a.bin", vec![("header", 0), ("data", 64)]);
        });
        assert_eq!(
            json!(events),
            json!([
                {"kind": "span_start", "id": 1, "parent": 0, "timestamp": "*", "name": "root"},
                {"kind": "mark",                "parent": 1, "timestamp": "*", "text": "round 0 begin"},
                {"kind": "sections", "path": "a.bin", "sections": [["header", 0], ["data", 64]]},
                {"kind": "span_end",   "id": 1,              "timestamp": "*"},
            ])
        );
    }

    #[test]
    fn nested_span() {
        let events = trace(|| {
            span!("child").enter(|| mark!("inside"));
            mark!("outside");
        });
        assert_eq!(
            json!(events),
            json!([
                {"kind": "span_start", "id": 1, "parent": 0, "timestamp": "*", "name": "root"},
                {"kind": "span_start", "id": 2, "parent": 1, "timestamp": "*", "name": "child"},
                {"kind": "mark",                "parent": 2, "timestamp": "*", "text": "inside"},
                {"kind": "span_end",   "id": 2,              "timestamp": "*"},
                {"kind": "mark",                "parent": 1, "timestamp": "*", "text": "outside"},
                {"kind": "span_end",   "id": 1,              "timestamp": "*"},
            ])
        );
    }

    #[test]
    fn span_ends_on_drop_after_children() {
        let (_file, sink) = file_sink();
        let root = AmbientContext::root("root", None, Some(sink.clone()));
        let (dropped, open) = root.measure(|| (span!("dropped"), span!("open")));
        drop(dropped);
        drop(root); // kept alive by `open`
        drop(open);
        sink.stop();
        assert_eq!(
            json!(lines(&sink)),
            json!([
                {"kind": "span_start", "id": 1, "parent": 0, "timestamp": "*", "name": "root"},
                {"kind": "span_start", "id": 2, "parent": 1, "timestamp": "*", "name": "dropped"},
                {"kind": "span_start", "id": 3, "parent": 1, "timestamp": "*", "name": "open"},
                {"kind": "span_end",   "id": 2,              "timestamp": "*"},
                {"kind": "span_end",   "id": 3,              "timestamp": "*"},
                {"kind": "span_end",   "id": 1,              "timestamp": "*"},
            ])
        );
    }

    #[test]
    fn rootless_events_go_to_the_global_sink() {
        if std::env::var_os("NEXTEST").is_none() {
            // `install()` is not hermetic (touches global var). So, allow only
            // in nextest, which runs each test in a separate process.
            return;
        }

        let (_file, sink) = file_sink();
        assert!(install(sink.clone()).is_ok());
        assert!(global().is_some());
        ambient::test(|| {
            mark!("rootless");
            // No context to hang a span on: a no-op.
            assert!(span!("rootless span").context().is_none());
        });
        sink.stop();
        assert_eq!(
            json!(lines(&sink)),
            json!([{"kind": "mark", "parent": 0, "timestamp": "*", "text": "rootless"}])
        );
    }
}
