use std::ops::Range;
use std::path::Path;

use ecow::EcoString;

use super::clock::now;
use super::event::Timestamp;
use super::record::with_sink;
use super::{Event, Op, Outcome, Sink, SpanId};

pub struct IoRequest(Option<Active>);

struct Active {
    sink: Sink,
    parent: SpanId,
    started: Option<Timestamp>,
    op: Op,
    path: EcoString,
    range: Range<u64>,
    outcome: Option<Outcome>,
}

impl IoRequest {
    #[cfg_attr(debug_assertions, track_caller)]
    pub fn new(op: Op, path: &Path, range: Range<u64>) -> Self {
        Self(with_sink("IoRequest::new()", |sink, parent| Active {
            sink: sink.clone(),
            parent,
            started: None,
            op,
            path: EcoString::from(path.to_string_lossy()),
            range,
            outcome: None,
        }))
    }

    pub fn start(&mut self) {
        if let Some(active) = &mut self.0 {
            active.started.get_or_insert_with(now);
        }
    }

    pub fn finish(&mut self, outcome: Outcome) {
        if let Some(active) = &mut self.0 {
            active.outcome.get_or_insert(outcome);
        }
    }

    pub fn set_end(&mut self, end: u64) {
        if let Some(active) = &mut self.0 {
            active.range.end = end;
        }
    }
}

impl Drop for IoRequest {
    fn drop(&mut self) {
        let Some(active) = self.0.take() else { return };
        let Some(started) = active.started else {
            return;
        };
        active.sink.send(Event::Request {
            parent: active.parent,
            started,
            ended: now(),
            op: active.op,
            path: active.path,
            offset: active.range.start,
            length: active.range.end - active.range.start,
            outcome: active.outcome.unwrap_or(Outcome::Cancelled),
        });
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::ambient::trace::testing::trace;

    #[test]
    fn request_not_started() {
        let events = trace(|| drop(IoRequest::new(Op::Read, Path::new("a.bin"), 0..1)));
        assert_eq!(
            json!(events),
            json!([
                {"kind": "span_start", "id": 1, "parent": 0, "timestamp": "*", "name": "root"},
                {"kind": "span_end",   "id": 1,              "timestamp": "*"},
            ])
        );
    }

    #[test]
    fn request_finished() {
        let events = trace(|| {
            let mut request = IoRequest::new(Op::ReadFrom, Path::new("a.bin"), 4..4);
            request.start();
            request.set_end(10);
            request.finish(Outcome::Ok);
            request.finish(Outcome::Err); // the first outcome wins
        });
        assert_eq!(
            json!(events),
            json!([
                {"kind": "span_start", "id": 1, "parent": 0, "timestamp": "*", "name": "root"},
                {"kind": "request",             "parent": 1, "started": "*", "ended": "*",
                                                "op": "read_from", "path": "a.bin", "offset": 4,
                                                "length": 6, "outcome": "ok"},
                {"kind": "span_end",   "id": 1,              "timestamp": "*"},
            ])
        );
    }

    #[test]
    fn request_without_outcome() {
        let events = trace(|| IoRequest::new(Op::Read, Path::new("a.bin"), 0..1).start());
        assert_eq!(
            json!(events),
            json!([
                {"kind": "span_start", "id": 1, "parent": 0, "timestamp": "*", "name": "root"},
                {"kind": "request",             "parent": 1, "started": "*", "ended": "*",
                                                "op": "read", "path": "a.bin", "offset": 0,
                                                "length": 1, "outcome": "cancelled"},
                {"kind": "span_end",   "id": 1,              "timestamp": "*"},
            ])
        );
    }
}
