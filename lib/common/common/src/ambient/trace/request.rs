use std::future::Future;
use std::ops::Range;
use std::path::Path;
use std::time::Instant;

use super::event::Event;
use super::sink::{SINK, elapsed_ns, enabled};
use super::span::Context;
use super::{Op, Outcome};

pub struct IoRequest(Option<Active>);

struct Active {
    parent: u64,
    started: Option<Instant>,
    op: Op,
    path: String,
    range: Range<u64>,
    outcome: Option<Outcome>,
}

impl IoRequest {
    pub fn new(op: Op, path: &Path, range: Range<u64>) -> Self {
        Self(enabled().then(|| Active {
            parent: Context::current().0,
            started: None,
            op,
            path: path.to_string_lossy().into_owned(),
            range,
            outcome: None,
        }))
    }

    pub fn start(&mut self) {
        if let Some(active) = &mut self.0 {
            active.started.get_or_insert_with(Instant::now);
        }
    }

    pub fn finish(&mut self, outcome: Outcome) {
        if let Some(active) = &mut self.0 {
            active.outcome.get_or_insert(outcome);
        }
    }

    pub fn set_result<T, E>(&mut self, result: &Result<T, E>) {
        self.finish(match result {
            Ok(_) => Outcome::Ok,
            Err(_) => Outcome::Err,
        });
    }

    pub fn set_end(&mut self, end: u64) {
        if let Some(active) = &mut self.0 {
            active.range.end = end;
        }
    }

    pub async fn wrap<T, E>(mut self, future: impl Future<Output = Result<T, E>>) -> Result<T, E> {
        self.start();
        let result = future.await;
        self.set_result(&result);
        result
    }
}

impl Drop for IoRequest {
    fn drop(&mut self) {
        let (Some(active), Some(sink)) = (self.0.take(), SINK.get()) else {
            return;
        };
        let Some(started) = active.started else {
            return;
        };
        sink.send(Event::Request {
            parent: active.parent,
            start_ns: elapsed_ns(sink.origin, started),
            end_ns: elapsed_ns(sink.origin, Instant::now()),
            op: active.op,
            path: active.path,
            offset: active.range.start,
            length: active.range.end - active.range.start,
            outcome: active.outcome.unwrap_or(Outcome::Cancelled),
        });
    }
}
