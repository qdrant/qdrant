//! Statistics of the remote requests issued through [`BlobFs`](crate::BlobFs) and
//! [`BlobFile`](crate::BlobFile), one [`OpStats`] per [`Op`]. One append or save is one
//! request regardless of how the backend performs it; backend retries and fallbacks are
//! not visible at this layer.

use std::ops::Range;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use common::uio_trace::{self, Op, Outcome};
use common::universal_io::{IsNotFound, OpGuard, OpStats, OpStatsSnapshot};
use strum::{EnumCount as _, IntoEnumIterator as _};

const OPS: usize = Op::COUNT;

/// Cloneable observer shared by a filesystem, its clones, and the files it opens.
#[derive(Clone, Debug, Default)]
pub struct RemoteIoStats(Arc<[OpStats; OPS]>);

impl RemoteIoStats {
    pub fn op(&self, op: Op) -> &OpStats {
        &self.0[op as usize]
    }

    pub fn snapshot(&self) -> RemoteIoStatsSnapshot {
        RemoteIoStatsSnapshot {
            ops: std::array::from_fn(|i| self.0[i].snapshot()),
        }
    }

    /// Observe one remote request in both the statistics and the trace. `range` is the
    /// payload the request transfers; its length is counted as bytes on success.
    pub(crate) fn request(&self, op: Op, path: &Path, range: Range<u64>) -> RequestObserver {
        RequestObserver {
            trace: uio_trace::Request::new(op, path, range.clone()),
            stats: self.op(op).clone(),
            guard: None,
            range,
        }
    }
}

/// Cumulative per-operation snapshot; see [`OpStatsSnapshot`] for the field semantics.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RemoteIoStatsSnapshot {
    ops: [OpStatsSnapshot; OPS],
}

impl RemoteIoStatsSnapshot {
    pub fn op(&self, op: Op) -> &OpStatsSnapshot {
        &self.ops[op as usize]
    }

    pub fn iter(&self) -> impl Iterator<Item = (Op, &OpStatsSnapshot)> {
        Op::iter().zip(&self.ops)
    }

    /// All operations summed together.
    pub fn total(&self) -> OpStatsSnapshot {
        let mut total = OpStatsSnapshot::default();
        for op in &self.ops {
            total += op;
        }
        total
    }

    pub fn is_empty(&self) -> bool {
        self.ops.iter().all(OpStatsSnapshot::is_empty)
    }

    /// Interval counters against an earlier snapshot of the same observer.
    pub fn delta_since(&self, earlier: &Self) -> Self {
        Self {
            ops: std::array::from_fn(|i| self.ops[i].delta_since(&earlier.ops[i])),
        }
    }

    /// Summary line and latency histogram per operation kind with activity; `None` when
    /// there is none.
    pub fn format_compact(&self) -> Option<String> {
        let lines: Vec<String> = self
            .iter()
            .filter_map(|(op, stats)| {
                Some(format!("{}: {}", <&str>::from(op), stats.format_compact()?))
            })
            .collect();
        (!lines.is_empty()).then(|| lines.join("\n"))
    }
}

/// One remote request under observation. Mirrors the [`uio_trace::Request`] lifecycle so
/// both the trace and the statistics see the same start, outcome and payload length; a
/// started request dropped without an outcome counts as abandoned.
pub(crate) struct RequestObserver {
    trace: uio_trace::Request,
    stats: OpStats,
    guard: Option<OpGuard>,
    range: Range<u64>,
}

impl RequestObserver {
    pub fn start(&mut self) {
        self.trace.start();
        if self.guard.is_none() {
            self.guard = Some(self.stats.start(Instant::now()));
        }
    }

    /// Set the payload end once the response reveals it.
    pub fn set_end(&mut self, end: u64) {
        self.trace.set_end(end);
        self.range.end = end;
    }

    pub fn set(&mut self, outcome: Outcome) {
        self.trace.set(outcome);
        let Some(guard) = self.guard.take() else {
            return;
        };
        match outcome {
            Outcome::Ok => guard.complete(self.range.end.saturating_sub(self.range.start) as usize),
            Outcome::NotFound => guard.not_found(),
            Outcome::Err => guard.failed(),
            Outcome::Cancelled => drop(guard),
        }
    }

    pub fn set_result<T, E: IsNotFound>(&mut self, result: &Result<T, E>) {
        match result {
            Ok(_) => self.set(Outcome::Ok),
            Err(err) => self.set_err(err),
        }
    }

    pub fn set_err(&mut self, err: &impl IsNotFound) {
        self.set(if err.is_not_found() {
            Outcome::NotFound
        } else {
            Outcome::Err
        });
    }

    pub async fn wrap<T, E: IsNotFound>(
        mut self,
        future: impl Future<Output = Result<T, E>>,
    ) -> Result<T, E> {
        self.start();
        let result = future.await;
        self.set_result(&result);
        result
    }
}
