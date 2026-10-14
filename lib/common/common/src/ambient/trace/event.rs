//! Types that would go into JSON.
//!
//! No logic.

use std::collections::VecDeque;

use ecow::EcoString;
use serde::Serialize;
use strum::{EnumCount, EnumIter, IntoStaticStr};

/// A single event in the log.
#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Event {
    /// A span opens; other events have `parent` set to its `id`, a root's own `parent` is `0`.
    /// Any event outside a span, recorded into the global sink, has `parent` set to `0` too.
    /// Create with [`super::span!`] or [`crate::ambient::AmbientContext::root`].
    SpanStart {
        id: SpanId,
        parent: SpanId,
        timestamp: Timestamp,
        name: EcoString,
    },
    /// The span closes. Created automatically on drop.
    SpanEnd { id: SpanId, timestamp: Timestamp },
    /// Text log-like event.
    /// Create with [`super::mark!`].
    Mark {
        parent: SpanId,
        timestamp: Timestamp,
        text: EcoString,
    },
    /// Single remote request.
    /// Created by UIO backend implementations, with [`super::IoRequest::new`].
    Request {
        parent: SpanId,
        started: Timestamp,
        ended: Timestamp,
        op: Op,
        path: EcoString,
        offset: u64,
        length: u64,
        outcome: Outcome,
    },
    /// Description of the file structure.
    /// Lets the visualizer distinguish offsets and links within the same file.
    /// Create with [`super::file_sections`].
    Sections {
        path: EcoString,
        sections: Vec<(&'static str, u64)>,
    },
    /// CPU usage.
    /// Create with [`super::CpuSampler`] and append to trace manually.
    Cpu {
        timestamps: VecDeque<Timestamp>,
        cpu_ns: VecDeque<u64>,
    },
    /// Emitted when can't keep up.
    /// Written automatically.
    Dropped { count: u64 },
}

/// Nanoseconds since the Unix epoch.
pub(super) type Timestamp = u64;
pub(in super::super) type SpanId = u64;

/// Request operation kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, EnumCount, EnumIter, IntoStaticStr)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum Op {
    List,
    Exists,
    Read,
    ReadFrom,
    Len,
    Create,
    Remove,
    Save,
    Append,
}

/// Request outcome.
#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Ok,
    /// The target does not exist: an expected answer to existence and length probes,
    /// kept apart from failures.
    NotFound,
    Err,
    Cancelled,
}

#[cfg(test)]
mod tests {
    use strum::IntoEnumIterator as _;

    use super::*;

    #[test]
    fn op_iter_is_in_discriminant_order() {
        for (i, op) in Op::iter().enumerate() {
            assert_eq!(op as usize, i);
            assert_eq!(serde_json::to_value(op).unwrap(), <&str>::from(op));
        }
    }
}
