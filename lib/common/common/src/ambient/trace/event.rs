use serde::Serialize;

/// A single event in the log.
/// Timestamps are relative to the trace start.
#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(super) enum Event {
    /// Span-like event. Can be nested.
    /// Create with [`super::Phase::start`].
    Phase {
        /// Other events can have `parent` set to this ID.
        id: u64,
        start_ns: Nanoseconds,
        end_ns: Nanoseconds,
        name: &'static str,
    },
    /// Text log-like event.
    /// Create with [`super::mark!`].
    Mark {
        parent: u64,
        at_ns: Nanoseconds,
        text: String,
    },
    /// Single remote request.
    /// Created by UIO backend implementations, with [`super::IoRequest::new`].
    Request {
        parent: u64,
        start_ns: Nanoseconds,
        end_ns: Nanoseconds,
        op: Op,
        path: String,
        offset: u64,
        length: u64,
        outcome: Outcome,
    },
    /// Description of the file structure.
    /// Lets the visualizer distinguish offsets and links within the same file.
    /// Create with [`super::file_sections`].
    Sections {
        path: String,
        sections: Vec<(&'static str, u64)>,
    },
    /// CPU usage.
    /// Written automatically.
    Cpu {
        at_ns: Nanoseconds,
        cpu_ns: Nanoseconds,
    },
    /// Emitted when can't keep up.
    /// Written automatically.
    Dropped { count: u64 },
}

pub(super) type Nanoseconds = u64;

/// Request operation kind.
///
/// Fieldless with implicit discriminants, so `op as usize` indexes an array of
/// [`Op::COUNT`](strum::EnumCount::COUNT) entries in [`Op::iter`](strum::IntoEnumIterator::iter)
/// order.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Serialize,
    strum::EnumCount,
    strum::EnumIter,
    strum::IntoStaticStr,
)]
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
