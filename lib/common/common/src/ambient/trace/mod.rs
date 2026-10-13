//! Tracing: spans, marks, file sections, IO requests and CPU samples of a
//! request.
//!
//! Produces jsonl files that can be visualized with
//! `tools/uio-trace-visualizer.html`.

mod clock;
mod cpu;
mod event;
mod record;
mod request;
mod sink;
#[cfg(test)]
pub(super) mod testing;

pub(super) use clock::{next_id, now};
pub use cpu::CpuSampler;
#[doc(hidden)]
pub use ecow::eco_format as __eco_format;
pub(super) use event::SpanId;
pub use event::{Event, Op, Outcome};
pub use record::{file_sections, mark, record_mark, span};
pub use request::IoRequest;
pub use sink::{Sink, global, install};
