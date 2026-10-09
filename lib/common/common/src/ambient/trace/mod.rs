//! Network request tracing for UIO.
//!
//! Visualize with `tools/uio-trace-visualizer.html`.

mod cpu;
mod event;
mod record;
mod request;
mod sink;
mod span;

pub use event::{Op, Outcome};
pub use record::{file_sections, mark, record_mark};
pub use request::IoRequest;
pub use sink::{FlushGuard, enabled, start};
pub use span::{Context, Phase, WithCtx};
