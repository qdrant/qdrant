//! Per-request ambient state, reached through a thread-local slot.

mod context;
mod future;
mod handoff;
pub mod hw;
mod slot;
#[cfg(test)]
mod tests;
pub mod trace;

pub use context::AmbientContext;
pub use future::{AmbientFuture, AmbientFutureExt};
pub use handoff::{Handoff, current, parallel, unmeasured, unmeasured_guard};
#[cfg(any(test, feature = "testing"))]
pub use handoff::{test, test_guard};
pub use slot::Scope;
