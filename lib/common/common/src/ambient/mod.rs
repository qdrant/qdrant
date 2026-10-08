mod context;
mod future;
mod handoff;
pub mod hw;
mod slot;
#[cfg(test)]
mod tests;
pub mod trace;

pub use context::AmbientContext;
pub use future::{HwFuture, HwFutureExt};
pub use handoff::{HwHandoff, current, parallel, unmeasured, unmeasured_guard};
#[cfg(any(test, feature = "testing"))]
pub use handoff::{test, test_guard};
pub use slot::HwScope;
