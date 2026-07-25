//! Portable monotonic clock.
//!
//! `std::time::Instant::now()` has no implementation on `wasm32-unknown-unknown` — it panics —
//! which takes down anything that measures its own duration (operation timings, rate limiting,
//! progress tracking) the moment it runs in a browser. [`web_time`] provides the same API backed
//! by `performance.now()`, so code that would otherwise `use std::time::Instant` uses this instead
//! and stays target-agnostic.
//!
//! Only the clock types differ per target; [`std::time::Duration`] is portable and is re-exported
//! unchanged so a single import covers both.

pub use std::time::Duration;
#[cfg(not(target_arch = "wasm32"))]
pub use std::time::{Instant, SystemTime, UNIX_EPOCH};

#[cfg(target_arch = "wasm32")]
pub use web_time::{Instant, SystemTime, UNIX_EPOCH};
