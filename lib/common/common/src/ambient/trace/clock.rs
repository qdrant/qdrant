use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use super::SpanId;
use super::event::Timestamp;

pub(crate) fn now() -> Timestamp {
    SystemTime::UNIX_EPOCH
        .elapsed()
        .map_or(0, |since| since.as_nanos() as Timestamp)
}

/// Span ids. Seeded from the clock, so processes sharing a log rarely collide;
/// kept under 2^53, so JSON readers keep the digits.
pub(crate) fn next_id() -> SpanId {
    static NEXT_ID: LazyLock<AtomicU64> = LazyLock::new(|| AtomicU64::new((now() >> 11).max(1)));
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}
