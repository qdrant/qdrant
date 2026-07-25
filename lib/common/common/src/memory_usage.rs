//! Process-wide memory usage reader with a small time-based cache.
//!
//! Strict mode uses this to reject memory-consuming updates when the process is
//! close to exhausting RAM. This crate does not depend on jemalloc: the binary
//! that links the jemalloc allocator registers a reader at startup via
//! [`set_resident_bytes_reader`]. Callers get `None` when no reader has been
//! installed (e.g. platforms without jemalloc support), and should treat that
//! as "memory check unavailable" and skip the check.

use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

/// How long to reuse a previously read memory value before refreshing.
const CACHE_TTL: Duration = Duration::from_secs(5);

/// Reader returning current process resident memory in bytes, or `None` if
/// unavailable on this invocation.
pub type ResidentBytesReader = fn() -> Option<usize>;

static READER: OnceLock<ResidentBytesReader> = OnceLock::new();
static CACHE: Mutex<Option<(Instant, usize)>> = Mutex::new(None);

/// Install the process-wide resident-memory reader. Call once at startup from
/// the binary that owns the allocator. Subsequent calls are ignored.
pub fn set_resident_bytes_reader(reader: ResidentBytesReader) {
    let _ = READER.set(reader);
}

/// Returns current process resident memory in bytes, served from a cached
/// value that refreshes at most once per [`CACHE_TTL`].
pub fn resident_bytes() -> Option<usize> {
    let reader = *READER.get()?;

    let mut guard = CACHE.lock().ok()?;
    let now = Instant::now();

    if let Some((cached_at, value)) = *guard
        && now.duration_since(cached_at) < CACHE_TTL
    {
        return Some(value);
    }

    let value = reader()?;
    *guard = Some((now, value));
    Some(value)
}
