//! Process-wide resident memory reader.
//!
//! This crate does not depend on jemalloc: the binary that links the jemalloc
//! allocator registers a reader at startup via [`set_resident_bytes_reader`].
//! Callers get `None` when no reader has been installed (e.g. platforms without
//! jemalloc support), and should treat that as "memory check unavailable" and
//! skip the check.
//!
//! Reading is not free — it advances the jemalloc stats epoch — and this module
//! deliberately does not cache. Deciding when a sample is stale needs to know
//! what the sample is compared against, so that policy lives with the caller
//! that owns the limits (`storage::quota::QuotaManager`).

use std::sync::OnceLock;

/// Reader returning current process resident memory in bytes, or `None` if
/// unavailable on this invocation.
pub type ResidentBytesReader = fn() -> Option<usize>;

static READER: OnceLock<ResidentBytesReader> = OnceLock::new();

/// Install the process-wide resident-memory reader. Call once at startup from
/// the binary that owns the allocator. Subsequent calls are ignored.
pub fn set_resident_bytes_reader(reader: ResidentBytesReader) {
    let _ = READER.set(reader);
}

/// Current process resident memory in bytes, read fresh on every call.
pub fn resident_bytes() -> Option<usize> {
    READER.get()?()
}
