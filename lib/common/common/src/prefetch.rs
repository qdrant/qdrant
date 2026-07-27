//! Best-effort software prefetch hints.

/// Cache line size assumed for prefetching, in bytes.
const CACHE_LINE: usize = 64;

/// Upper bound on cache lines prefetched per [`prefetch_slice`] call.
///
/// Prefetching is meant to hide the latency of the first touch of scattered
/// data; issuing hints for a large slice wastes line-fill buffer slots and can
/// evict data that is still needed. 16 lines (1 KiB, i.e. a 256-dim f32
/// vector) is enough to cover the hot prefix of a vector while the tail is
/// pulled in by the hardware prefetcher during the sequential scoring scan.
const MAX_PREFETCH_LINES: usize = 16;

/// Best-effort software prefetch (temporal, all cache levels) of the cache
/// lines spanning `bytes`, capped at [`MAX_PREFETCH_LINES`].
///
/// Experimental: used to overlap the scattered-load latency of HNSW batch
/// scoring. A prefetch is only a hint and never faults, so this is always safe.
/// No-op on non-x86_64 targets (aarch64 has no stable prefetch intrinsic).
#[inline(always)]
pub fn prefetch_slice(bytes: &[u8]) {
    #[cfg(target_arch = "x86_64")]
    {
        use std::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};

        let ptr = bytes.as_ptr();
        let len = bytes.len().min(MAX_PREFETCH_LINES * CACHE_LINE);
        let mut offset = 0;
        while offset < len {
            // SAFETY: `_mm_prefetch` is a non-faulting hint; the computed address
            // lies within a valid borrowed slice.
            unsafe { _mm_prefetch::<_MM_HINT_T0>(ptr.add(offset).cast::<i8>()) };
            offset += CACHE_LINE;
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    let _ = bytes;
}
