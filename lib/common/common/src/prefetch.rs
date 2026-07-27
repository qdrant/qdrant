//! Best-effort software prefetch hints.

/// Cache line size assumed for prefetching, in bytes.
///
/// Some aarch64 cores use 128-byte lines; issuing a hint per 64 bytes there is
/// harmless (the second hint lands on an already-requested line).
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
/// No-op on targets other than x86_64 and aarch64.
#[inline(always)]
pub fn prefetch_slice(bytes: &[u8]) {
    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    {
        let ptr = bytes.as_ptr();
        let len = bytes.len().min(MAX_PREFETCH_LINES * CACHE_LINE);
        let mut offset = 0;
        while offset < len {
            // SAFETY: prefetch is a non-faulting hint; the computed address
            // lies within a valid borrowed slice.
            prefetch_read(unsafe { ptr.add(offset) });
            offset += CACHE_LINE;
        }
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    let _ = bytes;
}

/// Hint the CPU to pull the cache line holding `ptr` into all cache levels
/// for a subsequent read (x86_64 `prefetcht0` / aarch64 `prfm pldl1keep`).
///
/// Safe to call with any pointer: a prefetch hint never faults, even on
/// invalid addresses.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn prefetch_read(ptr: *const u8) {
    use std::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};

    // SAFETY: `_mm_prefetch` is a non-faulting hint for any address.
    unsafe { _mm_prefetch::<_MM_HINT_T0>(ptr.cast::<i8>()) }
}

/// Hint the CPU to pull the cache line holding `ptr` into all cache levels
/// for a subsequent read (x86_64 `prefetcht0` / aarch64 `prfm pldl1keep`).
///
/// Safe to call with any pointer: a prefetch hint never faults, even on
/// invalid addresses. There is no stable `core::arch` prefetch intrinsic for
/// aarch64 yet, hence inline asm.
#[cfg(target_arch = "aarch64")]
#[inline(always)]
fn prefetch_read(ptr: *const u8) {
    // SAFETY: `prfm` is a non-faulting hint for any address; it does not
    // access memory architecturally, touch the stack, or clobber flags.
    unsafe {
        core::arch::asm!(
            "prfm pldl1keep, [{ptr}]",
            ptr = in(reg) ptr,
            options(nostack, preserves_flags),
        )
    }
}
