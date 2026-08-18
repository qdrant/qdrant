//! Best-effort software prefetch hints.

/// Cache line size assumed for prefetching, in bytes. On aarch64 cores with
/// 128-byte lines the second hint per line is harmless.
const CACHE_LINE: usize = 64;

/// Bytes of near (L1) prefetch lead; also the per-call cap of
/// [`prefetch_slice`] / [`prefetch_slice_l2`]. ~1 KiB covers one uncontended
/// DRAM fill at the scoring kernels' pace and is the most L1 can take without
/// evicting data still being scored; the hardware prefetcher covers the tail
/// of larger vectors.
const NEAR_BYTES: usize = 1024;

/// Bytes of far (L2) prefetch lead, sized to the ~3x longer fill latency when
/// many threads queue on the memory controller.
const FAR_BYTES: usize = 4096;

/// Largest batch read without prefetch hints: in tiny batches every hint
/// fires only moments before the read, too late to help. Deliberately not
/// scaled with the near window — that would silently disable prefetch for
/// common batch sizes (HNSW neighbor expansions) once the window is deep.
pub const MAX_UNPREFETCHED_BATCH: usize = 2;

/// Near (L1, [`prefetch_slice`]) and far (L2, [`prefetch_slice_l2`]) prefetch
/// window sizes in vectors, covering a constant [`NEAR_BYTES`] / [`FAR_BYTES`]
/// of lead — bytes, not vector counts, map to the time a fetch has to
/// complete.
///
/// Sub-cache-line vectors get no far window (`far == 0`, callers skip the L2
/// hints): their far hints would fire only nanoseconds before the near ones,
/// and under contention the too-early L2 installs get evicted before use.
#[inline]
pub fn prefetch_windows(vector_size_bytes: usize) -> (usize, usize) {
    let near = (NEAR_BYTES / vector_size_bytes.max(1)).clamp(1, 8);

    // Disable L2 far prefetch for small vectors.
    if vector_size_bytes < CACHE_LINE {
        return (near, 0);
    }

    let far = (FAR_BYTES / vector_size_bytes).clamp(near + 2, 16);
    (near, far)
}

/// Best-effort prefetch (temporal, all cache levels) of the cache lines
/// spanning `bytes`, capped at [`NEAR_BYTES`]. A prefetch is only a hint and
/// never faults; no-op on targets other than x86_64 and aarch64.
#[inline(always)]
pub fn prefetch_slice(bytes: &[u8]) {
    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    {
        let ptr = bytes.as_ptr();
        let len = bytes.len().min(NEAR_BYTES);
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

/// Like [`prefetch_slice`], but installs the lines into L2 instead of L1:
/// early lines survive in L2 until use, while an equally early L1 install
/// would evict data still being read.
#[inline(always)]
pub fn prefetch_slice_l2(bytes: &[u8]) {
    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    {
        let ptr = bytes.as_ptr();
        let len = bytes.len().min(NEAR_BYTES);
        let mut offset = 0;
        while offset < len {
            // SAFETY: prefetch is a non-faulting hint; the computed address
            // lies within a valid borrowed slice.
            prefetch_read_l2(unsafe { ptr.add(offset) });
            offset += CACHE_LINE;
        }
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    let _ = bytes;
}

/// Hint to pull the line holding `ptr` into all cache levels (`prefetcht0`).
/// Never faults, even on invalid addresses.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn prefetch_read(ptr: *const u8) {
    use std::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};

    // SAFETY: `_mm_prefetch` is a non-faulting hint for any address.
    unsafe { _mm_prefetch::<_MM_HINT_T0>(ptr.cast::<i8>()) }
}

/// Hint to pull the line holding `ptr` into all cache levels
/// (`prfm pldl1keep`). Never faults; inline asm because there is no stable
/// `core::arch` prefetch intrinsic for aarch64.
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

/// Hint to pull the line holding `ptr` into L2, not L1 (`prefetcht1`).
/// Never faults, even on invalid addresses.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn prefetch_read_l2(ptr: *const u8) {
    use std::arch::x86_64::{_MM_HINT_T1, _mm_prefetch};

    // SAFETY: `_mm_prefetch` is a non-faulting hint for any address.
    unsafe { _mm_prefetch::<_MM_HINT_T1>(ptr.cast::<i8>()) }
}

/// Hint to pull the line holding `ptr` into L2, not L1 (`prfm pldl2keep`).
/// Never faults; inline asm because there is no stable `core::arch` prefetch
/// intrinsic for aarch64.
#[cfg(target_arch = "aarch64")]
#[inline(always)]
fn prefetch_read_l2(ptr: *const u8) {
    // SAFETY: `prfm` is a non-faulting hint for any address; it does not
    // access memory architecturally, touch the stack, or clobber flags.
    unsafe {
        core::arch::asm!(
            "prfm pldl2keep, [{ptr}]",
            ptr = in(reg) ptr,
            options(nostack, preserves_flags),
        )
    }
}
