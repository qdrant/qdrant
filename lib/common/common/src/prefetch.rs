//! Best-effort software prefetch hints.

/// Cache line size assumed for prefetching, in bytes. On aarch64 cores with
/// 128-byte lines the second hint per line is harmless.
const CACHE_LINE: usize = 64;

/// Bytes of near (L1) prefetch lead; also the per-call cap of
/// [`prefetch_slice`] / [`prefetch_slice_l2`], i.e. 16 cache lines per hinted
/// vector. Measured: 16 hinted lines are exactly what trains the hardware
/// stream prefetcher, which then covers the tail of larger vectors at L2 hit
/// pace; hinting 32 lines regresses (Zen 5), so the cap is load-bearing.
const NEAR_BYTES: usize = 1024;

/// Bytes of far (L2) prefetch lead. The lead must cover the loaded memory
/// latency — measured while the scoring kernel's own fills are in flight,
/// ~2x the idle latency — at the kernel's consumption rate. 4 KiB sits at
/// the measured knee of the fastest-consuming kernel that is still
/// latency-bound (TurboQuant 4-bit, ~17 B/ns x ~250 ns loaded); shallower
/// windows regress it, deeper ones measured no further gain.
const FAR_BYTES: usize = 4096;

/// Largest batch read without prefetch hints: in tiny batches every hint
/// fires only moments before the read, too late to help. Deliberately not
/// scaled with the near window — that would silently disable prefetch for
/// common batch sizes (HNSW neighbor expansions) once the window is deep.
pub const MAX_UNPREFETCHED_BATCH: usize = 2;

/// Smallest per-segment quantized storage that still benefits from prefetch
/// hints. A storage at or below this size stays resident in a core's private
/// L2 (and low L3), so its vectors are already hot when scored and the hints
/// only add issue overhead. Measured: a cache-resident binary/128 storage
/// (~0.4 MB per segment) regressed ~2-4% under contention across Zen 2/3 and
/// Intel, while its ~3.2 MB per-segment 1024-dim counterpart, which overflows
/// L2, kept the full win. 1 MiB sits above the 512 KB-1 MB L2 of common server
/// cores and below that overflow point. Compared against the storage's own
/// byte size, so it scales with how points are split across segments.
pub const MIN_PREFETCH_STORAGE_BYTES: usize = 1 << 20;

/// Near (L1, [`prefetch_slice`]) and far (L2, [`prefetch_slice_l2`]) prefetch
/// window sizes in vectors, covering a constant [`NEAR_BYTES`] / [`FAR_BYTES`]
/// of lead — bytes, not vector counts, map to the time a fetch has to
/// complete.
///
/// Sub-cache-line vectors get no far window (`far == 0`, callers skip the L2
/// hints): kernels over such small codes consume faster than memory can
/// deliver, so extra window depth measured no effect in either direction and
/// the hints are pure issue overhead.
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

/// Like [`prefetch_slice`], but requests install no deeper than L2, so early
/// lines wait there until use instead of displacing L1 data still being
/// scored. Cores may ignore the level hint (measured: Zen 3 fills L1 for T1
/// exactly as for T0; Zen 4/5, Intel and Apple stop at L2) — both behaviors
/// are safe.
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

/// Hint to pull the line holding `ptr` no deeper than L2 (`prefetcht1`).
/// Never faults, even on invalid addresses.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn prefetch_read_l2(ptr: *const u8) {
    use std::arch::x86_64::{_MM_HINT_T1, _mm_prefetch};

    // SAFETY: `_mm_prefetch` is a non-faulting hint for any address.
    unsafe { _mm_prefetch::<_MM_HINT_T1>(ptr.cast::<i8>()) }
}

/// Hint to pull the line holding `ptr` no deeper than L2 (`prfm pldl2keep`).
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

#[cfg(test)]
mod tests {
    use super::*;

    /// L1 fill-buffer (MSHR) budget the near window must stay within, in
    /// cache lines. 24 is the measured-safe envelope: the schedule's worst
    /// band (115–146-byte codes, ~22–23.8 average lines in flight) benched
    /// clean end-to-end, while cores with fewer buffers (Intel ~10-12 LFBs)
    /// drop excess hints gracefully.
    const MSHR_LINE_BUDGET: f64 = 24.0;

    fn gcd(a: usize, b: usize) -> usize {
        if b == 0 { a } else { gcd(b, a % b) }
    }

    /// Average cache lines one hint touches, exact over the cycle of in-line
    /// offsets produced by packing vectors densely at `stride` bytes with
    /// byte alignment (in-line offset of vector `i` is `i * stride % 64`).
    fn avg_lines_per_hint(stride: usize, hint_bytes: usize) -> f64 {
        let cycle = CACHE_LINE / gcd(stride, CACHE_LINE);
        let total: usize = (0..cycle)
            .map(|i| ((i * stride) % CACHE_LINE + hint_bytes).div_ceil(CACHE_LINE))
            .sum();
        total as f64 / cycle as f64
    }

    /// Sweeps every vector size up to 32 KiB and asserts the schedule invariants, most importantly that
    /// the near (T0/L1) window can never keep more line fills in flight than
    /// the MSHR budget — the bound is `near * lines_per_hint`, an over-count
    /// since fills complete while the window slides, so a pass here is
    /// conservative.
    #[test]
    fn window_schedule_stays_within_hardware_budgets() {
        for size in 1..=32 * 1024 {
            let (near, far) = prefetch_windows(size);
            let hint_bytes = size.min(NEAR_BYTES);

            assert!(
                (1..=8).contains(&near),
                "size {size}: near={near} outside [1, 8]"
            );
            if size < CACHE_LINE {
                assert_eq!(far, 0, "size {size}: sub-line code but far={far}");
            } else {
                assert!(
                    (near + 2..=16).contains(&far),
                    "size {size}: far={far} outside [near+2, 16]"
                );
            }

            assert!(
                near * hint_bytes <= NEAR_BYTES,
                "size {size}: near window holds {} bytes > {NEAR_BYTES}",
                near * hint_bytes,
            );
            assert!(
                far * hint_bytes <= FAR_BYTES,
                "size {size}: far window holds {} bytes > {FAR_BYTES}",
                far * hint_bytes,
            );

            let t0_lines = near as f64 * avg_lines_per_hint(size, hint_bytes);
            assert!(
                t0_lines <= MSHR_LINE_BUDGET,
                "size {size}: near window keeps {t0_lines:.1} lines in \
                 flight > budget {MSHR_LINE_BUDGET} (near={near})",
            );
        }
    }
}
