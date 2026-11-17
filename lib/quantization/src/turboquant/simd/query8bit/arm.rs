//! aarch64 kernels for the symmetric 8-bit path (`score_8bit_internal`).
//!
//! `u ^ 0x80` turns both stored codes into signed bytes, which `SDOT` (or
//! `vmull_s8 → vpadalq_s16` without `dotprod`) multiplies exactly:
//! `|s_a · s_b| ≤ 128²` fits i16, a pairwise add fits i32.

use core::arch::aarch64::*;

use super::score_8bit_internal_integer;

const BLOCK: usize = 16;

#[inline]
#[target_feature(enable = "neon")]
unsafe fn load_signed(ptr: *const u8) -> int8x16_t {
    unsafe { vreinterpretq_s8_u8(veorq_u8(vld1q_u8(ptr), vdupq_n_u8(0x80))) }
}

/// `acc[lane] += Σ₄ a · b` (`SDOT`); inline asm because `vdotq_s32` is
/// still unstable (rust-lang/rust#117224).
#[inline]
#[target_feature(enable = "neon,dotprod")]
unsafe fn sdot(mut acc: int32x4_t, a: int8x16_t, b: int8x16_t) -> int32x4_t {
    unsafe {
        core::arch::asm!(
            "sdot {acc:v}.4s, {a:v}.16b, {b:v}.16b",
            acc = inout(vreg) acc,
            a = in(vreg) a,
            b = in(vreg) b,
            options(pure, nomem, nostack, preserves_flags),
        );
    }
    acc
}

/// NEON implementation of [`super::score_8bit_internal`] for CPUs without
/// `dotprod`.
///
/// # Safety
/// CPU must support `neon`; `a.len() == b.len()`.
#[target_feature(enable = "neon")]
pub unsafe fn score_8bit_internal_neon(a: &[u8], b: &[u8]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    unsafe {
        let mut acc = [vdupq_n_s32(0); 2];
        let blocks = a.len() / BLOCK;
        for i in 0..blocks {
            let sa = load_signed(a.as_ptr().add(i * BLOCK));
            let sb = load_signed(b.as_ptr().add(i * BLOCK));
            acc[0] = vpadalq_s16(acc[0], vmull_s8(vget_low_s8(sa), vget_low_s8(sb)));
            acc[1] = vpadalq_s16(acc[1], vmull_high_s8(sa, sb));
        }
        let simd = i64::from(vaddvq_s32(vaddq_s32(acc[0], acc[1])));
        let tail = blocks * BLOCK;
        (simd + score_8bit_internal_integer(&a[tail..], &b[tail..])) as f32
    }
}

/// NEON + `SDOT` implementation of [`super::score_8bit_internal`].
///
/// # Safety
/// CPU must support `neon` and `dotprod`; `a.len() == b.len()`.
#[target_feature(enable = "neon,dotprod")]
pub unsafe fn score_8bit_internal_neon_sdot(a: &[u8], b: &[u8]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    unsafe {
        let mut acc = [vdupq_n_s32(0); 2];
        let blocks = a.len() / BLOCK;
        for i in 0..blocks {
            let sa = load_signed(a.as_ptr().add(i * BLOCK));
            let sb = load_signed(b.as_ptr().add(i * BLOCK));
            acc[i & 1] = sdot(acc[i & 1], sa, sb);
        }
        let simd = i64::from(vaddvq_s32(vaddq_s32(acc[0], acc[1])));
        let tail = blocks * BLOCK;
        (simd + score_8bit_internal_integer(&a[tail..], &b[tail..])) as f32
    }
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng;
    use rand::prelude::StdRng;

    use super::*;
    use crate::turboquant::simd::shared::random_bytes;

    #[test]
    fn test_kernels_match_scalar() {
        let mut rng = StdRng::seed_from_u64(5);
        let has_dotprod = std::arch::is_aarch64_feature_detected!("dotprod");
        let dims = (0..=130).chain([255, 256, 257, 384, 1000, 1536, 4096, 65_536]);
        for dim in dims {
            let a = random_bytes(&mut rng, dim);
            let b = random_bytes(&mut rng, dim);
            let expected = score_8bit_internal_integer(&a, &b) as f32;
            unsafe {
                assert_eq!(score_8bit_internal_neon(&a, &b), expected, "neon dim={dim}");
                if has_dotprod {
                    let sdot = score_8bit_internal_neon_sdot(&a, &b);
                    assert_eq!(sdot, expected, "sdot dim={dim}");
                }
            }
        }
    }
}
