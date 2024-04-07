#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
pub mod sse_dot;

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
pub mod sse_cosine;

#[cfg(target_arch = "x86_64")]
pub mod sse_manhattan;

#[cfg(target_arch = "x86_64")]
pub mod sse_euclid;

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse")]
pub(crate) unsafe fn hsum128_ps_sse(x: __m128) -> f32 {
    let x64: __m128 = _mm_add_ps(x, _mm_movehl_ps(x, x));
    let x32: __m128 = _mm_add_ss(x64, _mm_shuffle_ps(x64, x64, 0x55));
    _mm_cvtss_f32(x32)
}
