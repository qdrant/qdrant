#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

#[cfg(target_arch = "x86_64")]
pub mod avx_dot;

#[cfg(target_arch = "x86_64")]
pub mod avx_cosine;

#[cfg(target_arch = "x86_64")]
pub mod avx_manhattan;

#[cfg(target_arch = "x86_64")]
pub mod avx_euclid;

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx")]
#[target_feature(enable = "fma")]
pub(crate) unsafe fn hsum256_ps_avx(x: __m256) -> f32 {
    let x128: __m128 = _mm_add_ps(_mm256_extractf128_ps(x, 1), _mm256_castps256_ps128(x));
    let x64: __m128 = _mm_add_ps(x128, _mm_movehl_ps(x128, x128));
    let x32: __m128 = _mm_add_ss(x64, _mm_shuffle_ps(x64, x64, 0x55));
    _mm_cvtss_f32(x32)
}
