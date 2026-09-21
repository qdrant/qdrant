//! Dot product on decoded originals, with the prototype's bf16 SIMD reduction order.
//! Storage decoding belongs to the adapter; inputs here may be arbitrary finite f32.

use crate::kernel::{path, Path};

/// Best-effort hint for just one selected original row, never the whole mapping.
/// The borrowed bytes stay alive throughout the syscall and CPU prefetch hints.
pub fn prefetch(bytes: &[u8]) {
    if bytes.is_empty() {
        return;
    }
    #[cfg(unix)]
    unsafe {
        static PAGE_SIZE: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
        let page_size = *PAGE_SIZE.get_or_init(|| {
            let size = libc::sysconf(libc::_SC_PAGESIZE);
            if size > 0 {
                size as usize
            } else {
                4096
            }
        });
        let address = bytes.as_ptr() as usize;
        let aligned = address / page_size * page_size;
        // WILLNEED does not alter the mapping or memory contents. Failure only
        // loses a performance hint; normal demand reads still fetch the row.
        let _ = libc::madvise(
            aligned as *mut libc::c_void,
            bytes.len() + address - aligned,
            libc::MADV_WILLNEED,
        );
    }
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
        for offset in (0..bytes.len()).step_by(64) {
            _mm_prefetch(bytes.as_ptr().add(offset).cast(), _MM_HINT_T0);
        }
    }
}

pub fn dot(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len());
    #[cfg(target_arch = "x86_64")]
    unsafe {
        match path() {
            Path::Avx512 => return dot_avx512(a, b),
            Path::Avx2 => return dot_avx2(a, b),
            Path::Scalar => {}
        }
    }
    scalar(a, b)
}

fn scalar(a: &[f32], b: &[f32]) -> f32 {
    let mut acc = [0.0f32; 8];
    let end = a.len() / 8 * 8;
    for i in (0..end).step_by(8) {
        for lane in 0..8 {
            acc[lane] += a[i + lane] * b[i + lane];
        }
    }
    let mut sum = ((acc[0] + acc[1]) + (acc[2] + acc[3])) + ((acc[4] + acc[5]) + (acc[6] + acc[7]));
    for i in end..a.len() {
        sum += a[i] * b[i];
    }
    sum
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn dot_avx512(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::x86_64::*;
    let (mut acc0, mut acc1) = (_mm512_setzero_ps(), _mm512_setzero_ps());
    let mut i = 0;
    while i + 32 <= a.len() {
        acc0 = _mm512_fmadd_ps(
            _mm512_loadu_ps(a.as_ptr().add(i)),
            _mm512_loadu_ps(b.as_ptr().add(i)),
            acc0,
        );
        acc1 = _mm512_fmadd_ps(
            _mm512_loadu_ps(a.as_ptr().add(i + 16)),
            _mm512_loadu_ps(b.as_ptr().add(i + 16)),
            acc1,
        );
        i += 32;
    }
    while i + 16 <= a.len() {
        acc0 = _mm512_fmadd_ps(
            _mm512_loadu_ps(a.as_ptr().add(i)),
            _mm512_loadu_ps(b.as_ptr().add(i)),
            acc0,
        );
        i += 16;
    }
    let mut sum = _mm512_reduce_add_ps(_mm512_add_ps(acc0, acc1));
    for j in i..a.len() {
        sum += a[j] * b[j];
    }
    sum
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
unsafe fn dot_avx2(a: &[f32], b: &[f32]) -> f32 {
    use std::arch::x86_64::*;
    let mut acc = _mm256_setzero_ps();
    let mut i = 0;
    while i + 8 <= a.len() {
        acc = _mm256_fmadd_ps(
            _mm256_loadu_ps(a.as_ptr().add(i)),
            _mm256_loadu_ps(b.as_ptr().add(i)),
            acc,
        );
        i += 8;
    }
    let hi = _mm256_extractf128_ps(acc, 1);
    let lo = _mm256_castps256_ps128(acc);
    let sum4 = _mm_add_ps(lo, hi);
    let sum4 = _mm_hadd_ps(sum4, sum4);
    let sum4 = _mm_hadd_ps(sum4, sum4);
    let mut sum = _mm_cvtss_f32(sum4);
    for j in i..a.len() {
        sum += a[j] * b[j];
    }
    sum
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn originals_dot_handles_vector_blocks_and_tails() {
        for n in [0, 1, 7, 8, 15, 16, 31, 32, 127, 128, 255, 256, 259] {
            let a: Vec<_> = (0..n).map(|i| (i as f32 % 17.0 - 8.0) / 8.0).collect();
            let b: Vec<_> = (0..n).map(|i| (i as f32 % 11.0 - 5.0) / 16.0).collect();
            // These binary fractions sum exactly in f32, independent of reduction order.
            let expected = a
                .iter()
                .zip(&b)
                .map(|(&a, &b)| a as f64 * b as f64)
                .sum::<f64>() as f32;
            assert_eq!(scalar(&a, &b), expected);
            assert_eq!(dot(&a, &b), expected);
            #[cfg(target_arch = "x86_64")]
            unsafe {
                if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
                    assert_eq!(dot_avx2(&a, &b), expected);
                }
                if is_x86_feature_detected!("avx512f") {
                    assert_eq!(dot_avx512(&a, &b), expected);
                }
            }
        }
    }
}
