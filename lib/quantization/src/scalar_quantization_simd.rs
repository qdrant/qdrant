/*
SQ SIMD is based on
_mm256_maddubs_epi16

Which takes:
[x1, x2, ..., x32] : [u8; 32]
[y1, y2, ..., y32] : [i8; 32]

And produces:
[x1 * y1 + x2 * y2, x3 * y3 + x4 * y4, ..., x31 * y31 + x32 * y32] : [i16; 16], where

It's a partial dot product of 2 vectors, where one vector is u8 and another is i8.

Because of `score_internal` function in SQ we use only 7 bits of i8, so it doesnt matter if it's signed or not.
*/

use std::arch::x86_64::{
    __m256i, _mm_add_epi16, _mm_extract_epi16, _mm_hadd_epi16, _mm256_add_epi8,
    _mm256_castsi256_si128, _mm256_extracti128_si256, _mm256_loadu_si256, _mm256_maddubs_epi16,
    _mm256_setzero_si256,
};

#[target_feature(enable = "avx")]
#[target_feature(enable = "avx2")]
#[target_feature(enable = "sse2")]
#[target_feature(enable = "ssse3")]
unsafe fn _mm256_hsum_epi16(r: __m256i) -> i32 {
    let x128 = _mm_add_epi16(_mm256_castsi256_si128(r), _mm256_extracti128_si256::<1>(r));
    let x64 = _mm_hadd_epi16(x128, x128);
    let x32 = _mm_hadd_epi16(x64, x64);
    let x16 = _mm_hadd_epi16(x32, x32);
    _mm_extract_epi16::<0>(x16)
}

fn permutate_query(query: &NotPermutedQuery) -> Vec<[i8; 32]> {
    let mut out = vec![];

    for chunk in query.chunks(32) {
        let mut buffs = [[0i8; 32]; 3];

        for (idx, dim) in chunk.iter().enumerate() {
            buffs[0][idx] = dim[0] as i8;
            buffs[1][idx] = dim[1] as i8;
            buffs[2][idx] = dim[2] as i8;
        }

        out.extend(buffs);
    }

    out
}

// Input query with larger accuracy type:
type NotPermutedQuery = [[u8; 3]]; // u16 is not enough, use u24 instead

// while encoding as a the last step do this:
// let q: [i8; 4] = [q[0][0] as i8, q[1][0] as i8, q[2][0] as i8, q[3][0] as i8];
// Not, encoded vector should have have type:
pub(crate) type PermutedQuery = [[i8; 32]];

// Step 0: prepare test. And compare with this function result:
fn original_score_dot_asymmetric(query: &NotPermutedQuery, vector: &[u8]) -> i32 {
    let mut sum = 0i32;
    for (q, v) in query.chunks(3).zip(vector.chunks(3)) {
        sum += i32::from(q[0][0]) * i32::from(v[0]);
        sum += (i32::from(q[0][1]) * i32::from(v[0])) << 8;
        sum += (i32::from(q[0][2]) * i32::from(v[0])) << 16;

        sum += i32::from(q[1][0]) * i32::from(v[1]);
        sum += (i32::from(q[1][1]) * i32::from(v[1])) << 8;
        sum += (i32::from(q[1][2]) * i32::from(v[1])) << 16;

        sum += i32::from(q[2][0]) * i32::from(v[2]);
        sum += (i32::from(q[2][1]) * i32::from(v[2])) << 8;
        sum += (i32::from(q[2][2]) * i32::from(v[2])) << 16;
    }
    sum
}

// Asymmetric dot product wihtout SIMD.
pub(crate) fn impl_score_dot_asymmetric(query: &PermutedQuery, vector: &[u8]) -> i32 {
    let mut sum1 = 0i32;
    let mut sum2 = 0i32;
    let mut sum3 = 0i32;

    for (q, v) in query.chunks_exact(3).zip(vector.chunks_exact(32)) {
        for chunk_pos in 0..32 {
            sum1 += i32::from(v[chunk_pos]) * i32::from(q[0][chunk_pos]);
            sum2 += i32::from(v[chunk_pos]) * i32::from(q[1][chunk_pos]);
            sum3 += i32::from(v[chunk_pos]) * i32::from(q[2][chunk_pos]);
        }
    }

    sum1 + (sum2 << 8) + (sum3 << 16)
}

// Asymmetric dot product with AVX2.
#[target_feature(enable = "avx")]
#[target_feature(enable = "avx2")]
pub fn impl_score_dot_asymmetric_avx(query: &PermutedQuery, vector: &[u8]) -> i32 {
    unsafe {
        let mut sum1 = _mm256_setzero_si256();
        let mut sum2 = _mm256_setzero_si256();
        let mut sum3 = _mm256_setzero_si256();

        for (q, v) in query.chunks_exact(3).zip(vector.chunks_exact(32)) {
            let v_reg = _mm256_loadu_si256(v.as_ptr().cast());

            let q0 = _mm256_loadu_si256(q[0].as_ptr().cast());
            sum1 = _mm256_add_epi8(sum1, _mm256_maddubs_epi16(q0, v_reg));

            let q1 = _mm256_loadu_si256(q[1].as_ptr().cast());
            sum2 = _mm256_add_epi8(sum2, _mm256_maddubs_epi16(q1, v_reg));

            let q2 = _mm256_loadu_si256(q[2].as_ptr().cast());
            sum3 = _mm256_add_epi8(sum3, _mm256_maddubs_epi16(q2, v_reg));
        }

        let hsum1 = _mm256_hsum_epi16(sum1);
        let hsum2 = _mm256_hsum_epi16(sum2) << 8;
        let hsum3 = _mm256_hsum_epi16(sum3) << 16;

        hsum1 + hsum2 + hsum3
    }
}

// Step 3: implementation for SSE
pub(crate) fn impl_score_dot_asymmetric_sse(query: &PermutedQuery, vector: &[u8]) -> i32 {
    todo!()
}

// Step 4: implementation for NEON
pub(crate) fn impl_score_dot_asymmetric_neon(query: &PermutedQuery, vector: &[u8]) -> i32 {
    // Differencies with x64:
    // 1. NEON has only 128 bit registers, no 256
    // 2. No direct equivalent of _mm256_maddubs_epi16, see vmull_u8 and vpadalq_u16.
    // all hints you can find in `impl_score_dot_neon`
    // 3. There are types of registors: uint8x16_t, int8x16_t, uint16x8_t, int16x8_t, uint32x4_t, int32x4_t
    // in x64 we have only one types __m256i/__m256 which can be used as any of them.
    todo!()
}

pub(crate) fn impl_score_dot(q_ptr: *const u8, v_ptr: *const u8, actual_dim: usize) -> i32 {
    unsafe {
        let mut score = 0i32;
        for i in 0..actual_dim {
            score += i32::from(*q_ptr.add(i)) * i32::from(*v_ptr.add(i));
        }
        score
    }
}

pub(crate) fn impl_score_l1(q_ptr: *const u8, v_ptr: *const u8, actual_dim: usize) -> i32 {
    unsafe {
        let mut score = 0i32;
        for i in 0..actual_dim {
            score += i32::from(*q_ptr.add(i)).abs_diff(i32::from(*v_ptr.add(i))) as i32;
        }
        score
    }
}

#[cfg(target_arch = "x86_64")]
unsafe extern "C" {
    pub(crate) fn impl_score_dot_avx(query_ptr: *const u8, vector_ptr: *const u8, dim: u32) -> f32;
    pub(crate) fn impl_score_l1_avx(query_ptr: *const u8, vector_ptr: *const u8, dim: u32) -> f32;

    pub(crate) fn impl_score_dot_sse(query_ptr: *const u8, vector_ptr: *const u8, dim: u32) -> f32;
    pub(crate) fn impl_score_l1_sse(query_ptr: *const u8, vector_ptr: *const u8, dim: u32) -> f32;
}

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
unsafe extern "C" {
    pub(crate) fn impl_score_dot_neon(query_ptr: *const u8, vector_ptr: *const u8, dim: u32)
    -> f32;
    pub(crate) fn impl_score_l1_neon(query_ptr: *const u8, vector_ptr: *const u8, dim: u32) -> f32;
}

#[cfg(test)]
mod test {
    use std::arch::x86_64::{_mm256_set1_epi16, _mm256_storeu_si256};

    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::*;

    #[test]
    fn test_naive() {
        let query: &[u8] = &[0, 1, 6, 12, 73, 10];

        let query_vec: Vec<_> = query
            .iter()
            .copied()
            .map(|i| u32_to_u24(i as u32))
            .collect();

        let other_vec: Vec<u8> = vec![1, 2, 5, 15, 70, 99];

        assert_eq!(
            original_score_dot_asymmetric(&query_vec, &other_vec) as u32,
            original_score_dot(&query, &other_vec) as u32
        );
    }

    #[test]
    fn test_cpu_version() {
        let query: Vec<u8> = (0..32).into_iter().collect();

        let query_vec: Vec<_> = query
            .iter()
            .copied()
            .map(|i| u32_to_u24(i as u32))
            .collect();
        let query_permuted = permutate_query(&query_vec);

        let mut rng = StdRng::seed_from_u64(42);
        let other_vec: Vec<u8> = query
            .iter()
            .map(|i| {
                let rand_add = rng.random_range(0..10) as u8;
                *i + rand_add
            })
            .collect();

        let refactor_res = impl_score_dot_asymmetric(&query_permuted, &other_vec);
        let original_res = original_score_dot(&query, &other_vec);
        assert_eq!(refactor_res, original_res as i32);

        let avx2_res = unsafe { impl_score_dot_asymmetric_avx(&query_permuted, &other_vec) };
        assert_eq!(avx2_res, original_res as i32);
    }

    #[test]
    fn test_check_hsum_epi16() {
        unsafe { check_hsum_epi16() }
    }

    #[target_feature(enable = "avx2")]
    #[target_feature(enable = "sse")]
    unsafe fn check_hsum_epi16() {
        let reg = _mm256_set1_epi16(2);
        let mut stored = [0i16; 16];
        unsafe {
            _mm256_storeu_si256(stored.as_mut_ptr().cast(), reg);
            assert_eq!(stored, [2i16; 16]);
            assert_eq!(_mm256_hsum_epi16(reg), 32);
        }
    }

    #[test]
    fn test_convert() {
        let v = 500;
        let i = 5u8;

        let u24 = u32_to_u24(v as u32);
        let mut o = 0;
        o += (i as i32) * (u24[0] as i32);
        o += (i as i32) * (u24[1] as i32) << 8;
        o += (i as i32) * (u24[2] as i32) << 16;

        assert_eq!(v * i as i32, o);
    }

    fn u32_to_u24(n: u32) -> [u8; 3] {
        n.to_le_bytes()[0..3].try_into().unwrap()
    }

    fn original_score_dot(query: &[u8], vector: &[u8]) -> u32 {
        query
            .iter()
            .zip(vector.iter())
            .map(|(q, v)| *q as u32 * *v as u32)
            .sum()
    }
}
