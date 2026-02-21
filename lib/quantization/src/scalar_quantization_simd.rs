use std::arch::x86_64::{
    __m128i, __m256i, _mm_add_epi16, _mm_extract_epi16, _mm_hadd_epi16, _mm_loadu_si128,
    _mm_maddubs_epi16, _mm_setzero_si128, _mm256_add_epi16, _mm256_castsi256_si128,
    _mm256_extracti128_si256, _mm256_loadu_si256, _mm256_maddubs_epi16, _mm256_setzero_si256,
};

/// Horizontal sum of 16x 16 bit integers.
#[target_feature(enable = "avx")]
#[target_feature(enable = "avx2")]
#[target_feature(enable = "sse2")]
#[target_feature(enable = "ssse3")]
unsafe fn _mm256_hsum_epi16(r: __m256i) -> i64 {
    let x128 = _mm_add_epi16(_mm256_castsi256_si128(r), _mm256_extracti128_si256::<1>(r));
    let x64 = _mm_hadd_epi16(x128, x128);
    // We extract the first two values rather than calling `_mm_hadd_epi16` again to avoid potential i16 overflow.
    _mm_extract_epi16::<0>(x64) as i64
        + _mm_extract_epi16::<1>(x64) as i64
        + _mm_extract_epi16::<2>(x64) as i64
        + _mm_extract_epi16::<3>(x64) as i64
}

/// Horizontal sum of 8x 16 bit integers.
#[target_feature(enable = "sse2")]
#[target_feature(enable = "ssse3")]
unsafe fn _mm_hsum_epi16(r: __m128i) -> i64 {
    let x64 = _mm_hadd_epi16(r, r);
    let x32 = _mm_hadd_epi16(x64, x64);

    // We extract the first two values rather than calling `_mm_hadd_epi16` again to avoid potential i16 overflow.
    _mm_extract_epi16::<0>(x32) as i64 + _mm_extract_epi16::<1>(x32) as i64
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
fn original_score_dot_asymmetric(query: &NotPermutedQuery, vector: &[u8]) -> i64 {
    let mut sum = 0i64;
    for (q, v) in query.chunks(3).zip(vector.chunks(3)) {
        sum += i64::from(q[0][0]) * i64::from(v[0]);
        sum += (i64::from(q[0][1]) * i64::from(v[0])) << 8;
        sum += (i64::from(q[0][2]) * i64::from(v[0])) << 16;

        sum += i64::from(q[1][0]) * i64::from(v[1]);
        sum += (i64::from(q[1][1]) * i64::from(v[1])) << 8;
        sum += (i64::from(q[1][2]) * i64::from(v[1])) << 16;

        sum += i64::from(q[2][0]) * i64::from(v[2]);
        sum += (i64::from(q[2][1]) * i64::from(v[2])) << 8;
        sum += (i64::from(q[2][2]) * i64::from(v[2])) << 16;
    }
    sum
}

// Asymmetric dot product wihtout SIMD.
pub(crate) fn impl_score_dot_asymmetric(query: &PermutedQuery, vector: &[u8]) -> i64 {
    let mut sum1 = 0i64;
    let mut sum2 = 0i64;
    let mut sum3 = 0i64;

    for (q, v) in query.chunks_exact(3).zip(vector.chunks_exact(32)) {
        for chunk_pos in 0..32 {
            let d1 = i64::from(v[chunk_pos]) * i64::from(q[0][chunk_pos]);
            sum1 += d1;
            let d2 = i64::from(v[chunk_pos]) * i64::from(q[1][chunk_pos]);
            sum2 += d2;
            let d3 = i64::from(v[chunk_pos]) * i64::from(q[2][chunk_pos]);
            sum3 += d3;
        }
    }

    sum1 + (sum2 << 8) + (sum3 << 16)
}

// Asymmetric dot product with AVX2.
#[target_feature(enable = "avx")]
#[target_feature(enable = "avx2")]
pub fn impl_score_dot_asymmetric_avx(query: &PermutedQuery, vector: &[u8]) -> i64 {
    unsafe {
        let mut sum0 = _mm256_setzero_si256();
        let mut sum1 = _mm256_setzero_si256();
        let mut sum2 = _mm256_setzero_si256();

        for (q, v) in query.chunks_exact(3).zip(vector.chunks_exact(32)) {
            let v_reg = _mm256_loadu_si256(v.as_ptr().cast());
            let q_reg: *const __m256i = q.as_ptr().cast();

            let q0 = _mm256_loadu_si256(q_reg);
            sum0 = _mm256_add_epi16(sum0, _mm256_maddubs_epi16(q0, v_reg));

            let q1 = _mm256_loadu_si256(q_reg.add(1));
            sum1 = _mm256_add_epi16(sum1, _mm256_maddubs_epi16(q1, v_reg));

            let q2 = _mm256_loadu_si256(q_reg.add(2));
            sum2 = _mm256_add_epi16(sum2, _mm256_maddubs_epi16(q2, v_reg));
        }

        let hsum1 = _mm256_hsum_epi16(sum0);
        let hsum2 = _mm256_hsum_epi16(sum1) << 8;
        let hsum3 = _mm256_hsum_epi16(sum2) << 16;

        println!("{hsum1} {hsum2} {hsum3}");

        hsum1 + hsum2 + hsum3
    }
}

// Step 3: implementation for SSE
#[target_feature(enable = "sse2")]
#[target_feature(enable = "ssse3")]
pub fn impl_score_dot_asymmetric_sse(query: &PermutedQuery, vector: &[u8]) -> i64 {
    unsafe {
        let mut sum1 = _mm_setzero_si128();
        let mut sum2 = _mm_setzero_si128();
        let mut sum3 = _mm_setzero_si128();
        let mut sum4 = _mm_setzero_si128();
        let mut sum5 = _mm_setzero_si128();
        let mut sum6 = _mm_setzero_si128();

        for (q, v) in query.chunks_exact(3).zip(vector.chunks_exact(32)) {
            let v_ptr: *const __m128i = v.as_ptr().cast();
            let v0 = _mm_loadu_si128(v_ptr);
            let v1 = _mm_loadu_si128(v_ptr.add(1));

            // Part 1
            let q0_ptr: *const __m128i = q[0].as_ptr().cast();

            let q0_0 = _mm_loadu_si128(q0_ptr);
            sum1 = _mm_add_epi16(sum1, _mm_maddubs_epi16(q0_0, v0));

            let q0_1 = _mm_loadu_si128(q0_ptr.add(1));
            sum2 = _mm_add_epi16(sum2, _mm_maddubs_epi16(q0_1, v1));

            // Part 2
            let q1_ptr: *const __m128i = q[1].as_ptr().cast();
            let q1_0 = _mm_loadu_si128(q1_ptr);
            sum3 = _mm_add_epi16(sum3, _mm_maddubs_epi16(q1_0, v0));

            let q1_1 = _mm_loadu_si128(q1_ptr.add(1));
            sum4 = _mm_add_epi16(sum4, _mm_maddubs_epi16(q1_1, v1));

            // Part 3
            let q2_ptr: *const __m128i = q[2].as_ptr().cast();
            let q2_0 = _mm_loadu_si128(q2_ptr);
            sum5 = _mm_add_epi16(sum5, _mm_maddubs_epi16(q2_0, v0));

            let q2_1 = _mm_loadu_si128(q2_ptr.add(1));
            sum6 = _mm_add_epi16(sum6, _mm_maddubs_epi16(q2_1, v1));
        }

        let hsum1 = _mm_hsum_epi16(sum1);
        let hsum2 = _mm_hsum_epi16(sum2);
        let hsum3 = _mm_hsum_epi16(sum3);
        let hsum4 = _mm_hsum_epi16(sum4);
        let hsum5 = _mm_hsum_epi16(sum5);
        let hsum6 = _mm_hsum_epi16(sum6);

        let sum1 = hsum1 + hsum2;
        let sum2 = (hsum3 + hsum4) << 8;
        let sum3 = (hsum5 + hsum6) << 16;

        sum1 + sum2 + sum3
    }
}

// Step 4: implementation for NEON
// <Implemented in CPP code!>
//
// pub(crate) fn impl_score_dot_asymmetric_neon(query: &PermutedQuery, vector: &[u8]) -> i32 {
// Differencies with x64:
// 1. NEON has only 128 bit registers, no 256
// 2. No direct equivalent of _mm256_maddubs_epi16, see vmull_u8 and vpadalq_u16.
// all hints you can find in `impl_score_dot_neon`
// 3. There are types of registors: uint8x16_t, int8x16_t, uint16x8_t, int16x8_t, uint32x4_t, int32x4_t
// in x64 we have only one types __m256i/__m256 which can be used as any of them.
// }

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

    pub(crate) fn impl_score_dot_asymmetric_neon(
        query_ptr: *const u8,
        vector_ptr: *const u8,
        dim: u32,
    ) -> i64;
}

#[cfg(test)]
mod test {
    use rand::rngs::StdRng;
    use rand::{RngCore, SeedableRng};

    use super::*;

    #[test]
    fn test_naive() {
        let query: &[u32] = &[0, 1, 6, 12, 73, 10];

        let query_vec: Vec<_> = query
            .iter()
            .copied()
            .map(|i| u32_to_u24(i as u32))
            .collect();

        let other_vec: Vec<u8> = vec![1, 2, 5, 15, 70, 99];

        assert_eq!(
            original_score_dot_asymmetric(&query_vec, &other_vec) as u32,
            original_asymmetric_dot(&query, &other_vec) as u32
        );
    }

    #[test]
    fn test_all() {
        let mut rng = StdRng::seed_from_u64(42);

        let query: Vec<u32> = (0..32)
            .into_iter()
            .map(|_| {
                let rand = (rng.next_u32() % (2u32.pow(24))).to_le_bytes();
                u32::from_le_bytes([rand[0] % 128, rand[1] % 128, rand[3] % 128, 0])
            })
            .collect();

        let query_vec: Vec<_> = query
            .iter()
            .copied()
            .map(|i| u32_to_u24(i as u32))
            .collect();
        let query_permuted = permutate_query(&query_vec);

        let other_vec: Vec<u8> = query
            .iter()
            .map(|i| (*i) % 127)
            .map(|i| i8::try_from(i).unwrap() as u8)
            .collect();

        let refactor_res = impl_score_dot_asymmetric(&query_permuted, &other_vec);
        let original_res = original_asymmetric_dot(&query, &other_vec);
        assert_eq!(refactor_res, original_res as i64);

        let avx2_res = unsafe { impl_score_dot_asymmetric_avx(&query_permuted, &other_vec) };
        assert_eq!(avx2_res, original_res as i64);

        let sse_res = unsafe { impl_score_dot_asymmetric_sse(&query_permuted, &other_vec) };
        assert_eq!(sse_res, original_res as i64);
    }

    #[test]
    fn test_max_query_for_overflow() {
        let max_val = i32::from_le_bytes([64, 64, 64, 0]) as u32;
        const LEN: usize = 32;

        let query_data: Vec<_> = (0..LEN).map(|_| max_val).collect();
        let query_vec: Vec<_> = query_data.iter().map(|i| u32_to_u24(*i)).collect();
        let query_permuted = permutate_query(&query_vec);

        let other_vec = [64u8; LEN];

        let refactor_res = impl_score_dot_asymmetric(&query_permuted, &other_vec);
        let original_res = original_asymmetric_dot(&query_data, &other_vec);
        assert_eq!(refactor_res, original_res);
        let avx2_res = unsafe { impl_score_dot_asymmetric_avx(&query_permuted, &other_vec) };
        assert_eq!(avx2_res, original_res);

        let sse_res = unsafe { impl_score_dot_asymmetric_sse(&query_permuted, &other_vec) };
        assert_eq!(sse_res, original_res);
    }

    #[test]
    fn test_check_hsums() {
        unsafe { check_hsums() }
    }

    #[target_feature(enable = "avx2")]
    #[target_feature(enable = "sse")]
    unsafe fn check_hsums() {
        unsafe {
            let data: Vec<i16> = (0..16).collect();
            let reg = _mm256_loadu_si256(data.as_ptr().cast());
            assert_eq!(
                _mm256_hsum_epi16(reg),
                data.iter().map(|i| *i as i64).sum::<i64>()
            );

            let reg_sse = _mm_loadu_si128(data.as_ptr().cast());
            assert_eq!(
                _mm_hsum_epi16(reg_sse),
                data[..8].iter().map(|i| *i as i64).sum::<i64>()
            );
        }
    }

    fn u32_to_u24(n: u32) -> [u8; 3] {
        let lebytes = n.to_le_bytes();
        assert_eq!(lebytes[3], 0);
        lebytes[0..3].try_into().unwrap()
    }

    fn original_asymmetric_dot(query: &[u32], vector: &[u8]) -> i64 {
        query
            .iter()
            .zip(vector)
            .map(|(q, v)| *q as i64 * *v as i64)
            .sum()
    }
}
