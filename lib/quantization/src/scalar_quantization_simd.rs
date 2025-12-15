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

fn toy_simd_sum(a: &[i16; 2], b: &[i16; 2]) -> [i16; 2] {
    let mut result = [0i16; 2];
    for i in 0..2 {
        result[i] = a[i] + b[i];
    }
    result
}

fn toy_simd_horizontal_sum(a: &[i16; 2]) -> i32 {
    let mut result = 0i32;
    for i in 0..2 {
        result += a[i] as i32;
    }
    result
}

fn _mm256_maddubs_epi16(
    a: &[u8], // 4 values
    b: &[i8], // 4 values
) -> [i16; 2] {
    let mut result = [0i16; 2];
    for i in 0..2 {
        let a1 = a[i * 2] as i16;
        let a2 = a[i * 2 + 1] as i16;
        let b1 = b[i * 2] as i16;
        let b2 = b[i * 2 + 1] as i16;
        result[i] = a1 * b1 + a2 * b2;
    }
    result
}

// Input query with larger accuracy type:
#[allow(dead_code)]
type NotPermutedQuery = [[u8; 3]]; // u16 is not enough, use u24 instead
// while encoding as a the last step do this:
// let q: [i8; 4] = [q[0][0] as i8, q[1][0] as i8, q[2][0] as i8, q[3][0] as i8];
// Not, encoded vector should have have type:
pub(crate) type PermutedQuery = [[i8; 4]]; // it's a Vec<u16> with permuted bytes

// Step 0: prepare test. And compare with this function result:
fn original_score_dot_asymmetric(query: &NotPermutedQuery, vector: &[u8]) -> i32 {
    let mut sum = 0i32;
    for (q, v) in query.iter().zip(vector.chunks(4)) {
        sum += i32::from(q[0]) * i32::from(v[0]);
        sum += (i32::from(q[1]) * i32::from(v[1])) << 8;
        sum += (i32::from(q[2]) * i32::from(v[2])) << 16;
    }
    sum
}

// Step 1: this function is a simple CPU case without SIMD.
// Just refactor it (or not)
pub(crate) fn impl_score_dot_asymmetric(query: &PermutedQuery, vector: &[u8]) -> i32 {
    let mut sum1: [i16; 2] = Default::default();
    let mut sum2: [i16; 2] = Default::default();
    let mut sum3: [i16; 2] = Default::default();
    for (q, v) in query.chunks_exact(2).zip(vector.chunks(4)) {
        sum1 = toy_simd_sum(&sum1, &_mm256_maddubs_epi16(v, &q[0]));
        sum2 = toy_simd_sum(&sum2, &_mm256_maddubs_epi16(v, &q[1]));
        sum3 = toy_simd_sum(&sum3, &_mm256_maddubs_epi16(v, &q[2]));
    }
    let sum1 = toy_simd_horizontal_sum(&sum1);
    let sum2 = toy_simd_horizontal_sum(&sum2);
    let sum3 = toy_simd_horizontal_sum(&sum3);
    let sum = sum1 + (sum2 << 8) + (sum3 << 16);
    sum
}

// Step 2: implementation for AVX2:
pub(crate) fn impl_score_dot_asymmetric_avx(query: &PermutedQuery, vector: &[u8]) -> i32 {
    todo!()
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
