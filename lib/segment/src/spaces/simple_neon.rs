#[cfg(target_feature = "neon")]
use std::arch::aarch64::*;

#[cfg(target_feature = "neon")]
use common::types::ScoreType;

#[cfg(target_feature = "neon")]
use crate::data_types::vectors::VectorElementType;
use crate::data_types::vectors::VectorType;

#[cfg(target_feature = "neon")]
pub(crate) unsafe fn euclid_similarity_neon(
    v1: &[VectorElementType],
    v2: &[VectorElementType],
) -> ScoreType {
    let n = v1.len();
    let m = n - (n % 16);
    let mut ptr1: *const f32 = v1.as_ptr();
    let mut ptr2: *const f32 = v2.as_ptr();
    let mut sum1 = vdupq_n_f32(0.);
    let mut sum2 = vdupq_n_f32(0.);
    let mut sum3 = vdupq_n_f32(0.);
    let mut sum4 = vdupq_n_f32(0.);

    let mut i: usize = 0;
    while i < m {
        let sub1 = vsubq_f32(vld1q_f32(ptr1), vld1q_f32(ptr2));
        sum1 = vfmaq_f32(sum1, sub1, sub1);

        let sub2 = vsubq_f32(vld1q_f32(ptr1.add(4)), vld1q_f32(ptr2.add(4)));
        sum2 = vfmaq_f32(sum2, sub2, sub2);

        let sub3 = vsubq_f32(vld1q_f32(ptr1.add(8)), vld1q_f32(ptr2.add(8)));
        sum3 = vfmaq_f32(sum3, sub3, sub3);

        let sub4 = vsubq_f32(vld1q_f32(ptr1.add(12)), vld1q_f32(ptr2.add(12)));
        sum4 = vfmaq_f32(sum4, sub4, sub4);

        ptr1 = ptr1.add(16);
        ptr2 = ptr2.add(16);
        i += 16;
    }
    let mut result = vaddvq_f32(sum1) + vaddvq_f32(sum2) + vaddvq_f32(sum3) + vaddvq_f32(sum4);
    for i in 0..n - m {
        result += (*ptr1.add(i) - *ptr2.add(i)).powi(2);
    }
    -result
}

#[cfg(target_feature = "neon")]
pub(crate) unsafe fn manhattan_similarity_neon(
    v1: &[VectorElementType],
    v2: &[VectorElementType],
) -> ScoreType {
    let n = v1.len();
    let m = n - (n % 16);
    let mut ptr1: *const f32 = v1.as_ptr();
    let mut ptr2: *const f32 = v2.as_ptr();
    let mut sum1 = vdupq_n_f32(0.);
    let mut sum2 = vdupq_n_f32(0.);
    let mut sum3 = vdupq_n_f32(0.);
    let mut sum4 = vdupq_n_f32(0.);

    let mut i: usize = 0;
    while i < m {
        let sub1 = vsubq_f32(vld1q_f32(ptr1), vld1q_f32(ptr2));
        sum1 = vaddq_f32(sum1, vabsq_f32(sub1));

        let sub2 = vsubq_f32(vld1q_f32(ptr1.add(4)), vld1q_f32(ptr2.add(4)));
        sum2 = vaddq_f32(sum2, vabsq_f32(sub2));

        let sub3 = vsubq_f32(vld1q_f32(ptr1.add(8)), vld1q_f32(ptr2.add(8)));
        sum3 = vaddq_f32(sum3, vabsq_f32(sub3));

        let sub4 = vsubq_f32(vld1q_f32(ptr1.add(12)), vld1q_f32(ptr2.add(12)));
        sum4 = vaddq_f32(sum4, vabsq_f32(sub4));

        ptr1 = ptr1.add(16);
        ptr2 = ptr2.add(16);
        i += 16;
    }
    let mut result = vaddvq_f32(sum1) + vaddvq_f32(sum2) + vaddvq_f32(sum3) + vaddvq_f32(sum4);
    for i in 0..n - m {
        result += (*ptr1.add(i) - *ptr2.add(i)).abs();
    }
    -result
}

#[cfg(target_feature = "neon")]
pub(crate) unsafe fn cosine_preprocess_neon(vector: VectorType) -> VectorType {
    let n = vector.len();
    let m = n - (n % 16);
    let mut ptr: *const f32 = vector.as_ptr();
    let mut sum1 = vdupq_n_f32(0.);
    let mut sum2 = vdupq_n_f32(0.);
    let mut sum3 = vdupq_n_f32(0.);
    let mut sum4 = vdupq_n_f32(0.);

    let mut i: usize = 0;
    while i < m {
        let d1 = vld1q_f32(ptr);
        sum1 = vfmaq_f32(sum1, d1, d1);

        let d2 = vld1q_f32(ptr.add(4));
        sum2 = vfmaq_f32(sum2, d2, d2);

        let d3 = vld1q_f32(ptr.add(8));
        sum3 = vfmaq_f32(sum3, d3, d3);

        let d4 = vld1q_f32(ptr.add(12));
        sum4 = vfmaq_f32(sum4, d4, d4);

        ptr = ptr.add(16);
        i += 16;
    }
    let mut length = vaddvq_f32(sum1) + vaddvq_f32(sum2) + vaddvq_f32(sum3) + vaddvq_f32(sum4);
    for v in vector.iter().take(n).skip(m) {
        length += v.powi(2);
    }
    if length < f32::EPSILON {
        return vector;
    }
    let length = length.sqrt();
    vector.into_iter().map(|x| x / length).collect()
}

#[cfg(target_feature = "neon")]
pub(crate) unsafe fn dot_similarity_neon(
    v1: &[VectorElementType],
    v2: &[VectorElementType],
) -> ScoreType {
    let n = v1.len();
    let m = n - (n % 16);
    let mut ptr1: *const f32 = v1.as_ptr();
    let mut ptr2: *const f32 = v2.as_ptr();
    let mut sum1 = vdupq_n_f32(0.);
    let mut sum2 = vdupq_n_f32(0.);
    let mut sum3 = vdupq_n_f32(0.);
    let mut sum4 = vdupq_n_f32(0.);

    let mut i: usize = 0;
    while i < m {
        sum1 = vfmaq_f32(sum1, vld1q_f32(ptr1), vld1q_f32(ptr2));
        sum2 = vfmaq_f32(sum2, vld1q_f32(ptr1.add(4)), vld1q_f32(ptr2.add(4)));
        sum3 = vfmaq_f32(sum3, vld1q_f32(ptr1.add(8)), vld1q_f32(ptr2.add(8)));
        sum4 = vfmaq_f32(sum4, vld1q_f32(ptr1.add(12)), vld1q_f32(ptr2.add(12)));
        ptr1 = ptr1.add(16);
        ptr2 = ptr2.add(16);
        i += 16;
    }
    let mut result = vaddvq_f32(sum1) + vaddvq_f32(sum2) + vaddvq_f32(sum3) + vaddvq_f32(sum4);
    for i in 0..n - m {
        result += (*ptr1.add(i)) * (*ptr2.add(i));
    }
    result
}

#[cfg(test)]
mod tests {
    #[cfg(target_feature = "neon")]
    #[test]
    fn test_spaces_neon() {
        use super::*;
        use crate::spaces::simple::*;

        if std::arch::is_aarch64_feature_detected!("neon") {
            let v1: Vec<f32> = vec![
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                26., 27., 28., 29., 30., 31.,
            ];
            let v2: Vec<f32> = vec![
                40., 41., 42., 43., 44., 45., 46., 47., 48., 49., 50., 51., 52., 53., 54., 55.,
                56., 57., 58., 59., 60., 61.,
            ];

            let euclid_simd = unsafe { euclid_similarity_neon(&v1, &v2) };
            let euclid = euclid_similarity(&v1, &v2);
            assert_eq!(euclid_simd, euclid);

            let manhattan_simd = unsafe { manhattan_similarity_neon(&v1, &v2) };
            let manhattan = manhattan_similarity(&v1, &v2);
            assert_eq!(manhattan_simd, manhattan);

            let dot_simd = unsafe { dot_similarity_neon(&v1, &v2) };
            let dot = dot_similarity(&v1, &v2);
            assert_eq!(dot_simd, dot);

            let cosine_simd = unsafe { cosine_preprocess_neon(v1.clone()) };
            let cosine = cosine_preprocess(v1);
            assert_eq!(cosine_simd, cosine);
        } else {
            println!("neon test skipped");
        }
    }
}
