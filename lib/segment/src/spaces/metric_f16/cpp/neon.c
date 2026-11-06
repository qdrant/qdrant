#if !defined PC_VER
#include <arm_neon.h>
#endif

#ifdef __ARM_FEATURE_FP16_VECTOR_ARITHMETIC
#include <arm_fp16.h>
float32_t dotProduct_half_4x4(const float16_t* pSrcA, const float16_t* pSrcB, uint32_t blockSize)
{
    float32_t dotProduct = 0.0f;
    // Accumulate in f32, not f16: an f16 lane sum saturates to +inf once it
    // exceeds 65504, while the scalar and AVX paths accumulate in f32
    // (see PR #9811). Keeping the four float16x8_t accumulators here makes
    // NEON disagree with those paths and can serialize a valid score as null.
    float32x4_t sum1_lo = vdupq_n_f32(0.0f);
    float32x4_t sum1_hi = vdupq_n_f32(0.0f);
    float32x4_t sum2_lo = vdupq_n_f32(0.0f);
    float32x4_t sum2_hi = vdupq_n_f32(0.0f);
    float32x4_t sum3_lo = vdupq_n_f32(0.0f);
    float32x4_t sum3_hi = vdupq_n_f32(0.0f);
    float32x4_t sum4_lo = vdupq_n_f32(0.0f);
    float32x4_t sum4_hi = vdupq_n_f32(0.0f);

    for(uint32_t i=0; i < blockSize - (blockSize % 32); i+=32)
    {
        float16x8_t a1 = vld1q_f16(pSrcA);
        float16x8_t b1 = vld1q_f16(pSrcB);
        sum1_lo = vfmaq_f32(sum1_lo, vcvt_f32_f16(vget_low_f16(a1)),  vcvt_f32_f16(vget_low_f16(b1)));
        sum1_hi = vfmaq_f32(sum1_hi, vcvt_f32_f16(vget_high_f16(a1)), vcvt_f32_f16(vget_high_f16(b1)));

        float16x8_t a2 = vld1q_f16(pSrcA+8);
        float16x8_t b2 = vld1q_f16(pSrcB+8);
        sum2_lo = vfmaq_f32(sum2_lo, vcvt_f32_f16(vget_low_f16(a2)),  vcvt_f32_f16(vget_low_f16(b2)));
        sum2_hi = vfmaq_f32(sum2_hi, vcvt_f32_f16(vget_high_f16(a2)), vcvt_f32_f16(vget_high_f16(b2)));

        float16x8_t a3 = vld1q_f16(pSrcA+16);
        float16x8_t b3 = vld1q_f16(pSrcB+16);
        sum3_lo = vfmaq_f32(sum3_lo, vcvt_f32_f16(vget_low_f16(a3)),  vcvt_f32_f16(vget_low_f16(b3)));
        sum3_hi = vfmaq_f32(sum3_hi, vcvt_f32_f16(vget_high_f16(a3)), vcvt_f32_f16(vget_high_f16(b3)));

        float16x8_t a4 = vld1q_f16(pSrcA+24);
        float16x8_t b4 = vld1q_f16(pSrcB+24);
        sum4_lo = vfmaq_f32(sum4_lo, vcvt_f32_f16(vget_low_f16(a4)),  vcvt_f32_f16(vget_low_f16(b4)));
        sum4_hi = vfmaq_f32(sum4_hi, vcvt_f32_f16(vget_high_f16(a4)), vcvt_f32_f16(vget_high_f16(b4)));

        pSrcA += 32;
        pSrcB += 32;
    }

    float32x4_t sum = vaddq_f32(sum1_lo, sum1_hi);
    sum = vaddq_f32(sum, sum2_lo);
    sum = vaddq_f32(sum, sum2_hi);
    sum = vaddq_f32(sum, sum3_lo);
    sum = vaddq_f32(sum, sum3_hi);
    sum = vaddq_f32(sum, sum4_lo);
    sum = vaddq_f32(sum, sum4_hi);

    dotProduct = vaddvq_f32(sum);
    for (uint32_t i=0; i < (blockSize % 32); i++) {
        // Multiply in f32: two f16 operands each up to 256.0 give 65536,
        // which overflows the f16 maximum 65504 (saturates to +inf) while
        // the f32-accumulating scalar and AVX paths stay finite.
        dotProduct += (float32_t)(*pSrcA) * (float32_t)(*pSrcB);
        pSrcA += 1;
        pSrcB += 1;
    }

    return dotProduct;
}

float32_t euclideanDist_half_4x4(const float16_t* pSrcA, const float16_t* pSrcB, uint32_t blockSize)
{
    float32_t euclideanDistance = 0.0f;
    // Accumulate in f32 (see dotProduct_half_4x4): a single lane with
    // a=256.0, b=0.0 contributes 65536 to the sum, which overflows the f16
    // maximum 65504 and saturates the lane sum to +inf, so the final score
    // is serialized as JSON null once the 32-element kernel loop executes.
    float32x4_t sum1_lo = vdupq_n_f32(0.0f);
    float32x4_t sum1_hi = vdupq_n_f32(0.0f);
    float32x4_t sum2_lo = vdupq_n_f32(0.0f);
    float32x4_t sum2_hi = vdupq_n_f32(0.0f);
    float32x4_t sum3_lo = vdupq_n_f32(0.0f);
    float32x4_t sum3_hi = vdupq_n_f32(0.0f);
    float32x4_t sum4_lo = vdupq_n_f32(0.0f);
    float32x4_t sum4_hi = vdupq_n_f32(0.0f);

    for(uint32_t i=0; i < blockSize - (blockSize % 32); i+=32)
    {
        float16x8_t a1 = vld1q_f16(pSrcA);
        float16x8_t b1 = vld1q_f16(pSrcB);
        float32x4_t sub1_lo = vsubq_f32(vcvt_f32_f16(vget_low_f16(a1)),  vcvt_f32_f16(vget_low_f16(b1)));
        float32x4_t sub1_hi = vsubq_f32(vcvt_f32_f16(vget_high_f16(a1)), vcvt_f32_f16(vget_high_f16(b1)));
        sum1_lo = vfmaq_f32(sum1_lo, sub1_lo, sub1_lo);
        sum1_hi = vfmaq_f32(sum1_hi, sub1_hi, sub1_hi);

        float16x8_t a2 = vld1q_f16(pSrcA+8);
        float16x8_t b2 = vld1q_f16(pSrcB+8);
        float32x4_t sub2_lo = vsubq_f32(vcvt_f32_f16(vget_low_f16(a2)),  vcvt_f32_f16(vget_low_f16(b2)));
        float32x4_t sub2_hi = vsubq_f32(vcvt_f32_f16(vget_high_f16(a2)), vcvt_f32_f16(vget_high_f16(b2)));
        sum2_lo = vfmaq_f32(sum2_lo, sub2_lo, sub2_lo);
        sum2_hi = vfmaq_f32(sum2_hi, sub2_hi, sub2_hi);

        float16x8_t a3 = vld1q_f16(pSrcA+16);
        float16x8_t b3 = vld1q_f16(pSrcB+16);
        float32x4_t sub3_lo = vsubq_f32(vcvt_f32_f16(vget_low_f16(a3)),  vcvt_f32_f16(vget_low_f16(b3)));
        float32x4_t sub3_hi = vsubq_f32(vcvt_f32_f16(vget_high_f16(a3)), vcvt_f32_f16(vget_high_f16(b3)));
        sum3_lo = vfmaq_f32(sum3_lo, sub3_lo, sub3_lo);
        sum3_hi = vfmaq_f32(sum3_hi, sub3_hi, sub3_hi);

        float16x8_t a4 = vld1q_f16(pSrcA+24);
        float16x8_t b4 = vld1q_f16(pSrcB+24);
        float32x4_t sub4_lo = vsubq_f32(vcvt_f32_f16(vget_low_f16(a4)),  vcvt_f32_f16(vget_low_f16(b4)));
        float32x4_t sub4_hi = vsubq_f32(vcvt_f32_f16(vget_high_f16(a4)), vcvt_f32_f16(vget_high_f16(b4)));
        sum4_lo = vfmaq_f32(sum4_lo, sub4_lo, sub4_lo);
        sum4_hi = vfmaq_f32(sum4_hi, sub4_hi, sub4_hi);

        pSrcA += 32;
        pSrcB += 32;
    }

    float32x4_t sum = vaddq_f32(sum1_lo, sum1_hi);
    sum = vaddq_f32(sum, sum2_lo);
    sum = vaddq_f32(sum, sum2_hi);
    sum = vaddq_f32(sum, sum3_lo);
    sum = vaddq_f32(sum, sum3_hi);
    sum = vaddq_f32(sum, sum4_lo);
    sum = vaddq_f32(sum, sum4_hi);

    euclideanDistance = vaddvq_f32(sum);
    for (uint32_t i=0; i < (blockSize % 32); i++) {
        // Compute the difference in f32: the f16 difference of two up-to-65504
        // operands (e.g. 65504.0 - (-65504.0) = 131008) or the f16 square of a
        // 256.0 difference (262144) would overflow the f16 maximum 65504 and
        // saturate to +inf, disagreeing with the scalar path.
        float32_t diff = (float32_t)(*pSrcA) - (float32_t)(*pSrcB);
        euclideanDistance += diff * diff;
        pSrcA += 1;
        pSrcB += 1;
    }

    return euclideanDistance;
}

float32_t manhattanDist_half_4x4(const float16_t* pSrcA, const float16_t* pSrcB, uint32_t blockSize)
{
    float32_t manhattanDistance = 0.0f;
    // Accumulate in f32 (see dotProduct_half_4x4): f16 lane sums of |a-b|
    // saturate to +inf past 65504, disagreeing with the f32-accumulating
    // scalar and AVX paths.
    float32x4_t sum1_lo = vdupq_n_f32(0.0f);
    float32x4_t sum1_hi = vdupq_n_f32(0.0f);
    float32x4_t sum2_lo = vdupq_n_f32(0.0f);
    float32x4_t sum2_hi = vdupq_n_f32(0.0f);
    float32x4_t sum3_lo = vdupq_n_f32(0.0f);
    float32x4_t sum3_hi = vdupq_n_f32(0.0f);
    float32x4_t sum4_lo = vdupq_n_f32(0.0f);
    float32x4_t sum4_hi = vdupq_n_f32(0.0f);
    uint32_t i = 0;

    for(i=0; i < blockSize - (blockSize % 32); i+=32)
    {
        float16x8_t a1 = vld1q_f16(pSrcA);
        float16x8_t b1 = vld1q_f16(pSrcB);
        float32x4_t sub1_lo = vsubq_f32(vcvt_f32_f16(vget_low_f16(a1)),  vcvt_f32_f16(vget_low_f16(b1)));
        float32x4_t sub1_hi = vsubq_f32(vcvt_f32_f16(vget_high_f16(a1)), vcvt_f32_f16(vget_high_f16(b1)));
        sum1_lo = vaddq_f32(sum1_lo, vabsq_f32(sub1_lo));
        sum1_hi = vaddq_f32(sum1_hi, vabsq_f32(sub1_hi));

        float16x8_t a2 = vld1q_f16(pSrcA+8);
        float16x8_t b2 = vld1q_f16(pSrcB+8);
        float32x4_t sub2_lo = vsubq_f32(vcvt_f32_f16(vget_low_f16(a2)),  vcvt_f32_f16(vget_low_f16(b2)));
        float32x4_t sub2_hi = vsubq_f32(vcvt_f32_f16(vget_high_f16(a2)), vcvt_f32_f16(vget_high_f16(b2)));
        sum2_lo = vaddq_f32(sum2_lo, vabsq_f32(sub2_lo));
        sum2_hi = vaddq_f32(sum2_hi, vabsq_f32(sub2_hi));

        float16x8_t a3 = vld1q_f16(pSrcA+16);
        float16x8_t b3 = vld1q_f16(pSrcB+16);
        float32x4_t sub3_lo = vsubq_f32(vcvt_f32_f16(vget_low_f16(a3)),  vcvt_f32_f16(vget_low_f16(b3)));
        float32x4_t sub3_hi = vsubq_f32(vcvt_f32_f16(vget_high_f16(a3)), vcvt_f32_f16(vget_high_f16(b3)));
        sum3_lo = vaddq_f32(sum3_lo, vabsq_f32(sub3_lo));
        sum3_hi = vaddq_f32(sum3_hi, vabsq_f32(sub3_hi));

        float16x8_t a4 = vld1q_f16(pSrcA+24);
        float16x8_t b4 = vld1q_f16(pSrcB+24);
        float32x4_t sub4_lo = vsubq_f32(vcvt_f32_f16(vget_low_f16(a4)),  vcvt_f32_f16(vget_low_f16(b4)));
        float32x4_t sub4_hi = vsubq_f32(vcvt_f32_f16(vget_high_f16(a4)), vcvt_f32_f16(vget_high_f16(b4)));
        sum4_lo = vaddq_f32(sum4_lo, vabsq_f32(sub4_lo));
        sum4_hi = vaddq_f32(sum4_hi, vabsq_f32(sub4_hi));

        pSrcA += 32;
        pSrcB += 32;
    }

    float32x4_t sum = vaddq_f32(sum1_lo, sum1_hi);
    sum = vaddq_f32(sum, sum2_lo);
    sum = vaddq_f32(sum, sum2_hi);
    sum = vaddq_f32(sum, sum3_lo);
    sum = vaddq_f32(sum, sum3_hi);
    sum = vaddq_f32(sum, sum4_lo);
    sum = vaddq_f32(sum, sum4_hi);

    manhattanDistance = vaddvq_f32(sum);
    for (i=0; i < (blockSize % 32); i++) {
        // Compute the difference in f32: the f16 difference of two up-to-65504
        // operands would overflow the f16 maximum 65504 and saturate to +inf,
        // disagreeing with the scalar path.
        float32_t diff = (float32_t)(*pSrcA) - (float32_t)(*pSrcB);
        manhattanDistance += (diff < 0.0f) ? -diff : diff;
        pSrcA += 1;
        pSrcB += 1;
    }

    return manhattanDistance;
}
#endif