// Platform-specific includes for ARM NEON
#if defined(_MSC_VER)
    // MSVC on Windows ARM64
    #include <arm64_neon.h>
    // MSVC doesn't define __ARM_FEATURE_FP16_VECTOR_ARITHMETIC,
    // but supports FP16 intrinsics on ARM64. We enable it manually.
    #define QDRANT_FP16_NEON_ENABLED 1
    // MSVC uses _Float16 instead of float16_t in some contexts
    // arm64_neon.h should provide float16_t, but we ensure compatibility
#elif !defined(PC_VER)
    // GCC/Clang on Unix-like systems
    #include <arm_neon.h>
    #ifdef __ARM_FEATURE_FP16_VECTOR_ARITHMETIC
        #include <arm_fp16.h>
        #define QDRANT_FP16_NEON_ENABLED 1
    #endif
#endif

#ifdef QDRANT_FP16_NEON_ENABLED

// For MSVC, we need to handle the vabsh_f16 intrinsic which may not be available
#if defined(_MSC_VER)
    // MSVC may not have vabsh_f16, provide a fallback using fabsf
    #include <math.h>
    static inline float16_t qdrant_abs_f16(float16_t x) {
        // Convert to float, take abs, convert back
        // This is a scalar fallback for the remainder loop
        float f = (float)x;
        return (float16_t)fabsf(f);
    }
    #define VABSH_F16(x) qdrant_abs_f16(x)
#else
    #define VABSH_F16(x) vabsh_f16(x)
#endif

float32_t dotProduct_half_4x4(const float16_t* pSrcA, const float16_t* pSrcB, uint32_t blockSize)
{
    float32_t dotProduct = 0.0f;
    float16x8_t sum1 = vdupq_n_f16(0.0f);
    float16x8_t sum2 = vdupq_n_f16(0.0f);
    float16x8_t sum3 = vdupq_n_f16(0.0f);
    float16x8_t sum4 = vdupq_n_f16(0.0f);

    for(uint32_t i=0; i < blockSize - (blockSize % 32); i+=32)
    {
        sum1 = vfmaq_f16(sum1, vld1q_f16(pSrcA), vld1q_f16(pSrcB));
        sum2 = vfmaq_f16(sum2, vld1q_f16(pSrcA+8), vld1q_f16(pSrcB+8));
        sum3 = vfmaq_f16(sum3, vld1q_f16(pSrcA+16), vld1q_f16(pSrcB+16));
        sum4 = vfmaq_f16(sum4, vld1q_f16(pSrcA+24), vld1q_f16(pSrcB+24));

        pSrcA += 32;
        pSrcB += 32;
    }

    float32x4_t sum = vcvt_f32_f16(vget_high_f16(sum1));
    sum = vaddq_f32(sum, vcvt_f32_f16(vget_low_f16(sum1)));
    sum = vaddq_f32(sum, vcvt_f32_f16(vget_high_f16(sum2)));
    sum = vaddq_f32(sum, vcvt_f32_f16(vget_low_f16(sum2)));
    sum = vaddq_f32(sum, vcvt_f32_f16(vget_high_f16(sum3)));
    sum = vaddq_f32(sum, vcvt_f32_f16(vget_low_f16(sum3)));
    sum = vaddq_f32(sum, vcvt_f32_f16(vget_high_f16(sum4)));
    sum = vaddq_f32(sum, vcvt_f32_f16(vget_low_f16(sum4)));

    dotProduct = vaddvq_f32(sum);
    for (uint32_t i=0; i < (blockSize % 32); i++) {
        dotProduct += (float32_t)((*pSrcA)*(*pSrcB));
        pSrcA += 1;
        pSrcB += 1;
    }

    return dotProduct;
}

float32_t euclideanDist_half_4x4(const float16_t* pSrcA, const float16_t* pSrcB, uint32_t blockSize)
{
    float32_t euclideanDistance = 0.0f;
    float16x8_t sum1 = vdupq_n_f16(0.0f);
    float16x8_t sub1 = vdupq_n_f16(0.0f);
    float16x8_t sum2 = vdupq_n_f16(0.0f);
    float16x8_t sub2 = vdupq_n_f16(0.0f);
    float16x8_t sum3 = vdupq_n_f16(0.0f);
    float16x8_t sub3 = vdupq_n_f16(0.0f);
    float16x8_t sum4 = vdupq_n_f16(0.0f);
    float16x8_t sub4 = vdupq_n_f16(0.0f);

    for(uint32_t i=0; i < blockSize - (blockSize % 32); i+=32)
    {
        sub1 = vsubq_f16(vld1q_f16(pSrcA), vld1q_f16(pSrcB));
        sum1 = vfmaq_f16(sum1, sub1, sub1);

        sub2 = vsubq_f16(vld1q_f16(pSrcA+8), vld1q_f16(pSrcB+8));
        sum2 = vfmaq_f16(sum2, sub2, sub2);

        sub3 = vsubq_f16(vld1q_f16(pSrcA+16), vld1q_f16(pSrcB+16));
        sum3 = vfmaq_f16(sum3, sub3, sub3);

        sub4 = vsubq_f16(vld1q_f16(pSrcA+24), vld1q_f16(pSrcB+24));
        sum4 = vfmaq_f16(sum4, sub4, sub4);

        pSrcA += 32;
        pSrcB += 32;
    }

    float32x4_t sum = vcvt_f32_f16(vget_high_f16(sum1));
    sum = vaddq_f32(sum, vcvt_f32_f16(vget_low_f16(sum1)));
    sum = vaddq_f32(sum, vcvt_f32_f16(vget_high_f16(sum2)));
    sum = vaddq_f32(sum, vcvt_f32_f16(vget_low_f16(sum2)));
    sum = vaddq_f32(sum, vcvt_f32_f16(vget_high_f16(sum3)));
    sum = vaddq_f32(sum, vcvt_f32_f16(vget_low_f16(sum3)));
    sum = vaddq_f32(sum, vcvt_f32_f16(vget_high_f16(sum4)));
    sum = vaddq_f32(sum, vcvt_f32_f16(vget_low_f16(sum4)));

    euclideanDistance = vaddvq_f32(sum);
    float16_t tmp = 0.0f;
    for (uint32_t i=0; i < (blockSize % 32); i++) {
        tmp = (*pSrcA - *pSrcB);
        euclideanDistance += (float32_t)(tmp * tmp);
        pSrcA += 1;
        pSrcB += 1;
    }

    return euclideanDistance;
}

float32_t manhattanDist_half_4x4(const float16_t* pSrcA, const float16_t* pSrcB, uint32_t blockSize)
{
    float32_t manhattanDistance = 0.0f;
    float16x8_t sum1 = vdupq_n_f16(0.0f);
    float16x8_t sum2 = vdupq_n_f16(0.0f);
    float16x8_t sum3 = vdupq_n_f16(0.0f);
    float16x8_t sum4 = vdupq_n_f16(0.0f);
    uint32_t i = 0;

    for(i=0; i < blockSize - (blockSize % 32); i+=32)
    {
        sum1 = vaddq_f16(sum1, vabdq_f16(vld1q_f16(pSrcA), vld1q_f16(pSrcB)));

        sum2 = vaddq_f16(sum2, vabdq_f16(vld1q_f16(pSrcA+8), vld1q_f16(pSrcB+8)));

        sum3 = vaddq_f16(sum3, vabdq_f16(vld1q_f16(pSrcA+16), vld1q_f16(pSrcB+16)));

        sum4 = vaddq_f16(sum4, vabdq_f16(vld1q_f16(pSrcA+24), vld1q_f16(pSrcB+24)));

        pSrcA += 32;
        pSrcB += 32;
    }

    float32x4_t sum = vcvt_f32_f16(vget_high_f16(sum1));
    sum = vaddq_f32(sum, vcvt_f32_f16(vget_low_f16(sum1)));
    sum = vaddq_f32(sum, vcvt_f32_f16(vget_high_f16(sum2)));
    sum = vaddq_f32(sum, vcvt_f32_f16(vget_low_f16(sum2)));
    sum = vaddq_f32(sum, vcvt_f32_f16(vget_high_f16(sum3)));
    sum = vaddq_f32(sum, vcvt_f32_f16(vget_low_f16(sum3)));
    sum = vaddq_f32(sum, vcvt_f32_f16(vget_high_f16(sum4)));
    sum = vaddq_f32(sum, vcvt_f32_f16(vget_low_f16(sum4)));

    manhattanDistance = vaddvq_f32(sum);
    for (i=0; i < (blockSize % 32); i++) {
        manhattanDistance += (float32_t)(VABSH_F16(*pSrcA - *pSrcB));
        pSrcA += 1;
        pSrcB += 1;
    }

    return manhattanDistance;
}

#endif /* QDRANT_FP16_NEON_ENABLED */
