#if defined(_MSC_VER)
#include <arm64_neon.h>
#define QDRANT_FP16_NEON_ENABLED 1
#else
#include <arm_neon.h>
#ifdef __ARM_FEATURE_FP16_VECTOR_ARITHMETIC
#include <arm_fp16.h>
#define QDRANT_FP16_NEON_ENABLED 1
#endif
#endif

#ifdef QDRANT_FP16_NEON_ENABLED
#if defined(_MSC_VER)
#define QDRANT_F16_STORAGE uint16_t
#define QDRANT_F16_ZERO() vreinterpretq_f16_u16(vdupq_n_u16(0))
#define QDRANT_F16_LOAD(ptr) vreinterpretq_f16_u16(vld1q_u16((const uint16_t*)(ptr)))

static inline float16x4_t qdrant_f16_splat(uint16_t bits)
{
    return vreinterpret_f16_u16(vdup_n_u16(bits));
}

static inline float qdrant_f16_lane_to_f32(float16x4_t value)
{
    return vgetq_lane_f32(vcvt_f32_f16(value), 0);
}

static inline float qdrant_mulf_f16(uint16_t lhs, uint16_t rhs)
{
    return qdrant_f16_lane_to_f32(vmul_f16(qdrant_f16_splat(lhs), qdrant_f16_splat(rhs)));
}

static inline float qdrant_sqdiff_f16(uint16_t lhs, uint16_t rhs)
{
    float16x4_t diff = vsub_f16(qdrant_f16_splat(lhs), qdrant_f16_splat(rhs));
    return qdrant_f16_lane_to_f32(vmul_f16(diff, diff));
}

static inline float qdrant_absdiff_f16(uint16_t lhs, uint16_t rhs)
{
    return qdrant_f16_lane_to_f32(vabd_f16(qdrant_f16_splat(lhs), qdrant_f16_splat(rhs)));
}
#else
#define QDRANT_F16_STORAGE float16_t
#define QDRANT_F16_ZERO() vdupq_n_f16(0.0f)
#define QDRANT_F16_LOAD(ptr) vld1q_f16(ptr)
#endif

float32_t dotProduct_half_4x4(const QDRANT_F16_STORAGE* pSrcA, const QDRANT_F16_STORAGE* pSrcB, uint32_t blockSize)
{
    float32_t dotProduct = 0.0f;
    float16x8_t sum1 = QDRANT_F16_ZERO();
    float16x8_t sum2 = QDRANT_F16_ZERO();
    float16x8_t sum3 = QDRANT_F16_ZERO();
    float16x8_t sum4 = QDRANT_F16_ZERO();

    for(uint32_t i=0; i < blockSize - (blockSize % 32); i+=32)
    {
        sum1 = vfmaq_f16(sum1, QDRANT_F16_LOAD(pSrcA), QDRANT_F16_LOAD(pSrcB));
        sum2 = vfmaq_f16(sum2, QDRANT_F16_LOAD(pSrcA+8), QDRANT_F16_LOAD(pSrcB+8));
        sum3 = vfmaq_f16(sum3, QDRANT_F16_LOAD(pSrcA+16), QDRANT_F16_LOAD(pSrcB+16));
        sum4 = vfmaq_f16(sum4, QDRANT_F16_LOAD(pSrcA+24), QDRANT_F16_LOAD(pSrcB+24));

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
#if defined(_MSC_VER)
        dotProduct += qdrant_mulf_f16(*pSrcA, *pSrcB);
#else
        dotProduct += (float32_t)((*pSrcA)*(*pSrcB));
#endif
        pSrcA += 1;
        pSrcB += 1;
    }

    return dotProduct;
}

float32_t euclideanDist_half_4x4(const QDRANT_F16_STORAGE* pSrcA, const QDRANT_F16_STORAGE* pSrcB, uint32_t blockSize)
{
    float32_t euclideanDistance = 0.0f;
    float16x8_t sum1 = QDRANT_F16_ZERO();
    float16x8_t sub1 = QDRANT_F16_ZERO();
    float16x8_t sum2 = QDRANT_F16_ZERO();
    float16x8_t sub2 = QDRANT_F16_ZERO();
    float16x8_t sum3 = QDRANT_F16_ZERO();
    float16x8_t sub3 = QDRANT_F16_ZERO();
    float16x8_t sum4 = QDRANT_F16_ZERO();
    float16x8_t sub4 = QDRANT_F16_ZERO();

    for(uint32_t i=0; i < blockSize - (blockSize % 32); i+=32)
    {
        sub1 = vsubq_f16(QDRANT_F16_LOAD(pSrcA), QDRANT_F16_LOAD(pSrcB));
        sum1 = vfmaq_f16(sum1, sub1, sub1);

        sub2 = vsubq_f16(QDRANT_F16_LOAD(pSrcA+8), QDRANT_F16_LOAD(pSrcB+8));
        sum2 = vfmaq_f16(sum2, sub2, sub2);

        sub3 = vsubq_f16(QDRANT_F16_LOAD(pSrcA+16), QDRANT_F16_LOAD(pSrcB+16));
        sum3 = vfmaq_f16(sum3, sub3, sub3);

        sub4 = vsubq_f16(QDRANT_F16_LOAD(pSrcA+24), QDRANT_F16_LOAD(pSrcB+24));
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
    for (uint32_t i=0; i < (blockSize % 32); i++) {
#if defined(_MSC_VER)
        euclideanDistance += qdrant_sqdiff_f16(*pSrcA, *pSrcB);
#else
        float16_t tmp = (*pSrcA - *pSrcB);
        euclideanDistance += (float32_t)(tmp * tmp);
#endif
        pSrcA += 1;
        pSrcB += 1;
    }

    return euclideanDistance;
}

float32_t manhattanDist_half_4x4(const QDRANT_F16_STORAGE* pSrcA, const QDRANT_F16_STORAGE* pSrcB, uint32_t blockSize)
{
    float32_t manhattanDistance = 0.0f;
    float16x8_t sum1 = QDRANT_F16_ZERO();
    float16x8_t sum2 = QDRANT_F16_ZERO();
    float16x8_t sum3 = QDRANT_F16_ZERO();
    float16x8_t sum4 = QDRANT_F16_ZERO();
    uint32_t i = 0;

    for(i=0; i < blockSize - (blockSize % 32); i+=32)
    {
        sum1 = vaddq_f16(sum1, vabdq_f16(QDRANT_F16_LOAD(pSrcA), QDRANT_F16_LOAD(pSrcB)));

        sum2 = vaddq_f16(sum2, vabdq_f16(QDRANT_F16_LOAD(pSrcA+8), QDRANT_F16_LOAD(pSrcB+8)));

        sum3 = vaddq_f16(sum3, vabdq_f16(QDRANT_F16_LOAD(pSrcA+16), QDRANT_F16_LOAD(pSrcB+16)));

        sum4 = vaddq_f16(sum4, vabdq_f16(QDRANT_F16_LOAD(pSrcA+24), QDRANT_F16_LOAD(pSrcB+24)));

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
#if defined(_MSC_VER)
        manhattanDistance += qdrant_absdiff_f16(*pSrcA, *pSrcB);
#else
        manhattanDistance += (float32_t)(vabsh_f16(*pSrcA - *pSrcB));
#endif
        pSrcA += 1;
        pSrcB += 1;
    }

    return manhattanDistance;
}
#endif
