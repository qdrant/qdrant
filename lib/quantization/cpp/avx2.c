#include <stdlib.h>
#include <stdint.h>
#include <immintrin.h>

#include "export_macro.h"

#define HSUM256_PS(X, R) \
    float R = 0.0f; \
    { \
    __m128 x128 = _mm_add_ps(_mm256_extractf128_ps(X, 1), _mm256_castps256_ps128(X)); \
    __m128 x64 = _mm_add_ps(x128, _mm_movehl_ps(x128, x128)); \
    __m128 x32 = _mm_add_ss(x64, _mm_shuffle_ps(x64, x64, 0x55)); \
    R = _mm_cvtss_f32(x32); \
    }

#define HSUM256_EPI32(X, R) \
    int R = 0; \
    { \
    __m128i x128 = _mm_add_epi32(_mm256_extractf128_si256(X, 1), _mm256_castsi256_si128(X)); \
    __m128i x64 = _mm_add_epi32(x128, _mm_srli_si128(x128, 8)); \
    __m128i x32 = _mm_add_epi32(x64, _mm_srli_si128(x64, 4)); \
    R = _mm_cvtsi128_si32(x32); \
    }

EXPORT float impl_score_dot_avx(
    const uint8_t* query_ptr,
    const uint8_t* vector_ptr,
    uint32_t dim
) {
    const __m256i* v_ptr = (const __m256i*)vector_ptr;
    const __m256i* q_ptr = (const __m256i*)query_ptr;

    __m256i mul1 = _mm256_setzero_si256();
    __m256i mask_epu32 = _mm256_set1_epi32(0xFFFF);
    for (uint32_t _i = 0; _i < dim / 32; _i++) {
        __m256i v = _mm256_loadu_si256(v_ptr);
        __m256i q = _mm256_loadu_si256(q_ptr);
        v_ptr++;
        q_ptr++;

        __m256i s = _mm256_maddubs_epi16(v, q);
        __m256i s_low = _mm256_cvtepi16_epi32(_mm256_castsi256_si128(s));
        __m256i s_high = _mm256_cvtepi16_epi32(_mm256_extractf128_si256(s, 1));
        mul1 = _mm256_add_epi32(mul1, s_low);
        mul1 = _mm256_add_epi32(mul1, s_high);
    }

    // the vector sizes are assumed to be multiples of 16, check if one last 16-element part remaining
    if (dim % 32 != 0) {
        __m128i v_short = _mm_loadu_si128((const __m128i*)v_ptr);
        __m128i q_short = _mm_loadu_si128((const __m128i*)q_ptr);

        __m256i v1 = _mm256_cvtepu8_epi16(v_short);
        __m256i q1 = _mm256_cvtepu8_epi16(q_short);

        __m256i s = _mm256_mullo_epi16(v1, q1);
        mul1 = _mm256_add_epi32(mul1, _mm256_and_si256(s, mask_epu32));
        mul1 = _mm256_add_epi32(mul1, _mm256_srli_epi32(s, 16));
    }
    __m256 mul_ps = _mm256_cvtepi32_ps(mul1);
    HSUM256_PS(mul_ps, mul_scalar);
    return mul_scalar;
}

EXPORT float impl_score_l1_avx(
    const uint8_t* query_ptr,
    const uint8_t* vector_ptr,
    uint32_t dim
) {
    const __m256i* v_ptr = (const __m256i*)vector_ptr;
    const __m256i* q_ptr = (const __m256i*)query_ptr;

    uint32_t m = dim - (dim % 32);
    __m256i sum256 = _mm256_setzero_si256();

    for (uint32_t i = 0; i < m; i += 32) {
        __m256i v = _mm256_loadu_si256(v_ptr);
        __m256i q = _mm256_loadu_si256(q_ptr);
        v_ptr++;
        q_ptr++;

        // Compute the difference in both directions and take the maximum for abs
        __m256i diff1 = _mm256_subs_epu8(v, q);
        __m256i diff2 = _mm256_subs_epu8(q, v);

        __m256i abs_diff = _mm256_max_epu8(diff1, diff2);

        __m256i abs_diff16_lo = _mm256_unpacklo_epi8(abs_diff, _mm256_setzero_si256());
        __m256i abs_diff16_hi = _mm256_unpackhi_epi8(abs_diff, _mm256_setzero_si256());

        sum256 = _mm256_add_epi16(sum256, abs_diff16_lo);
        sum256 = _mm256_add_epi16(sum256, abs_diff16_hi);
    }

    // the vector sizes are assumed to be multiples of 16, check if one last 16-element part remaining
    if (m < dim) {
        __m128i v_short = _mm_loadu_si128((const __m128i * ) v_ptr);
        __m128i q_short = _mm_loadu_si128((const __m128i * ) q_ptr);

        __m128i diff1 = _mm_subs_epu8(v_short, q_short);
        __m128i diff2 = _mm_subs_epu8(q_short, v_short);

        __m128i abs_diff = _mm_max_epu8(diff1, diff2);

        __m128i abs_diff16_lo_128 = _mm_unpacklo_epi8(abs_diff, _mm_setzero_si128());
        __m128i abs_diff16_hi_128 = _mm_unpackhi_epi8(abs_diff, _mm_setzero_si128());

        __m256i abs_diff16_lo = _mm256_cvtepu16_epi32(abs_diff16_lo_128);
        __m256i abs_diff16_hi = _mm256_cvtepu16_epi32(abs_diff16_hi_128);

        sum256 = _mm256_add_epi16(sum256, abs_diff16_lo);
        sum256 = _mm256_add_epi16(sum256, abs_diff16_hi);
    }

    __m256i sum_epi32 = _mm256_add_epi32(
        _mm256_unpacklo_epi16(sum256, _mm256_setzero_si256()),
        _mm256_unpackhi_epi16(sum256, _mm256_setzero_si256()));

    HSUM256_EPI32(sum_epi32, sum);

    return (float) sum;
}
