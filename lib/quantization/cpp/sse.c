#include <stdlib.h>
#include <stdint.h>
#include <immintrin.h>

#include "export_macro.h"

#define HSUM128_PS(X, R) \
    float R = 0.0f; \
    { \
    __m128 x64 = _mm_add_ps(X, _mm_movehl_ps(X, X)); \
    __m128 x32 = _mm_add_ss(x64, _mm_shuffle_ps(x64, x64, 0x55)); \
    R = _mm_cvtss_f32(x32); \
    }

#define HSUM128_EPI16(X, R) \
    int R = 0; \
    { \
    __m128i x64 = _mm_add_epi16(X, _mm_srli_si128(X, 8)); \
    __m128i x32 = _mm_add_epi16(x64, _mm_srli_si128(x64, 4)); \
    R = _mm_extract_epi16(x32, 0) + _mm_extract_epi16(x32, 1); \
    }

EXPORT float impl_score_dot_sse(
    const uint8_t* query_ptr,
    const uint8_t* vector_ptr,
    uint32_t dim
) {
    const __m128i* v_ptr = (const __m128i*)vector_ptr;
    const __m128i* q_ptr = (const __m128i*)query_ptr;

    __m128i mul = _mm_setzero_si128();
    for (uint32_t _i = 0; _i < dim / 16; _i++) {
        __m128i v = _mm_loadu_si128(v_ptr);
        __m128i q = _mm_loadu_si128(q_ptr);
        v_ptr++;
        q_ptr++;

        __m128i s = _mm_maddubs_epi16(v, q);
        __m128i s_low = _mm_cvtepi16_epi32(s);
        __m128i s_high = _mm_cvtepi16_epi32(_mm_srli_si128(s, 8));
        mul = _mm_add_epi32(mul, s_low);
        mul = _mm_add_epi32(mul, s_high);
    }
    __m128 mul_ps = _mm_cvtepi32_ps(mul);
    HSUM128_PS(mul_ps, mul_scalar);
    return mul_scalar;
}

EXPORT uint32_t impl_xor_popcnt_sse_uint128(
    const uint8_t* query_ptr,
    const uint8_t* vector_ptr,
    uint32_t count
) {
    int64_t result = 0;
    for (uint32_t _i = 0; _i < count; _i++) {
        const uint64_t* v_ptr_1 = (const uint64_t*)vector_ptr;
        const uint64_t* q_ptr_1 = (const uint64_t*)query_ptr;
        uint64_t x_1 = (*v_ptr_1) ^ (*q_ptr_1);
        result += _mm_popcnt_u64(x_1);

        const uint64_t* v_ptr_2 = v_ptr_1 + 1;
        const uint64_t* q_ptr_2 = q_ptr_1 + 1;
        uint64_t x_2 = (*v_ptr_2) ^ (*q_ptr_2);
        result += _mm_popcnt_u64(x_2);

        vector_ptr += 16;
        query_ptr += 16;
    }
    return (uint32_t)result;
}

EXPORT uint32_t impl_xor_popcnt_sse_uint64(
    const uint8_t* query_ptr,
    const uint8_t* vector_ptr,
    uint32_t count
) {
    int64_t result = 0;
    for (uint32_t _i = 0; _i < count; _i++) {
        const uint64_t* v_ptr = (const uint64_t*)vector_ptr;
        const uint64_t* q_ptr = (const uint64_t*)query_ptr;
        uint64_t x = (*v_ptr) ^ (*q_ptr);
        result += _mm_popcnt_u64(x);

        vector_ptr += 8;
        query_ptr += 8;
    }
    return (uint32_t)result;
}

EXPORT uint32_t impl_xor_popcnt_sse_uint32(
    const uint8_t* query_ptr,
    const uint8_t* vector_ptr,
    uint32_t count
) {
    int result = 0;
    for (uint32_t _i = 0; _i < count; _i++) {
        const uint32_t* v_ptr = (const uint32_t*)vector_ptr;
        const uint32_t* q_ptr = (const uint32_t*)query_ptr;
        uint32_t x = (*v_ptr) ^ (*q_ptr);
        result += _mm_popcnt_u32(x);

        vector_ptr += 4;
        query_ptr += 4;
    }
    return (uint32_t)result;
}

EXPORT float impl_score_l1_sse(
    const uint8_t* query_ptr,
    const uint8_t* vector_ptr,
    uint32_t dim
) {
    const __m128i* v_ptr = (const __m128i*)vector_ptr;
    const __m128i* q_ptr = (const __m128i*)query_ptr;

    uint32_t m = dim - (dim % 16);
    __m128i sum128 = _mm_setzero_si128();

    // the vector sizes are assumed to be multiples of 16, no remaining part here
    for (uint32_t i = 0; i < m; i += 16) {
        __m128i vec2 = _mm_loadu_si128(v_ptr);
        __m128i vec1 = _mm_loadu_si128(q_ptr);
        v_ptr++;
        q_ptr++;

        // Compute the difference in both directions
        __m128i diff1 = _mm_subs_epu8(vec1, vec2);
        __m128i diff2 = _mm_subs_epu8(vec2, vec1);

        // Take the maximum
        __m128i abs_diff = _mm_max_epu8(diff1, diff2);

        __m128i abs_diff16_low = _mm_unpacklo_epi8(abs_diff, _mm_setzero_si128());
        __m128i abs_diff16_high = _mm_unpackhi_epi8(abs_diff, _mm_setzero_si128());

        sum128 = _mm_add_epi16(sum128, abs_diff16_low);
        sum128 = _mm_add_epi16(sum128, abs_diff16_high);
    }

    // Convert 16-bit sums to 32-bit and sum them up
    __m128i sum_epi32 = _mm_add_epi32(
        _mm_unpacklo_epi16(sum128, _mm_setzero_si128()),
        _mm_unpackhi_epi16(sum128, _mm_setzero_si128()));

    // Horizontal sum using the macro
    HSUM128_EPI16(sum_epi32, sum);

    return (float) sum;
}
