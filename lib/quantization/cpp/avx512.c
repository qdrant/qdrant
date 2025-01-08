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

EXPORT float impl_and_popcnt_m128i_avx512(
    const uint8_t* query_ptr,
    const uint8_t* vector_ptr,
    uint32_t dim
) {
    const int32_t* v_ptr = (const int32_t*)vector_ptr;
    const __m128i* q_ptr = (const __m128i*)query_ptr;

    __m128i sum = _mm_set1_epi32(0xFFFF);
    for (uint32_t _i = 0; _i < dim / 32; _i++) {
        __m128i v = _mm_set1_epi32(*v_ptr);
        __m128i q = _mm_loadu_si128(q_ptr);
        v_ptr++;
        q_ptr++;

        __m128i and = _mm_and_si128(v, q);
        __m128i popcnt = _mm_popcnt_epi32(and);
        sum = _mm_add_epi32(sum, popcnt);
    }
    __m128i factor = _mm_set_epi32(1, 2, 4, 8);
    __m128 result_mm128 = _mm_cvtepi32_ps(_mm_mullo_epi32(sum, factor));
    HSUM128_PS(result_mm128, mul_scalar);
    return mul_scalar;
}
