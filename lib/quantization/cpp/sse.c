#include <stdlib.h>
#include <stdint.h>
#include <immintrin.h>

#include "export_macro.h"

#ifdef _MSC_VER
#include <intrin.h>
#define __builtin_popcount __popcnt
#endif

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

EXPORT uint32_t impl_xor_popcnt_scalar8_sse_uint128(
    const uint8_t* query_ptr,
    const uint8_t* vector_ptr,
    uint32_t count
) {
    const uint64_t* v_ptr = (const uint64_t*)vector_ptr;
    const uint64_t* q_ptr = (const uint64_t*)query_ptr;

    __m128i sum1 = _mm_set1_epi32(0);
    __m128i sum2 = _mm_set1_epi32(0);
    for (uint32_t _i = 0; _i < count; _i++) {
        uint64_t v_1 = *v_ptr;
        uint64_t v_2 = *(v_ptr + 1);

        __m128i popcnt1 = _mm_set_epi32(
            _mm_popcnt_u64(v_1 ^ *(q_ptr + 0)),
            _mm_popcnt_u64(v_1 ^ *(q_ptr + 2)),
            _mm_popcnt_u64(v_1 ^ *(q_ptr + 4)),
            _mm_popcnt_u64(v_1 ^ *(q_ptr + 6))
        );
        sum1 = _mm_add_epi32(sum1, popcnt1);

        __m128i popcnt2 = _mm_set_epi32(
            _mm_popcnt_u64(v_1 ^ *(q_ptr + 8)),
            _mm_popcnt_u64(v_1 ^ *(q_ptr + 10)),
            _mm_popcnt_u64(v_1 ^ *(q_ptr + 12)),
            _mm_popcnt_u64(v_1 ^ *(q_ptr + 14))
        );
        sum2 = _mm_add_epi32(sum2, popcnt2);

        __m128i popcnt3 = _mm_set_epi32(
            _mm_popcnt_u64(v_2 ^ *(q_ptr + 1)),
            _mm_popcnt_u64(v_2 ^ *(q_ptr + 3)),
            _mm_popcnt_u64(v_2 ^ *(q_ptr + 5)),
            _mm_popcnt_u64(v_2 ^ *(q_ptr + 7))
        );
        sum1 = _mm_add_epi32(sum1, popcnt3);

        __m128i popcnt4 = _mm_set_epi32(
            _mm_popcnt_u64(v_2 ^ *(q_ptr + 9)),
            _mm_popcnt_u64(v_2 ^ *(q_ptr + 11)),
            _mm_popcnt_u64(v_2 ^ *(q_ptr + 13)),
            _mm_popcnt_u64(v_2 ^ *(q_ptr + 15))
        );
        sum2 = _mm_add_epi32(sum2, popcnt4);

        v_ptr += 2;
        q_ptr += 16;
    }
    __m128i factor1 = _mm_set_epi32(1, 2, 4, 8);
    __m128i factor2 = _mm_set_epi32(16, 32, 64, 128);
    __m128 result1_mm128 = _mm_cvtepi32_ps(_mm_mullo_epi32(sum1, factor1));
    __m128 result2_mm128 = _mm_cvtepi32_ps(_mm_mullo_epi32(sum2, factor2));
    HSUM128_PS(_mm_add_ps(result1_mm128, result2_mm128), mul_scalar);
    return (uint32_t)mul_scalar;
}

EXPORT uint32_t impl_xor_popcnt_scalar4_sse_uint128(
    const uint8_t* query_ptr,
    const uint8_t* vector_ptr,
    uint32_t count
) {
    const uint64_t* v_ptr = (const uint64_t*)vector_ptr;
    const uint64_t* q_ptr = (const uint64_t*)query_ptr;

    __m128i sum = _mm_set1_epi32(0);
    for (uint32_t _i = 0; _i < count; _i++) {
        uint64_t v_1 = *v_ptr;
        uint64_t v_2 = *(v_ptr + 1);

        __m128i popcnt1 = _mm_set_epi32(
            _mm_popcnt_u64(v_1 ^ *(q_ptr + 0)),
            _mm_popcnt_u64(v_1 ^ *(q_ptr + 2)),
            _mm_popcnt_u64(v_1 ^ *(q_ptr + 4)),
            _mm_popcnt_u64(v_1 ^ *(q_ptr + 6))
        );
        sum = _mm_add_epi32(sum, popcnt1);

        __m128i popcnt2 = _mm_set_epi32(
            _mm_popcnt_u64(v_2 ^ *(q_ptr + 1)),
            _mm_popcnt_u64(v_2 ^ *(q_ptr + 3)),
            _mm_popcnt_u64(v_2 ^ *(q_ptr + 5)),
            _mm_popcnt_u64(v_2 ^ *(q_ptr + 7))
        );
        sum = _mm_add_epi32(sum, popcnt2);

        v_ptr += 2;
        q_ptr += 8;
    }
    __m128i factor = _mm_set_epi32(1, 2, 4, 8);
    __m128 result_mm128 = _mm_cvtepi32_ps(_mm_mullo_epi32(sum, factor));
    HSUM128_PS(result_mm128, mul_scalar);
    return (uint32_t)mul_scalar;
}

EXPORT uint32_t impl_xor_popcnt_scalar8_sse_u8(
    const uint8_t* q_ptr,
    const uint8_t* v_ptr,
    uint32_t count
) {
    __m128i sum1 = _mm_set1_epi32(0);
    __m128i sum2 = _mm_set1_epi32(0);
    for (uint32_t _i = 0; _i < count / 8; _i++) {
        uint64_t v = *((const uint64_t*)v_ptr);

        uint8_t values1[8] = {
            *(q_ptr + 0),
            *(q_ptr + 8),
            *(q_ptr + 16),
            *(q_ptr + 24),
            *(q_ptr + 32),
            *(q_ptr + 40),
            *(q_ptr + 48),
            *(q_ptr + 56),
        };
        uint8_t values2[8] = {
            *(q_ptr + 1),
            *(q_ptr + 9),
            *(q_ptr + 17),
            *(q_ptr + 25),
            *(q_ptr + 33),
            *(q_ptr + 41),
            *(q_ptr + 49),
            *(q_ptr + 57),
        };
        uint8_t values3[8] = {
            *(q_ptr + 2),
            *(q_ptr + 10),
            *(q_ptr + 18),
            *(q_ptr + 26),
            *(q_ptr + 34),
            *(q_ptr + 42),
            *(q_ptr + 50),
            *(q_ptr + 58),
        };
        uint8_t values4[8] = {
            *(q_ptr + 3),
            *(q_ptr + 11),
            *(q_ptr + 19),
            *(q_ptr + 27),
            *(q_ptr + 35),
            *(q_ptr + 43),
            *(q_ptr + 51),
            *(q_ptr + 59),
        };
        uint8_t values5[8] = {
            *(q_ptr + 4),
            *(q_ptr + 12),
            *(q_ptr + 20),
            *(q_ptr + 28),
            *(q_ptr + 36),
            *(q_ptr + 44),
            *(q_ptr + 52),
            *(q_ptr + 60),
        };
        uint8_t values6[8] = {
            *(q_ptr + 5),
            *(q_ptr + 13),
            *(q_ptr + 21),
            *(q_ptr + 29),
            *(q_ptr + 37),
            *(q_ptr + 45),
            *(q_ptr + 53),
            *(q_ptr + 61),
        };
        uint8_t values7[8] = {
            *(q_ptr + 6),
            *(q_ptr + 14),
            *(q_ptr + 22),
            *(q_ptr + 30),
            *(q_ptr + 38),
            *(q_ptr + 46),
            *(q_ptr + 54),
            *(q_ptr + 62),
        };
        uint8_t values8[8] = {
            *(q_ptr + 7),
            *(q_ptr + 15),
            *(q_ptr + 23),
            *(q_ptr + 31),
            *(q_ptr + 39),
            *(q_ptr + 47),
            *(q_ptr + 55),
            *(q_ptr + 63),
        };

        uint64_t values1_u64 = *((const uint64_t*)values1);
        uint64_t values2_u64 = *((const uint64_t*)values2);
        uint64_t values3_u64 = *((const uint64_t*)values3);
        uint64_t values4_u64 = *((const uint64_t*)values4);
        uint64_t values5_u64 = *((const uint64_t*)values5);
        uint64_t values6_u64 = *((const uint64_t*)values6);
        uint64_t values7_u64 = *((const uint64_t*)values7);
        uint64_t values8_u64 = *((const uint64_t*)values8);

        __m128i popcnt1 = _mm_set_epi32(
            _mm_popcnt_u64(v ^ values1_u64),
            _mm_popcnt_u64(v ^ values2_u64),
            _mm_popcnt_u64(v ^ values3_u64),
            _mm_popcnt_u64(v ^ values4_u64)
        );
        sum1 = _mm_add_epi32(sum1, popcnt1);

        __m128i popcnt2 = _mm_set_epi32(
            _mm_popcnt_u64(v ^ values5_u64),
            _mm_popcnt_u64(v ^ values6_u64),
            _mm_popcnt_u64(v ^ values7_u64),
            _mm_popcnt_u64(v ^ values8_u64)
        );
        sum2 = _mm_add_epi32(sum2, popcnt2);

        v_ptr += 8;
        q_ptr += 64;
    }

    uint32_t dr1 = 0;
    uint32_t dr2 = 0;
    uint32_t dr3 = 0;
    uint32_t dr4 = 0;
    uint32_t dr5 = 0;
    uint32_t dr6 = 0;
    uint32_t dr7 = 0;
    uint32_t dr8 = 0;
    for (uint32_t _i = count % 8; _i > 0; _i--) {
        uint8_t v = *(v_ptr++);
        uint8_t q1 = *(q_ptr++);
        uint8_t q2 = *(q_ptr++);
        uint8_t q3 = *(q_ptr++);
        uint8_t q4 = *(q_ptr++);
        uint8_t q5 = *(q_ptr++);
        uint8_t q6 = *(q_ptr++);
        uint8_t q7 = *(q_ptr++);
        uint8_t q8 = *(q_ptr++);
        
        uint8_t x1 = v ^ q1;
        uint8_t x2 = v ^ q2;
        uint8_t x3 = v ^ q3;
        uint8_t x4 = v ^ q4;
        uint8_t x5 = v ^ q5;
        uint8_t x6 = v ^ q6;
        uint8_t x7 = v ^ q7;
        uint8_t x8 = v ^ q8;

        dr1 += __builtin_popcount(x1);
        dr2 += __builtin_popcount(x2);
        dr3 += __builtin_popcount(x3);
        dr4 += __builtin_popcount(x4);
        dr5 += __builtin_popcount(x5);
        dr6 += __builtin_popcount(x6);
        dr7 += __builtin_popcount(x7);
        dr8 += __builtin_popcount(x8);
    }

    __m128i factor1 = _mm_set_epi32(1, 2, 4, 8);
    __m128i factor2 = _mm_set_epi32(16, 32, 64, 128);
    __m128 result_mm128 = _mm_cvtepi32_ps(
        _mm_add_epi32(
            _mm_mullo_epi32(sum1, factor1),
            _mm_mullo_epi32(sum2, factor2)
        )
    );
    HSUM128_PS(result_mm128, mul_scalar);
    return (uint32_t)mul_scalar + dr1 + (dr2 << 1) + (dr3 << 2) + (dr4 << 3) + (dr5 << 4) + (dr6 << 5) + (dr7 << 6) + (dr8 << 7);
}

EXPORT uint32_t impl_xor_popcnt_scalar4_sse_u8(
    const uint8_t* q_ptr,
    const uint8_t* v_ptr,
    uint32_t count
) {
    __m128i sum = _mm_set1_epi32(0);
    for (uint32_t _i = 0; _i < count / 8; _i++) {
        uint64_t v = *((const uint64_t*)v_ptr);

        uint8_t values1[8] = {
            *(q_ptr + 0),
            *(q_ptr + 4),
            *(q_ptr + 8),
            *(q_ptr + 12),
            *(q_ptr + 16),
            *(q_ptr + 20),
            *(q_ptr + 24),
            *(q_ptr + 28),
        };
        uint8_t values2[8] = {
            *(q_ptr + 1),
            *(q_ptr + 5),
            *(q_ptr + 9),
            *(q_ptr + 13),
            *(q_ptr + 17),
            *(q_ptr + 21),
            *(q_ptr + 25),
            *(q_ptr + 29),
        };
        uint8_t values3[8] = {
            *(q_ptr + 2),
            *(q_ptr + 6),
            *(q_ptr + 10),
            *(q_ptr + 14),
            *(q_ptr + 18),
            *(q_ptr + 22),
            *(q_ptr + 26),
            *(q_ptr + 30),
        };
        uint8_t values4[8] = {
            *(q_ptr + 3),
            *(q_ptr + 7),
            *(q_ptr + 11),
            *(q_ptr + 15),
            *(q_ptr + 19),
            *(q_ptr + 23),
            *(q_ptr + 27),
            *(q_ptr + 31),
        };

        uint64_t values1_u64 = *((const uint64_t*)values1);
        uint64_t values2_u64 = *((const uint64_t*)values2);
        uint64_t values3_u64 = *((const uint64_t*)values3);
        uint64_t values4_u64 = *((const uint64_t*)values4);

        __m128i popcnt = _mm_set_epi32(
            _mm_popcnt_u64(v ^ values1_u64),
            _mm_popcnt_u64(v ^ values2_u64),
            _mm_popcnt_u64(v ^ values3_u64),
            _mm_popcnt_u64(v ^ values4_u64)
        );
        sum = _mm_add_epi32(sum, popcnt);

        v_ptr += 8;
        q_ptr += 32;
    }

    uint32_t dr1 = 0;
    uint32_t dr2 = 0;
    uint32_t dr3 = 0;
    uint32_t dr4 = 0;
    for (uint32_t _i = count % 8; _i > 0; _i--) {
        uint8_t v = *(v_ptr++);
        uint8_t q1 = *(q_ptr++);
        uint8_t q2 = *(q_ptr++);
        uint8_t q3 = *(q_ptr++);
        uint8_t q4 = *(q_ptr++);
        
        uint8_t x1 = v ^ q1;
        uint8_t x2 = v ^ q2;
        uint8_t x3 = v ^ q3;
        uint8_t x4 = v ^ q4;
        dr1 += __builtin_popcount(x1);
        dr2 += __builtin_popcount(x2);
        dr3 += __builtin_popcount(x3);
        dr4 += __builtin_popcount(x4);
    }

    __m128i factor = _mm_set_epi32(1, 2, 4, 8);
    __m128 result_mm128 = _mm_cvtepi32_ps(_mm_mullo_epi32(sum, factor));
    HSUM128_PS(result_mm128, mul_scalar);
    return (uint32_t)mul_scalar + dr1 + (dr2 << 1) + (dr3 << 2) + (dr4 << 3);
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
